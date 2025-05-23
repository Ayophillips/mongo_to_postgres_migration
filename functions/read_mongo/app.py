import pymongo # type: ignore
import os
import json
import logging
import time
import boto3
from botocore.exceptions import ClientError
from bson import json_util, ObjectId # type: ignore
from bson.json_util import default
from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError, NetworkTimeout

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# MongoDB client - initialize outside the handler for connection reuse across invocations
mongo_client = None

# Lambda-optimized MongoDB connection settings
CONNECT_TIMEOUT_MS = 5000  # 5 seconds
SOCKET_TIMEOUT_MS = 10000  # 10 seconds
SERVER_SELECTION_TIMEOUT_MS = 15000  # 15 seconds
MAX_POOL_SIZE = 10  # Reduced pool size for Lambda's limited concurrency
DEFAULT_BATCH_SIZE = 10  # Reduced batch size for better throughput

class MongoReadError(Exception):
    """Custom exception for MongoDB read operations"""
    pass

class MongoTimeoutError(MongoReadError):
    """Exception for MongoDB timeout errors"""
    pass

class SecretsManagerError(Exception):
    """Exception for AWS Secrets Manager errors"""
    pass

# Cache for secrets to avoid repeated calls to Secrets Manager
_secret_cache = {}

def get_secret(secret_arn):
    """
    Get secret from AWS Secrets Manager with caching
    """
    # Check cache first
    if secret_arn in _secret_cache:
        logger.info(f"Using cached secret for {secret_arn}")
        return _secret_cache[secret_arn]
    
    logger.info(f"Fetching secret from AWS Secrets Manager: {secret_arn}")
    client = boto3.client('secretsmanager')
    
    try:
        response = client.get_secret_value(SecretId=secret_arn)
        if 'SecretString' in response:
            secret = json.loads(response['SecretString'])
            # Cache the secret
            _secret_cache[secret_arn] = secret
            return secret
        else:
            raise SecretsManagerError("Secret value is not a string")
    except ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', 'Unknown')
        error_message = e.response.get('Error', {}).get('Message', 'Unknown error')
        logger.error(f"Failed to get secret: {error_code} - {error_message}")
        raise SecretsManagerError(f"Failed to get secret: {error_code} - {error_message}")

def get_mongo_client():
    """Get MongoDB client with connection pooling for Lambda"""
    global mongo_client
    if mongo_client is None:
        logger.info("Initializing MongoDB connection with Lambda-optimized settings")
        try:
            # Get MongoDB URI from Secrets Manager
            secret_arn = os.environ.get('MONGO_SECRETS_ARN')
            if not secret_arn:
                raise MongoReadError("MONGO_SECRETS_ARN environment variable is not set")
            
            secrets = get_secret(secret_arn)
            mongo_uri = secrets.get('uri')
            
            if not mongo_uri:
                raise MongoReadError("MongoDB URI not found in secrets")
            
            # Lambda-optimized connection settings
            mongo_client = pymongo.MongoClient(
                mongo_uri,
                connectTimeoutMS=CONNECT_TIMEOUT_MS,
                socketTimeoutMS=SOCKET_TIMEOUT_MS,
                serverSelectionTimeoutMS=SERVER_SELECTION_TIMEOUT_MS,
                maxPoolSize=MAX_POOL_SIZE,
                retryWrites=True,
                retryReads=True,
                appName="LambdaMongoReader"
            )
            # Test connection to validate it immediately
            mongo_client.admin.command('ping')
            logger.info("Successfully connected to MongoDB")
        except Exception as e:
            logger.error(f"Failed to establish MongoDB connection: {str(e)}")
            mongo_client = None
            raise
    return mongo_client

def lambda_handler(event, context):
    try:
        start_time = time.time()
        batch_size = int(os.environ.get('BATCH_SIZE', DEFAULT_BATCH_SIZE))
        last_processed_id = event.get('last_processed_id', None)
        
        # Get MongoDB client - reuses existing connection if available
        client = get_mongo_client()
        db = client["contacts-backend"]
        collection = db["users"]

        query = {}
        if last_processed_id:
            query['_id'] = {'$gt': ObjectId(last_processed_id)}
        
        # Add timeout for cursor to prevent long-running queries
        users = list(collection.find(query, no_cursor_timeout=False).limit(batch_size).max_time_ms(20000))
        
        if not users:
            return {
                "statusCode": 200,
                "users": [],
                "has_more": False,
                "last_processed_id": None
            }

        has_more = len(users) == batch_size
        last_id = str(users[-1]["_id"])
        
        elapsed_time = time.time() - start_time
        logger.info(f"Successfully read {len(users)} documents from MongoDB in {elapsed_time:.2f} seconds")
        
        # Convert MongoDB documents to JSON-serializable format
        serialized_users = json.loads(json_util.dumps(users))
        
        return {
            "statusCode": 200,
            "users": serialized_users,
            "has_more": has_more,
            "last_processed_id": last_id
        }
    
    except ServerSelectionTimeoutError as e:
        logger.error(f"MongoDB server selection timeout: {str(e)}")
        raise MongoTimeoutError(f"Failed to select MongoDB server: {str(e)}")
        
    except ConnectionFailure as e:
        logger.error(f"MongoDB connection failure: {str(e)}")
        # Reset the client on connection failure to force reconnection on next attempt
        global mongo_client
        mongo_client = None
        raise MongoTimeoutError(f"Failed to connect to MongoDB: {str(e)}")
        
    except NetworkTimeout as e:
        logger.error(f"MongoDB network timeout: {str(e)}")
        raise MongoTimeoutError(f"MongoDB network operation timed out: {str(e)}")
        
    except SecretsManagerError as e:
        logger.error(f"Failed to get MongoDB credentials: {str(e)}")
        raise MongoReadError(f"Failed to get MongoDB credentials: {str(e)}")
    
    except pymongo.errors.PyMongoError as e:
        logger.error(f"MongoDB operation error: {str(e)}")
        raise MongoReadError(f"Failed to read from MongoDB: {str(e)}")
    
    except json.JSONDecodeError as e:
        logger.error(f"JSON serialization error: {str(e)}")
        raise MongoReadError(f"Failed to serialize MongoDB data: {str(e)}")
        
    except TypeError as e:
        if 'not JSON serializable' in str(e):
            logger.error(f"JSON serialization error: {str(e)}")
            raise MongoReadError(f"Failed to serialize MongoDB data: {str(e)}")
        else:
            logger.error(f"Type error: {str(e)}")
            raise MongoReadError(f"Type error during read: {str(e)}")
            
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise MongoReadError(f"Unexpected error during read: {str(e)}")
    
    # Note: Don't close the client in Lambda to allow connection reuse
    # The client will be reused across invocations

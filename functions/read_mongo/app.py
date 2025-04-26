import pymongo # type: ignore
import os
import json
import logging
import time
from bson import json_util, ObjectId # type: ignore
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

class MongoReadError(Exception):
    """Custom exception for MongoDB read operations"""
    pass

class MongoTimeoutError(MongoReadError):
    """Exception for MongoDB timeout errors"""
    pass

def get_mongo_client():
    """Get MongoDB client with connection pooling for Lambda"""
    global mongo_client
    if mongo_client is None:
        logger.info("Initializing MongoDB connection with Lambda-optimized settings")
        try:
            # Lambda-optimized connection settings
            mongo_client = pymongo.MongoClient(
                os.environ['MONGO_URI'],
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
        batch_size = int(os.environ.get('BATCH_SIZE', 1000))
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
        
        return {
            "statusCode": 200,
            "users": users,
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
        
    except pymongo.errors.PyMongoError as e:
        logger.error(f"MongoDB operation error: {str(e)}")
        raise MongoReadError(f"Failed to read from MongoDB: {str(e)}")
        
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise MongoReadError(f"Unexpected error during read: {str(e)}")
    
    # Note: Don't close the client in Lambda to allow connection reuse
    # The client will be reused across invocations

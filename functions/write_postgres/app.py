import psycopg2 # type: ignore
import os
import logging
import time
import json
import boto3
from botocore.exceptions import ClientError
from psycopg2.extras import execute_batch # type: ignore
from psycopg2 import pool, errors # type: ignore

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# PostgreSQL connection pool - initialize outside the handler
pg_pool = None

# Constants for PostgreSQL connection
PG_CONNECTION_TIMEOUT = 10  # seconds
PG_MAX_POOL_SIZE = 5
PG_MIN_POOL_SIZE = 1
PG_MAX_BATCH_SIZE = 100
PG_MAX_RETRIES = 3
PG_RETRY_DELAY = 1  # seconds

# PostgreSQL error codes for transient errors that can be retried
RETRYABLE_ERROR_CODES = [
    '40001',  # serialization_failure
    '40P01',  # deadlock_detected
    '53300',  # too_many_connections
    '53400',  # configuration_limit_exceeded
    '57P01',  # admin_shutdown
    '57P02',  # crash_shutdown
    '57P03',  # cannot_connect_now
    '08000',  # connection_exception
    '08003',  # connection_does_not_exist
    '08006',  # connection_failure
    '08001',  # sqlclient_unable_to_establish_sqlconnection
    '08004',  # sqlserver_rejected_establishment_of_sqlconnection
]

class PostgresWriteError(Exception):
    """Custom exception for PostgreSQL write operations"""
    pass

class PostgresRetryableError(PostgresWriteError):
    """Exception for PostgreSQL errors that can be retried"""
    pass

class PostgresDataError(PostgresWriteError):
    """Exception for PostgreSQL data validation errors"""
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

def get_pg_pool():
    """
    Get or initialize PostgreSQL connection pool for Lambda with connection reuse
    """
    global pg_pool
    if pg_pool is None:
        logger.info("Initializing PostgreSQL connection pool")
        try:
            # Get database credentials from Secrets Manager
            secret_arn = os.environ.get('POSTGRES_SECRETS_ARN')
            if not secret_arn:
                raise PostgresWriteError("POSTGRES_SECRETS_ARN environment variable is not set")
                
            secrets = get_secret(secret_arn)
            
            # Connection parameters
            connect_params = {
                'dbname': secrets.get('database'),
                'user': secrets.get('username'),
                'password': secrets.get('password'),
                'host': secrets.get('host'),
                'connect_timeout': PG_CONNECTION_TIMEOUT
            }
            
            # Add port if specified
            if 'PG_PORT' in os.environ:
                connect_params['port'] = os.environ['PG_PORT']
                
            # Add SSL parameters if enabled
            if os.environ.get('PG_SSL', 'false').lower() == 'true':
                connect_params['sslmode'] = 'require'
                
                # Add additional SSL parameters if provided
                if 'PG_SSL_CA' in os.environ:
                    connect_params['sslrootcert'] = os.environ['PG_SSL_CA']
                    
            # Create connection pool
            pg_pool = pool.ThreadedConnectionPool(
                PG_MIN_POOL_SIZE, 
                PG_MAX_POOL_SIZE, 
                **connect_params
            )
            
            # Test the connection
            test_conn = pg_pool.getconn()
            test_conn.autocommit = True
            with test_conn.cursor() as test_cur:
                test_cur.execute("SELECT 1")
            pg_pool.putconn(test_conn)
            
            logger.info("Successfully connected to PostgreSQL")
        except Exception as e:
            logger.error(f"Failed to initialize PostgreSQL connection pool: {str(e)}")
            pg_pool = None
            raise
    return pg_pool

def validate_data(record):
    """
    Validate record data before writing to PostgreSQL
    """
    # Check required fields
    if not record.get("id"):
        raise PostgresDataError("Missing required field: id")
    
    # Validate data types (basic validation)
    if not isinstance(record.get("username", ""), str):
        raise PostgresDataError(f"Invalid username type for id {record['id']}: {type(record.get('username'))}")
    
    if not isinstance(record.get("email", ""), str):
        raise PostgresDataError(f"Invalid email type for id {record['id']}: {type(record.get('email'))}")
    
    return True

def lambda_handler(event, context):
    conn = None
    cur = None
    start_time = time.time()
    
    try:
        transformed_data = event.get("transformed", [])
        
        # Log data sample for debugging
        if transformed_data and len(transformed_data) > 0:
            # Safely log sample record, potentially with sensitive info redacted
            sample_record = transformed_data[0].copy() if isinstance(transformed_data[0], dict) else transformed_data[0]
            if isinstance(sample_record, dict) and 'email' in sample_record:
                sample_record['email'] = '****@****.com'  # Redact email for security
            logger.info(f"First record sample: {json.dumps(sample_record)}")
        
        if not transformed_data:
            return {
                "statusCode": 200,
                "message": "No data to write",
                "records_processed": 0,
                "has_more": event.get("has_more", False),
                "last_processed_id": event.get("last_processed_id")
            }
        
        # Get connection from pool
        pool = get_pg_pool()
        conn = pool.getconn()
        cur = conn.cursor()
        
        insert_query = """
            INSERT INTO users (id, username, email, created_at, updated_at) 
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE 
            SET username = EXCLUDED.username,
                email = EXCLUDED.email,
                updated_at = EXCLUDED.updated_at;
        """
        
        # Validate data before processing
        valid_data = []
        invalid_records = []
        
        for user in transformed_data:
            try:
                if validate_data(user):
                    valid_data.append((
                        user["id"], 
                        user.get("username", ""), 
                        user.get("email", ""), 
                        user.get("created_at"), 
                        user.get("updated_at")
                    ))
            except PostgresDataError as e:
                logger.warning(f"Data validation error: {str(e)}")
                invalid_records.append({
                    "id": user.get("id", "unknown"),
                    "error": str(e)
                })
        
        if not valid_data:
            logger.warning("No valid data to write after validation")
            return {
                "statusCode": 200,
                "message": "No valid data to write",
                "records_processed": 0,
                "invalid_records": invalid_records,
                "has_more": event.get("has_more", False),
                "last_processed_id": event.get("last_processed_id")
            }
        
        # Process in batches to avoid timeout issues
        total_processed = 0
        batch_size = min(PG_MAX_BATCH_SIZE, len(valid_data))
        
        for i in range(0, len(valid_data), batch_size):
            batch = valid_data[i:i+batch_size]
            retry_count = 0
            
            while retry_count <= PG_MAX_RETRIES:
                try:
                    execute_batch(cur, insert_query, batch)
                    conn.commit()
                    total_processed += len(batch)
                    logger.info(f"Batch processed: {len(batch)} records, total: {total_processed}/{len(valid_data)}")
                    break
                except psycopg2.Error as e:
                    error_code = getattr(e, 'pgcode', None)
                    if error_code in RETRYABLE_ERROR_CODES and retry_count < PG_MAX_RETRIES:
                        retry_count += 1
                        logger.warning(f"Retryable error (attempt {retry_count}/{PG_MAX_RETRIES}): {error_code} - {str(e)}")
                        conn.rollback()
                        time.sleep(PG_RETRY_DELAY * retry_count)  # Exponential backoff
                    else:
                        logger.error(f"Database error during batch: {str(e)}")
                        conn.rollback()
                        raise
        
        elapsed_time = time.time() - start_time
        logger.info(f"Successfully wrote {total_processed} records to PostgreSQL in {elapsed_time:.2f} seconds")
        
        return {
            "statusCode": 200,
            "message": "Write successful",
            "records_processed": total_processed,
            "invalid_records": invalid_records,
            "elapsed_time_seconds": elapsed_time,
            "has_more": event.get("has_more", False),
            "last_processed_id": event.get("last_processed_id")
        }

    except errors.UniqueViolation as e:
        logger.error(f"Unique constraint violation: {str(e)}")
        if conn:
            conn.rollback()
        raise PostgresDataError(f"Duplicate record found: {str(e)}")
    
    except errors.ForeignKeyViolation as e:
        logger.error(f"Foreign key violation: {str(e)}")
        if conn:
            conn.rollback()
        raise PostgresDataError(f"Reference constraint failed: {str(e)}")
    
    except errors.CheckViolation as e:
        logger.error(f"Check constraint violation: {str(e)}")
        if conn:
            conn.rollback()
        raise PostgresDataError(f"Check constraint failed: {str(e)}")
        
    except SecretsManagerError as e:
        logger.error(f"Failed to get database credentials: {str(e)}")
        raise PostgresWriteError(f"Failed to get database credentials: {str(e)}")
    
    except psycopg2.Error as e:
        logger.error(f"Database error: {str(e)}")
        if conn:
            conn.rollback()
        raise PostgresWriteError(f"Database operation failed: {str(e)}")
        
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        if conn:
            conn.rollback()
        raise PostgresWriteError(f"Write operation failed: {str(e)}")
        
    finally:
        # Return connection to pool instead of closing
        if conn and pg_pool:
            try:
                if cur:
                    cur.close()
                pg_pool.putconn(conn)
                logger.debug("Connection returned to pool")
            except Exception as e:
                logger.warning(f"Failed to return connection to pool: {str(e)}")
                # Try to close if we couldn't return to pool
                try:
                    conn.close()
                except:
                    pass

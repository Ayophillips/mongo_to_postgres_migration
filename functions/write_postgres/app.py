import psycopg2 # type: ignore
import os
import logging
from psycopg2.extras import execute_batch # type: ignore

logger = logging.getLogger()
logger.setLevel(logging.INFO)

class PostgresWriteError(Exception):
    """Custom exception for PostgreSQL write operations"""
    pass

def lambda_handler(event, context):
    conn = None
    cur = None
    try:
        transformed_data = event.get("transformed", [])
        if not transformed_data:
            return {
                "statusCode": 200,
                "message": "No data to write",
                "records_processed": 0,
                "has_more": event.get("has_more", False),
                "last_processed_id": event.get("last_processed_id")
            }

        conn = psycopg2.connect(
            dbname=os.environ['PG_DB'],
            user=os.environ['PG_USER'],
            password=os.environ['PG_PASS'],
            host=os.environ['PG_HOST']
        )
        cur = conn.cursor()
        
        insert_query = """
            INSERT INTO users (id, username, email, created_at, updated_at) 
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE 
            SET username = EXCLUDED.username,
                email = EXCLUDED.email,
                updated_at = EXCLUDED.updated_at;
        """
        
        data = [
            (user["id"], user["username"], user["email"], 
             user["created_at"], user["updated_at"])
            for user in transformed_data
        ]
        
        execute_batch(cur, insert_query, data)
        conn.commit()
        
        logger.info(f"Successfully wrote {len(data)} records to PostgreSQL")
        
        return {
            "statusCode": 200,
            "message": "Write successful",
            "records_processed": len(data),
            "has_more": event.get("has_more", False),
            "last_processed_id": event.get("last_processed_id")
        }

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
        if cur:
            cur.close()
        if conn:
            conn.close()
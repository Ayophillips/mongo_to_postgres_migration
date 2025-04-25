import pymongo # type: ignore
import os
import json
import logging
from bson import json_util, ObjectId # type: ignore

logger = logging.getLogger()
logger.setLevel(logging.INFO)

class MongoReadError(Exception):
    """Custom exception for MongoDB read operations"""
    pass

def lambda_handler(event, context):
    client = None
    try:
        batch_size = int(os.environ.get('BATCH_SIZE', 1000))
        last_processed_id = event.get('last_processed_id', None)
        
        client = pymongo.MongoClient(os.environ['MONGO_URI'])
        db = client["test"]
        collection = db["users"]

        query = {}
        if last_processed_id:
            query['_id'] = {'$gt': ObjectId(last_processed_id)}
        
        users = list(collection.find(query).limit(batch_size))
        
        if not users:
            return {
                "statusCode": 200,
                "users": [],
                "has_more": False,
                "last_processed_id": None
            }

        has_more = len(users) == batch_size
        last_id = str(users[-1]["_id"])
        
        logger.info(f"Successfully read {len(users)} documents from MongoDB")
        
        return {
            "statusCode": 200,
            "users": users,
            "has_more": has_more,
            "last_processed_id": last_id
        }

    except pymongo.errors.PyMongoError as e:
        logger.error(f"MongoDB operation error: {str(e)}")
        raise MongoReadError(f"Failed to read from MongoDB: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise MongoReadError(f"Unexpected error during read: {str(e)}")
    finally:
        if client:
            client.close()
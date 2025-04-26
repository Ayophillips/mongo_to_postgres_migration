import logging
import json
from datetime import datetime
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class TransformError(Exception):
    """Custom exception for data transformation"""
    pass

def extract_date(date_field):
    """
    Extract ISO date string from various date formats that could come from MongoDB serialization
    """
    if date_field is None:
        return None
        
    # If it's already a string
    if isinstance(date_field, str):
        return date_field
        
    # If it's a datetime object
    if hasattr(date_field, 'isoformat'):
        return date_field.isoformat()
        
    # If it's a serialized MongoDB date ({"$date": "2023-04-26T..."})
    if isinstance(date_field, dict) and "$date" in date_field:
        date_value = date_field["$date"]
        # Handle timestamp (milliseconds since epoch)
        if isinstance(date_value, int):
            return datetime.fromtimestamp(date_value / 1000).isoformat()
        # Handle ISO string
        return date_value
    
    # Unable to process date
    logger.warning(f"Unrecognized date format: {date_field} (type: {type(date_field)})")
    return None

def lambda_handler(event, context):
    try:
        users = event.get("users", [])
        
        # Log the first user for debugging (if available)
        if users and len(users) > 0:
            logger.info(f"First user example structure: {json.dumps(users[0][:100])}")  # Truncate for logging
            
            # Log specific date fields if present
            if "createdAt" in users[0]:
                logger.info(f"createdAt field type: {type(users[0]['createdAt'])} value: {users[0]['createdAt']}")
        
        if not users:
            return {
                "statusCode": 200,
                "transformed": [],
                "has_more": False,
                "last_processed_id": None
            }

        transformed = []
        for user in users:
            try:
                transformed_user = {
                    "id": str(user["_id"]) if "_id" in user else 
                         str(user["$oid"]) if "$oid" in user else None,
                    "username": user.get("username"),
                    "email": user.get("email"),
                    "created_at": extract_date(user.get("createdAt")),
                    "updated_at": extract_date(user.get("updatedAt"))
                }
                transformed.append(transformed_user)
                
            except Exception as e:
                logger.error(f"Error transforming user {user.get('_id', 'unknown')}: {str(e)}")
                # Continue with next record instead of failing the entire batch
                continue

        logger.info(f"Successfully transformed {len(transformed)} records out of {len(users)} input records")
        return {
            "statusCode": 200,
            "transformed": transformed,
            "has_more": event.get("has_more", False),
            "last_processed_id": event.get("last_processed_id")
        }

    except KeyError as e:
        logger.error(f"Missing required field: {str(e)}")
        raise TransformError(f"Missing required field: {str(e)}")
    except TypeError as e:
        # Specific handling for type errors which are common with date conversion
        logger.error(f"Type error during transformation: {str(e)}")
        logger.error(f"Event structure: {json.dumps(event)[:200]}...")  # Log truncated event for debugging
        raise TransformError(f"Type error during transformation: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error during transformation: {str(e)}")
        raise TransformError(f"Transform failed: {str(e)}")

import logging
import json
import traceback
from datetime import datetime
from bson import ObjectId, json_util

logger = logging.getLogger()
logger.setLevel(logging.INFO)

class TransformError(Exception):
    """Custom exception for data transformation"""
    pass

class ValidationError(TransformError):
    """Custom exception for data validation"""
    pass

def safe_serialize(obj):
    """
    Safely serialize MongoDB objects and handle special types
    """
    try:
        # Handle ObjectId
        if isinstance(obj, ObjectId):
            return str(obj)
        
        # Handle slice objects (unhashable)
        if isinstance(obj, slice):
            return f"slice({obj.start}, {obj.stop}, {obj.step})"
        
        # Handle other types that json_util can handle
        serialized = json.loads(json_util.dumps(obj))
        return serialized
    except TypeError as e:
        logger.error(f"Cannot serialize object of type {type(obj)}: {str(e)}")
        # Return string representation for problematic objects
        return str(obj)
    except Exception as e:
        logger.error(f"Error serializing object: {str(e)}")
        return str(obj)

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

def validate_user_data(user):
    """Validate user data before transformation"""
    # Check if user is a dict
    if not isinstance(user, dict):
        logger.error(f"Invalid user data type: {type(user)}")
        raise ValidationError(f"User data must be a dictionary, got {type(user)}")
    
    # Check for required _id field
    if "_id" not in user and "$oid" not in user:
        user_sample = str(user)[:100] + "..." if len(str(user)) > 100 else str(user)
        logger.error(f"Missing _id field in user data: {user_sample}")
        raise ValidationError("User data missing _id field")
    
    return True

def lambda_handler(event, context):
    try:
        users = event.get("users", [])
        
        # Log debugging information about the input
        logger.info(f"Received {len(users)} users to transform")
        
        # Log the first user for debugging (if available)
        if users and len(users) > 0:
            try:
                user_sample = safe_serialize(users[0])
                logger.info(f"First user example structure: {json.dumps(user_sample)[:500]}")  # Truncate for logging
                
                # Log specific date fields if present
                if "createdAt" in users[0]:
                    date_type = type(users[0]["createdAt"]).__name__
                    date_value = str(users[0]["createdAt"])[:100]
                    logger.info(f"createdAt field type: {date_type} value: {date_value}")
            except Exception as e:
                logger.error(f"Error logging user sample: {str(e)}")
        
        if not users:
            logger.info("No users to transform")
            return {
                "statusCode": 200,
                "transformed": [],
                "has_more": False,
                "last_processed_id": None
            }

        transformed = []
        error_count = 0
        
        for i, user in enumerate(users):
            try:
                # Validate user data first
                validate_user_data(user)
                
                # Safe extraction of ID field with fallbacks
                user_id = None
                if "_id" in user:
                    if isinstance(user["_id"], ObjectId):
                        user_id = str(user["_id"])
                    elif isinstance(user["_id"], dict) and "$oid" in user["_id"]:
                        user_id = user["_id"]["$oid"]
                    else:
                        user_id = str(user["_id"])
                elif "$oid" in user:
                    user_id = str(user["$oid"])

                transformed_user = {
                    "id": user_id,
                    "username": user.get("username", ""),
                    "email": user.get("email", ""),
                    "created_at": extract_date(user.get("createdAt")),
                    "updated_at": extract_date(user.get("updatedAt"))
                }
                transformed.append(transformed_user)
                
            except ValidationError as e:
                logger.warning(f"Validation error for user at index {i}: {str(e)}")
                error_count += 1
                continue
                
            except TypeError as e:
                logger.error(f"Type error transforming user at index {i}: {str(e)}")
                logger.error(f"User data type: {type(user)}")
                if isinstance(user, dict) and "_id" in user:
                    logger.error(f"User ID: {user['_id']}")
                error_count += 1
                continue
                
            except Exception as e:
                logger.error(f"Error transforming user at index {i}: {str(e)}")
                logger.error(traceback.format_exc())
                error_count += 1
                continue

        success_rate = (len(transformed) / len(users)) * 100 if users else 100
        logger.info(f"Successfully transformed {len(transformed)} records out of {len(users)} input records ({success_rate:.1f}% success rate)")
        
        if error_count > 0:
            logger.warning(f"{error_count} records had transformation errors and were skipped")
            
        return {
            "statusCode": 200,
            "transformed": transformed,
            "has_more": event.get("has_more", False),
            "last_processed_id": event.get("last_processed_id"),
            "error_count": error_count
        }

    except KeyError as e:
        logger.error(f"Missing required field: {str(e)}")
        logger.error(traceback.format_exc())
        raise TransformError(f"Missing required field: {str(e)}")
        
    except TypeError as e:
        # Specific handling for type errors which are common with date conversion
        logger.error(f"Type error during transformation: {str(e)}")
        logger.error(traceback.format_exc())
        
        # Log problematic event structure
        try:
            event_sample = json.dumps(safe_serialize(event))[:500]
            logger.error(f"Event structure (safely serialized): {event_sample}...")
        except:
            logger.error("Could not serialize event structure for logging")
            
        raise TransformError(f"Type error during transformation: {str(e)}")
        
    except ValidationError as e:
        logger.error(f"Validation error: {str(e)}")
        raise TransformError(f"Validation error: {str(e)}")
        
    except Exception as e:
        logger.error(f"Unexpected error during transformation: {str(e)}")
        logger.error(traceback.format_exc())
        raise TransformError(f"Transform failed: {str(e)}")

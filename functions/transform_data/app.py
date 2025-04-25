import logging
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

class TransformError(Exception):
    """Custom exception for data transformation"""
    pass

def lambda_handler(event, context):
    try:
        users = event.get("users", [])
        if not users:
            return {
                "statusCode": 200,
                "transformed": [],
                "has_more": False,
                "last_processed_id": None
            }

        transformed = []
        for user in users:
            transformed_user = {
                "id": str(user["_id"]),
                "username": user.get("username"),
                "email": user.get("email"),
                "created_at": user["createdAt"].isoformat() if user.get("createdAt") else None,
                "updated_at": user["updatedAt"].isoformat() if user.get("updatedAt") else None
            }
            transformed.append(transformed_user)

        logger.info(f"Successfully transformed {len(transformed)} records")

        return {
            "statusCode": 200,
            "transformed": transformed,
            "has_more": event.get("has_more", False),
            "last_processed_id": event.get("last_processed_id")
        }

    except KeyError as e:
        logger.error(f"Missing required field: {str(e)}")
        raise TransformError(f"Missing required field: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error during transformation: {str(e)}")
        raise TransformError(f"Transform failed: {str(e)}")
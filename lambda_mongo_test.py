#!/usr/bin/env python3
import sys
import os
import time
import socket
import getpass
import json
from urllib.parse import quote_plus
from pymongo import MongoClient, monitoring
from pymongo.errors import (
    ConnectionFailure,
    ConfigurationError,
    OperationFailure,
    ServerSelectionTimeoutError,
    NetworkTimeout
)

# Constants for Lambda optimization
DEFAULT_LAMBDA_CONNECT_TIMEOUT_MS = 5000    # 5 seconds
DEFAULT_LAMBDA_SOCKET_TIMEOUT_MS = 10000    # 10 seconds
DEFAULT_LAMBDA_SERVER_SELECTION_TIMEOUT_MS = 15000  # 15 seconds
DEFAULT_MAX_POOL_SIZE = 10  # Reduced pool size for Lambda's limited concurrency
DEFAULT_MAX_IDLE_TIME_MS = 60000  # 60 seconds - Lambda functions rarely run longer

# MongoDB connection monitoring for detailed diagnostics
class ConnectionMonitor(monitoring.CommandListener):
    def started(self, event):
        print(f"Command {event.command_name} started on server {event.connection_id}")
    
    def succeeded(self, event):
        print(f"Command {event.command_name} on server {event.connection_id} succeeded in {event.duration_micros / 1000.0} ms")
    
    def failed(self, event):
        print(f"Command {event.command_name} on server {event.connection_id} failed in {event.duration_micros / 1000.0} ms")
        print(f"Failure: {event.failure}")

def test_dns_resolution(hostname):
    """Test DNS resolution of the MongoDB host"""
    print(f"\nTesting DNS resolution for {hostname}...")
    try:
        ip_addresses = socket.gethostbyname_ex(hostname)
        print(f"✅ DNS resolution successful. IP addresses: {ip_addresses[2]}")
        return True
    except socket.gaierror as e:
        print(f"❌ DNS resolution failed: {e}")
        return False

def test_tcp_connection(hostname, port=27017, timeout=5):
    """Test TCP connection to the MongoDB host"""
    print(f"Testing TCP connection to {hostname}:{port}...")
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    try:
        sock.connect((hostname, port))
        print(f"✅ TCP connection successful to {hostname}:{port}")
        sock.close()
        return True
    except (socket.timeout, socket.error) as e:
        print(f"❌ TCP connection failed: {e}")
        return False
    finally:
        sock.close()

def get_lambda_connection_options(enable_monitoring=False):
    """Get MongoDB connection options optimized for Lambda"""
    options = {
        # Connection settings
        'connectTimeoutMS': DEFAULT_LAMBDA_CONNECT_TIMEOUT_MS,
        'socketTimeoutMS': DEFAULT_LAMBDA_SOCKET_TIMEOUT_MS,
        'serverSelectionTimeoutMS': DEFAULT_LAMBDA_SERVER_SELECTION_TIMEOUT_MS,
        
        # Pool settings
        'maxPoolSize': DEFAULT_MAX_POOL_SIZE,
        'maxIdleTimeMS': DEFAULT_MAX_IDLE_TIME_MS,
        
        # Heartbeat settings (faster heartbeats for Lambda)
        'heartbeatFrequencyMS': 10000,  # 10 seconds
        
        # Connection retries
        'retryWrites': True,
        'retryReads': True,
        
        # App name for monitoring
        'appName': 'LambdaMongoTest'
    }
    
    # Add event listeners if monitoring is enabled
    if enable_monitoring:
        options['event_listeners'] = [ConnectionMonitor()]
    
    return options

def test_mongodb_lambda_connection():
    """
    Test MongoDB connection with settings optimized for AWS Lambda.
    Provides detailed feedback and error handling.
    """
    print("\n" + "="*80)
    print("MongoDB Atlas Lambda Connection Tester")
    print("="*80)
    
    print("\nThis script tests MongoDB connections with settings optimized for AWS Lambda.")
    print("Lambda functions require specific configurations to reliably connect to MongoDB Atlas.")
    
    # Get connection details
    choice = input("\nWould you like to: \n1. Enter the full connection string \n2. Build the connection string from components\nEnter 1 or 2: ")
    
    if choice == "1":
        # Get the complete connection string
        connection_string = getpass.getpass("\nEnter your MongoDB connection string (input will be hidden): ")
    else:
        # Build connection string from components
        print("\nEnter connection components (input will be hidden where appropriate):")
        username = input("Username: ")
        password = getpass.getpass("Password: ")
        cluster = input("Cluster address (e.g., cluster0.abc123.mongodb.net): ")
        database = input("Database name (optional, press Enter to skip): ")
        
        # URL encode username and password to handle special characters
        username_encoded = quote_plus(username)
        password_encoded = quote_plus(password)
        
        # Build the connection string
        connection_string = f"mongodb+srv://{username_encoded}:{password_encoded}@{cluster}"
        if database:
            connection_string += f"/{database}"
        connection_string += "?retryWrites=true&w=majority"
    
    # Print a masked version of the connection string for verification
    masked_string = connection_string
    if "://" in masked_string and "@" in masked_string:
        prefix = masked_string.split("://")[0] + "://"
        suffix = "@" + masked_string.split("@")[1]
        credentials = masked_string.split("://")[1].split("@")[0]
        if ":" in credentials:
            username = credentials.split(":")[0]
            masked_string = f"{prefix}{username}:****{suffix}"
    
    print(f"\nAttempting to connect with: {masked_string}")
    
    # Extract hostname for network tests
    hostname = None
    if "@" in connection_string and "/" in connection_string.split("@")[1]:
        hostname = connection_string.split("@")[1].split("/")[0].split("?")[0]
        if ":" in hostname:  # Remove port if present
            hostname = hostname.split(":")[0]
    
    # Perform network tests if hostname could be extracted
    if hostname:
        print("\n" + "-"*40)
        print("NETWORK CONNECTIVITY TESTS")
        print("-"*40)
        print("Testing network connectivity to MongoDB Atlas from your environment...")
        
        # For each shard in a cluster
        if hostname.startswith(("cluster", "shard")):
            # Try main cluster hostname first
            dns_ok = test_dns_resolution(hostname)
            if dns_ok:
                test_tcp_connection(hostname)
                
            # Test individual shards if this is a typical Atlas cluster
            if "-" in hostname and "." in hostname:
                base_name = hostname.split(".")[0]
                domain_part = ".".join(hostname.split(".")[1:])
                
                if not base_name.endswith(("-00-00", "-00-01", "-00-02")):
                    # Try to infer shard hostnames
                    for i in range(3):
                        shard_host = f"{base_name}-shard-00-0{i}.{domain_part}"
                        dns_ok = test_dns_resolution(shard_host)
                        if dns_ok:
                            test_tcp_connection(shard_host)
        else:
            # Single hostname
            dns_ok = test_dns_resolution(hostname)
            if dns_ok:
                test_tcp_connection(hostname)
    
    print("\n" + "-"*40)
    print("MONGODB CONNECTION TEST")
    print("-"*40)
    
    # Ask for monitoring level
    monitoring_enabled = input("\nEnable detailed connection monitoring? (y/n): ").lower() == 'y'
    
    # Get connection options optimized for Lambda
    options = get_lambda_connection_options(enable_monitoring=monitoring_enabled)
    
    start_time = time.time()
    
    # Attempt to connect with Lambda-optimized settings
    try:
        print(f"\nConnecting with Lambda-optimized settings:")
        print(f"- Connect timeout: {options['connectTimeoutMS']}ms")
        print(f"- Socket timeout: {options['socketTimeoutMS']}ms")
        print(f"- Server selection timeout: {options['serverSelectionTimeoutMS']}ms")
        print(f"- Max pool size: {options['maxPoolSize']}")
        
        client = MongoClient(connection_string, **options)
        
        # Test the connection by running a simple command
        result = client.admin.command('ping')
        
        elapsed_time = time.time() - start_time
        print(f"\n✅ Successfully connected to MongoDB Atlas in {elapsed_time:.2f} seconds!")
        print(f"Ping result: {json.dumps(result)}")
        
        # List available databases
        try:
            databases = client.list_database_names()
            print(f"Available databases: {', '.join(databases)}")
        except OperationFailure as e:
            print(f"Connected successfully, but insufficient permissions to list databases: {e}")
        
        # Test a small write and read if possible
        try:
            db_name = input("\nEnter a database name to test write/read operations (or press Enter to skip): ")
            if db_name:
                db = client[db_name]
                collection = db.lambda_test
                
                # Generate a unique ID for this test
                test_id = f"lambda_test_{int(time.time())}"
                
                # Insert a test document
                insert_result = collection.insert_one({
                    "test_id": test_id,
                    "message": "Lambda connection test",
                    "timestamp": time.time()
                })
                
                print(f"✅ Write test successful! Inserted document with ID: {insert_result.inserted_id}")
                
                # Read the document back
                read_result = collection.find_one({"test_id": test_id})
                if read_result:
                    print(f"✅ Read test successful! Retrieved document with test_id: {test_id}")
                else:
                    print("❌ Read test failed: Document not found")
                
                # Clean up
                collection.delete_one({"test_id": test_id})
                print(f"✅ Cleanup successful! Test document removed.")
        except Exception as e:
            print(f"Write/read test skipped or failed: {e}")
        
        return True
    
    except ConnectionFailure as e:
        elapsed_time = time.time() - start_time
        print(f"\n❌ Connection error after {elapsed_time:.2f} seconds: {e}")
        print("\nTroubleshooting tips for Lambda environments:")
        print("- Ensure Lambda has internet access (check VPC settings)")
        print("- Check if Lambda needs VPC configuration to access MongoDB Atlas")
        print("- Verify MongoDB Atlas IP whitelist includes Lambda's NAT gateway IPs")
    
    except ConfigurationError as e:
        elapsed_time = time.time() - start_time
        print(f"\n❌ Configuration error after {elapsed_time:.2f} seconds: {e}")
        print("\nTroubleshooting tips:")
        print("- Check your connection string format")
        print("- Ensure SRV records are correctly configured")
    
    except ServerSelectionTimeoutError as e:
        elapsed_time = time.time() - start_time
        print(f"\n❌ Server selection timeout after {elapsed_time:.2f} seconds: {e}")
        print("\nTroubleshooting tips for Lambda:")
        print("- This is a common error in Lambda. It indicates the Lambda function")
        print("  cannot reach the MongoDB servers within the timeout period.")
        print("- Check if Lambda needs to be in a VPC with access to the internet")
        print("- Increase serverSelectionTimeoutMS in your Lambda function")
        print("- Ensure your Atlas cluster is in a region close to your Lambda function")
    
    except OperationFailure as e:
        elapsed_time = time.time() - start_time
        print(f"\n❌ Authentication failed after {elapsed_time:.2f} seconds: {e}")
        print("\nTroubleshooting tips:")
        print("- Verify your username and password are correct")
        print("- Check if the user has appropriate permissions")
        print("- Ensure special characters in username/password are properly URL-encoded")
    
    except NetworkTimeout as e:
        elapsed_time = time.time() - start_time
        print(f"\n❌ Network timeout after {elapsed_time:.2f} seconds: {e}")
        print("\nTroubleshooting tips for Lambda:")
        print("- Lambda functions have limited execution time")
        print("- Reduce connection timeouts for Lambda environments")
        print("- Consider using AWS PrivateLink or VPC peering for Atlas")
    
    except Exception as e:
        elapsed_time = time.time() - start_time
        print(f"\n❌ An unexpected error occurred after {elapsed_time:.2f} seconds: {e}")
        print("Please check all connection parameters and Lambda configuration.")
    
    return False

def print_lambda_recommendations():
    """Print recommendations for MongoDB connections in Lambda environments"""
    print("\n" + "-"*40)
    print("AWS LAMBDA RECOMMENDATIONS")
    print("-"*40)
    print("1. CONNECTION POOLING:")
    print("   - Use a global variable to store the MongoDB client")
    print("   - Initialize the connection outside the handler function")
    print("   - This allows connection reuse across Lambda invocations")
    print("\n2. TIMEOUT SETTINGS:")
    print("   - Use shorter timeouts than default (Lambda has limited execution time)")
    print("   - connectTimeoutMS: 3000-5000ms")
    print("   - socketTimeoutMS: 5000-10000ms")
    print("   - serverSelectionTimeoutMS: 5000-15000ms")
    print("\n3. VPC CONFIGURATION:")
    print("   - If MongoDB Atlas IP whitelist is used, Lambda needs a NAT Gateway")
    print("   - Configure Lambda in a VPC with private subnets and a NAT Gateway")
    print("   - Or use AWS PrivateLink for MongoDB Atlas")
    print("\n4. CODE SAMPLE:")
    print("""
    # MongoDB client - initialize OUTSIDE the handler
    client = None
    
    def get_mongo_client():
        global client
        if client is None:
            # Initialize the MongoDB client with Lambda-optimized settings
            client = MongoClient(
                'mongodb+srv://username:password@cluster.mongodb.net/?retryWrites=true&w=majority',
                connectTimeoutMS=5000,
                socketTimeoutMS=10000,
                serverSelectionTimeoutMS=15000,
                maxPoolSize=10,
                maxIdleTimeMS=60000,
                retryWrites=True,
                retryReads=True
            )
        return client
    
    def lambda_handler(event, context):
        try:
            # Get the MongoDB client - reuses existing connection if available
            mongo = get_mongo_client()
            
            # Use the client
            db = mongo.your_database
            result = db.your_collection.find_one({'_id': 'some_id'})
            return {
                'statusCode': 200,
                'body': json.dumps(result)
            }
        except Exception as e:
            return {
                'statusCode': 500,
                'body': json.dumps({'error': str(e)})
            }
    """)

if __name__ == "__main__":
    print_lambda_recommendations()
    success = test_mongodb_lambda_connection()
    
    if success:
        print("\nConnection test completed successfully! You can use these settings in your Lambda function.")
    else:
        print("\nConnection test failed. Please check the error messages above and try again.")
        sys.exit(1)

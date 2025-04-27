# MongoDB to PostgreSQL Migration Tool
> A serverless data migration tool built with AWS Lambda and Step Functions

Author: Ayo Phillips

## Overview
This project provides a serverless solution for migrating data from MongoDB to PostgreSQL databases. It uses AWS Lambda functions orchestrated by Step Functions to handle the ETL (Extract, Transform, Load) process in a scalable and reliable way.


![stepfunctions_graph](https://github.com/user-attachments/assets/465066b4-ff9d-42cd-bec8-43b869e8a495)

## Architecture
The solution consists of three main components:
- VPC Infrastructure for secure database connectivity
- Lambda functions for ETL operations
- Step Functions state machine for orchestration

### Lambda Functions
1. **Read MongoDB** (`ReadMongoFunction`)
   - Extracts data from MongoDB Atlas in batches
   - Handles connection pooling and retry logic

2. **Transform Data** (`TransformDataFunction`)
   - Converts MongoDB documents to PostgreSQL-compatible format
   - Validates and cleanses data

3. **Write PostgreSQL** (`WritePostgresFunction`)
   - Loads transformed data into PostgreSQL
   - Manages database connections and transaction handling

## Prerequisites
- AWS CLI configured with appropriate credentials
- AWS SAM CLI installed
- Python 3.9
- MongoDB Atlas cluster
- PostgreSQL database instance

## Configuration
1. VPC Setup (`vpc-infrastructure.yaml`):
```yaml
Parameters:
  VpcCIDR: 10.0.0.0/16
  EnvironmentName: MongoDBAtlasVPC
```

2. Database Credentials:
   - Store MongoDB URI in AWS Secrets Manager
   - Store PostgreSQL credentials in AWS Secrets Manager

## Deployment
Deploy in the following order:

1. Deploy VPC Infrastructure:
```bash
sam deploy --config-env vpc
```

2. Deploy Migration Stack:
```bash
sam deploy --config-env default
```

## Layer Dependencies
### PostgreSQL Layer
```bash
cd layers/postgresql
mkdir -p python
docker run --rm --platform linux/amd64 -v "$PWD":/var/task public.ecr.aws/sam/build-python3.9 pip install -r requirements.txt -t python/
```

### MongoDB Layer
```bash
cd layers/mongodb
mkdir -p python
docker run --rm --platform linux/amd64 -v "$PWD":/var/task public.ecr.aws/sam/build-python3.9 pip install -r requirements.txt -t python/
```

## Testing
Use the included test script to verify MongoDB connectivity:
```bash
python lambda_mongo_test.py
```

## Error Handling
- Retries configured for transient failures
- Custom error types for specific failure scenarios
- Comprehensive logging for troubleshooting

## Monitoring
- CloudWatch Logs for Lambda function execution
- Step Functions execution history
- Custom metrics for migration progress

## Security
- VPC isolation for database access
- Secrets Manager for credential management
- IAM roles with least privilege access

## Limitations
- Maximum Lambda execution time: 15 minutes
- Batch size limitations for large datasets
- Network timeout considerations

## License
MIT License

---
For questions or issues, please open a GitHub issue.

import json
import boto3
import logging
from boto3.dynamodb.conditions import Key
from typing import List, Dict

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize DynamoDB resource
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('terraform_resources')

# CORS headers
headers = {
    'Access-Control-Allow-Origin': '*',  
    'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
    'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
}

def get_user_deployments(user_id: str):
    """Get all deployments for a specific user_id using GSI."""
    try:
        # Query using GSI on user_id without the notified filter
        response = table.query(
            IndexName='user_id-index',
            KeyConditionExpression=Key('user_id').eq(user_id),
            ScanIndexForward=False  # Sort in descending order
        )
        
        items = response['Items']
        
        # Group resources by session_id
        deployments = {}
        for item in items:
            session_id = item['session_id']
            if session_id not in deployments:
                deployments[session_id] = {
                    'session_id': session_id,
                    'resources': [],
                    'timestamp': item.get('timestamp')
                }
            
            # Add resource details with all stored fields
            resource = {
                'type': item['resource_type'],
                'deployment_id': item['deployment_id'],
                'resource_name': item['resource_name'],
                'value': item.get('value'),
                'is_sensitive': item.get('is_sensitive', False),
                'timestamp': item.get('timestamp')
            }
            
            # Add type-specific fields
            if item['resource_type'] == 'ec2':
                if 'ip_address' in item:
                    resource['ip_address'] = item['ip_address']
            
            elif item['resource_type'] == 'rds':
                if 'endpoint' in item:
                    resource['endpoint'] = item['endpoint']
                if 'username' in item:
                    resource['username'] = item['username']
                if 'password' in item:
                    resource['password'] = item['password']
            
            elif item['resource_type'] in ['loadbalancer', 'ecs']:
                if 'dns_name' in item:
                    resource['dns_name'] = item['dns_name']
            
            # Add any additional fields that might exist
            for field in item:
                if field not in ['resource_type', 'deployment_id', 'resource_name', 
                               'value', 'is_sensitive', 'timestamp', 'session_id', 
                               'user_id', 'notified', 'ip_address', 'dns_name', 
                               'endpoint', 'username', 'password']:
                    resource[field] = item[field]
            
            deployments[session_id]['resources'].append(resource)
        
        # Convert to list and sort by timestamp
        deployment_list = list(deployments.values())
        deployment_list.sort(key=lambda x: x['timestamp'] if x['timestamp'] else '', reverse=True)
        
        return {
            'statusCode': 200,
            'headers': headers,
            'body': json.dumps({
                'user_id': user_id,
                'deployments': deployment_list,
                'total_deployments': len(deployment_list),
                'total_resources': sum(len(d['resources']) for d in deployment_list)
            })
        }
        
    except Exception as e:
        logger.error(f"Error fetching deployments for user {user_id}: {str(e)}")
        return {
            'statusCode': 500,
            'headers': headers,
            'body': json.dumps({
                'message': f'Error fetching deployments: {str(e)}',
                'error': str(e)
            })
        }

def lambda_handler(event, context):
    """
    Gets user_id from Cognito claims in the request context
    """
    try:
        # Get user_id from Cognito claims
        try:
            user_id = event['requestContext']['authorizer']['claims']['email']
            logger.info(f"Found user_id in Cognito claims: {user_id}")
        except KeyError:
            logger.error("Unable to get user_id from Cognito claims")
            return {
                'statusCode': 401,
                'headers': headers,
                'body': json.dumps({
                    'message': 'Unauthorized - Unable to get user identity'
                })
            }
        
        logger.info(f"Fetching all deployments for user: {user_id}")
        return get_user_deployments(user_id)
        
    except Exception as e:
        logger.error(f"Error in lambda handler: {str(e)}")
        return {
            'statusCode': 500,
            'headers': headers,
            'body': json.dumps({
                'message': f'Error processing request: {str(e)}',
                'error': str(e)
            })
        }
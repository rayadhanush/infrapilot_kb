import json
import logging
from datetime import datetime
import boto3
from botocore.exceptions import ClientError

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize DynamoDB client
dynamodb = boto3.client('dynamodb')

def get_all_key_mappings():
    """Get all entries from result_key_mapping table."""
    try:
        response = dynamodb.scan(
            TableName=os.environ.get("TABLE_NAME")
        )
        
        mappings = {}
        for item in response.get('Items', []):
            key_id = item['key_id']['S']
            mappings[key_id] = {
                'user_id': item['user_id']['S'],
                'session_id': item['session_id']['S']
            }
        
        # Handle pagination if there are more results
        while 'LastEvaluatedKey' in response:
            response = dynamodb.scan(
                TableName=os.environ.get("TABLE_NAME"),
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            for item in response.get('Items', []):
                key_id = item['key_id']['S']
                mappings[key_id] = {
                    'user_id': item['user_id']['S'],
                    'session_id': item['session_id']['S']
                }
                
        return mappings
    except ClientError as e:
        logger.error(f"Error scanning result-key-mapping table: {str(e)}")
        raise

def parse_rds_value(value):
    """Parse RDS value string into endpoint, and optionally username and password."""
    try:
        if not isinstance(value, str):
            return {'endpoint': str(value)}  # If it's not a string, treat entire value as endpoint
            
        # Split the value string into components
        components = [comp.strip() for comp in value.split(',')]
        
        result = {}
        # Always treat first component as endpoint
        if components:
            result['endpoint'] = components[0]
            
        # Add username and password if they exist
        if len(components) > 1:
            result['username'] = components[1]
        if len(components) > 2:
            result['password'] = components[2]
            
        return result
    except Exception as e:
        logger.error(f"Error parsing RDS value: {str(e)}")
        return {'endpoint': str(value)}  # Fallback to treating entire value as endpoint

def parse_terraform_output(output_str, key_mappings):
    """Parse terraform output and find matching resources from key mappings."""
    try:
        if "::debug::stdout:" in output_str:
            output_str = output_str.split("::debug::stdout:")[1].split("::debug::stderr:")[0].strip()
        
        output_str = output_str.replace('%0A', '\n').replace('%20', ' ')
        data = json.loads(output_str)
        
        matching_resources = []
        
        # Check each terraform output against our key mappings
        for key, value in data.items():
            if key in key_mappings:
                resource_info = {
                    'name': key,
                    'type': 'unknown',
                    'value': value.get('value'),
                    'sensitive': value.get('sensitive', False),
                    'data_type': value.get('type'),
                    'timestamp': key.split('_')[-1] if key.split('_')[-1].isdigit() else None,
                    'user_id': key_mappings[key]['user_id'],
                    'session_id': key_mappings[key]['session_id']
                }

                # Determine resource type and add specific attributes
                if 'ec2' in key.lower():
                    resource_info['type'] = 'ec2'
                    resource_info['ip_address'] = value.get('value') if 'ip' in key.lower() else None
                elif 'ecs' in key.lower():
                    resource_info['type'] = 'ecs'
                    if 'dns' in key.lower() or 'alb' in key.lower():
                        resource_info['dns_name'] = value.get('value')
                elif 'rds' in key.lower():
                    resource_info['type'] = 'rds'
                    rds_details = parse_rds_value(value.get('value'))
                    resource_info.update(rds_details)
                    resource_info['sensitive'] = bool(rds_details.get('username') or rds_details.get('password'))
                elif 'lb' in key.lower() or 'loadbalancer' in key.lower():
                    resource_info['type'] = 'loadbalancer'
                    resource_info['dns_name'] = value.get('value')
                elif 'key' in key.lower() or 'private_key' in key.lower():
                    resource_info['type'] = 'ssh_key'
                
                matching_resources.append(resource_info)
        
        if not matching_resources:
            logger.info("No matching resources found in key mappings")
            return None

        return {
            'resources': matching_resources,
            'sensitive_data': any(r['sensitive'] for r in matching_resources),
            'metadata': {
                'total_resources': len(matching_resources),
                'timestamp': datetime.now().isoformat(),
                'resource_types': list(set(r['type'] for r in matching_resources))
            }
        }
        
    except json.JSONDecodeError as e:
        logger.error(f"Failed to parse JSON: {str(e)}")
        logger.error(f"Problematic output string: {output_str}")
        raise
    except Exception as e:
        logger.error(f"Error processing terraform output: {str(e)}")
        raise
    
def store_resource_data(resource_data, user_id, session_id):
    """Store resource data in DynamoDB results table."""
    try:
        deployment_id = resource_data['name']
        
        item = {
            'deployment_id': {'S': deployment_id},
            'resource_name': {'S': resource_data['name']},
            'user_id': {'S': user_id},
            'session_id': {'S': session_id},
            'resource_type': {'S': resource_data['type']},
            'value': {'S': str(resource_data['value'])},
            'timestamp': {'S': datetime.now().isoformat()},
            'is_sensitive': {'BOOL': resource_data['sensitive']},
            'notified': {'BOOL': False}
        }
        
        # Add optional fields if they exist
        if resource_data.get('ip_address'):
            item['ip_address'] = {'S': resource_data['ip_address']}
        if resource_data.get('dns_name'):
            item['dns_name'] = {'S': resource_data['dns_name']}
        if resource_data.get('endpoint'):
            item['endpoint'] = {'S': resource_data['endpoint']}
        if resource_data.get('username'):
            item['username'] = {'S': resource_data['username']}
        if resource_data.get('password'):
            item['password'] = {'S': resource_data['password']}
        
        dynamodb.put_item(
            TableName='terraform_resources', 
            Item=item
        )
        logger.info(f"Stored resource data for {resource_data['name']}")
        
    except ClientError as e:
        logger.error(f"Error storing data in DynamoDB: {str(e)}")
        raise

def process_resource(resource, user_id, session_id):
    """Process individual resource information."""
    try:
        resource_type = resource['type']
        resource_name = resource['name']
        
        store_resource_data(resource, user_id, session_id)

        if resource['sensitive']:
            logger.info(f"Processed sensitive {resource_type} resource: {resource_name}")
            if resource_type == 'rds':
                credentials_msg = []
                if resource.get('username'): credentials_msg.append('username')
                if resource.get('password'): credentials_msg.append('password')
                if credentials_msg:
                    logger.info(f"RDS credentials stored: {', '.join(credentials_msg)}")
            elif resource_type == 'ssh_key':
                logger.info("SSH key detected - storing securely")
        else:
            logger.info(f"Processed {resource_type} resource: {resource_name} with value: {resource['value']}")
            
        if resource_type == 'ec2':
            logger.info(f"EC2 instance deployed: {resource['value']}")
            if resource.get('ip_address'):
                logger.info(f"EC2 IP address: {resource['ip_address']}")
        elif resource_type == 'loadbalancer':
            logger.info(f"Load balancer DNS: {resource['dns_name']}")
        elif resource_type == 'rds':
            logger.info(f"RDS endpoint processed: {resource['endpoint']}")
            
        if resource.get('timestamp'):
            logger.info(f"Resource timestamp: {resource['timestamp']}")
        
        return True
    except Exception as e:
        logger.error(f"Error processing resource {resource.get('name')}: {str(e)}")
        return False

def lambda_handler(event, context):
    """Main Lambda handler function."""
    logger.info("Processing new SQS message")
    
    try:
        for record in event['Records']:
            message_body = record['body']
            
            # Get all possible key mappings
            key_mappings = get_all_key_mappings()
            if not key_mappings:
                logger.warning("No key mappings found in result-key-mapping table")
                continue
            
            # Parse output and find matching resources
            parsed_output = parse_terraform_output(message_body, key_mappings)
            if not parsed_output:
                logger.info("No matching resources found for any key mappings")
                continue
            
            success = True
            processed_resources = []
            
            # Process each matching resource
            for resource in parsed_output['resources']:
                user_id = resource.pop('user_id')  # Remove from resource dict after getting value
                session_id = resource.pop('session_id')  # Remove from resource dict after getting value
                
                logger.info(f"Processing resource {resource['name']} for user_id: {user_id}, session_id: {session_id}")
                
                if not process_resource(resource, user_id, session_id):
                    success = False
                else:
                    processed_resources.append(resource['name'])
            
            response_body = {
                'message': 'Successfully processed all matching resources' if success else 'Some resources failed processing',
                'processed_resources': processed_resources,
                'total_processed': len(processed_resources),
                'contains_sensitive_data': parsed_output['sensitive_data'],
                'metadata': parsed_output['metadata']
            }
            
            return {
                'statusCode': 200 if success else 500,
                'body': json.dumps(response_body)
            }
            
    except Exception as e:
        logger.error(f"Error processing message: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': f'Error processing message: {str(e)}',
                'error': str(e)
            })
        }
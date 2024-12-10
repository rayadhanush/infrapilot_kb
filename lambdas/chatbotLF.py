import json
import boto3
import numpy as np
from supabase import create_client
from openai import OpenAI
import requests
import re

# Define global variables for API endpoints
API_BASE_URL = os.environ.get("API_BASE_URL")
USERNAME = os.environ.get("USERNAME")
PASSWORD = os.environ.get("PASSWORD")

# Initialize OpenAI API
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")
#embedding_function = OpenAIEmbeddings(openai_api_key=OPENAI_API_KEY)
client = OpenAI(api_key=OPENAI_API_KEY)

# Initialize Supabase client
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
supabase_client = create_client(SUPABASE_URL, SUPABASE_KEY)

# Initialize DynamoDB client
dynamodb = boto3.client('dynamodb')
SESSION_TABLE = os.environ.get("SESSION_TABLE")
KEY_MAP_TABLE = os.environ.get("KEY_MAP_TABLE")

# Predefined intents (to be retrieved from somewhere, could be in a database or static list)
intents_list = ['create an EC2 instance', 'create an RDS DB instance', 'create an S3 bucket', 'hi hello']

headers = {
        'Access-Control-Allow-Origin': '*', 
        'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token',
        'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        }

def cosine_similarity(query_matrix, embedding_matrix):
    """
    Compute cosine similarity between a single query vector (1, N) and a set of embedding vectors (M, N).
    Both query_matrix and embedding_matrix should be 2D arrays.
    """
    query_matrix = np.array(query_matrix)
    embedding_matrix = np.array(embedding_matrix)
    
    # Normalize the query vector and embedding matrix
    query_norm = np.linalg.norm(query_matrix, axis=1, keepdims=True)  # (1, N)
    embedding_norm = np.linalg.norm(embedding_matrix, axis=1, keepdims=True)  # (M, N)
    
    dot_product = np.dot(embedding_matrix, query_matrix.T).flatten()  # (M,)

    cosine_similarities = [dot_product] / (embedding_norm.flatten() * query_norm.flatten())

    return cosine_similarities

# Helper functions


def get_intent_vectorsearch(user_input, threshold=0.8):
    response = client.embeddings.create(
                    input=user_input,
                    model="text-embedding-ada-002",  
                    encoding_format="float"
                )
    query_embedding = response.data[0].embedding

    intent_response = supabase_client.rpc(
        "match_intent",  
        {"query_embedding": query_embedding}
    ).execute()
    if intent_response.data and intent_response.data[0]["similarity"] >= threshold:
        return intent_response.data[0]["intent"]  # Assuming "intent_name" is the column for intent
    else:
        return None

"""Retrieve the best matching template from Supabase based on user input."""

def retrieve_template(user_input):    
    response = client.embeddings.create(
                    input=user_input,
                    model="text-embedding-ada-002",  
                    encoding_format="float"
                )
    query_embedding = response.data[0].embedding
    
    template = supabase_client.rpc(
        "match_template",  
        {"query_embedding": query_embedding}
    ).execute()

    if(template.data):
        matching_template = template.data[0]  # Assuming "intent_name" is the column for intent
        return(
                matching_template["intent"],
                matching_template["template"],
                matching_template["required_slots"],
                matching_template["method"],
                matching_template["endpoint"]
            )
    return None, None, None, None, None

def update_session(user_id, session_id, intent=None, slots=None):
    """Update session state in DynamoDB."""
    session_data = {
        'SessionID': {'S': session_id},
        'UserId': {'S': user_id},
        'Intent': {'S': intent or ""},
        'Slots': {'S': json.dumps(slots or {})}
    }
    dynamodb.put_item(TableName=SESSION_TABLE, Item=session_data)

def get_session(session_id):
    """Retrieve session state from DynamoDB using session_id."""
    try:
        response = dynamodb.get_item(TableName=SESSION_TABLE, Key={'SessionID': {'S': session_id}})
        if 'Item' in response:
            return {
                'intent': response['Item']['Intent']['S'],
                'slots': json.loads(response['Item']['Slots']['S'])
            }
    except Exception as e:
        print(f"Error retrieving session: {e}")
    return {'intent': None, 'slots': {}}


def lambda_handler(event, context):

    print(event)
    user_id = event['requestContext']['authorizer']['claims']['email']
    session_id = json.loads(event['body'])['session_id']
    user_input = json.loads(event['body'])['message']

    if not user_input:
        return {
            'statusCode': 400,
            'headers': headers,
            'body': json.dumps({'response': 'User input is required'})
        }

    # Retrieve session state
    session = get_session(session_id)
    intent = session['intent']
    slots = session['slots']

    print("retrive session state")
    print("Intent:", intent)

    if not intent:
        # Identify intent if not already identified
        intent = get_intent_vectorsearch(user_input)
        print("Identified Int:", intent)
        if intent:
            if(intent =='hi hello'):
                return {
                'statusCode': 200,
                'headers': headers,
                'body': json.dumps({'response': f"Hi {user_id}, how may I assist you today with your AWS infrastructure?"})
            }
            if(intent =='Create a security group'):
                data = retrieve_and_generate_rag(user_input)
                data_payload = {
                    "file_data": data
                }
                send_rag_post_req(data_payload, "POST", "/api/custom/", user_id, session_id)
                return {
                'statusCode': 200,
                'headers': headers,
                'body': json.dumps({'response': f"We have processed your request to {intent}. Please wait while we fetch the resources."})
                }

            _, _, required_slots, _, _ = retrieve_template(user_input)

            if(required_slots):
                slots = {slot: None for slot in required_slots}
            else:
                slots = None
            update_session(user_id, session_id, intent, slots)
            
            if(required_slots):
                return {
                    'statusCode': 200,
                    'headers': headers,
                    'body': json.dumps({'response': f"I understand you want to {intent}. Can you provide the following details? \n \n {required_slots[0]}:"})
                }
        else:
            return {
                'statusCode': 200,
                'headers': headers,
                'body': json.dumps({'response': "Sorry, I couldn't understand your request. Can you clarify?"})
            }

    # If intent is already identified, handle slot filling
    if(slots):
        for slot, value in slots.items():
            if value is None:
                # Ask for the next unfilled slot
                slots[slot] = user_input  # Here, validate the input if necessary
                if not validate_slot(slot, user_input):
                    return {
                        'statusCode': 200,
                        'headers': headers,
                        'body': json.dumps({'response': f"Sorry, that is an incorrect value for {slot}. Please provide it again."})
                    }
                update_session(user_id, session_id, intent, slots)
                next_slot = next((s for s, v in slots.items() if v is None), None)
                if next_slot:
                    return {
                        'statusCode': 200,
                        'headers': headers,
                        'body': json.dumps({'response': f"Please provide {next_slot}."})
                    }
                break
    
    print("slots:", slots)
    # If all slots are filled, fulfill the request
    _, template, _, method, endpoint = retrieve_template(intent)
    #filled_template = template.format(**slots)
    
    if(intent == 'Create an EC2 instance'):
        data_payload = {
        "username": user_id,
        "ec_instance_name": slots["Instance Name"],
        "ec2_instance_type": slots["Instance Type"],
        "ec2_ami_id": slots["Ami ID"]
        }
    
    elif(intent == 'Search or Get your EC2 instances'):
        data_payload = {
        "username": user_id
        }
    
    elif(intent == 'Delete your EC2 instance'):
        data_payload = {
        "resource_name": slots["Resource Name"]
        }

    elif(intent == 'Create an RDS Database Instance'):
        data_payload = {
        "username": user_id,
        "db_name": slots["DB Name"],
        "db_engine": slots["DB Engine"],
        "instance_class": slots["Instance Class"],
        "db_storage": slots["DB Storage"]
        }
    
    elif(intent == 'Get your exisitng RDS Database instances'):
        data_payload = {
        "username": user_id
        }
    
    elif(intent == 'Delete your RDS instance'):
        data_payload = {
        "resource_name": slots["Resource Name"]
        }

    elif(intent == 'Create an ECS Cluster'):
        data_payload = {
        "user_id": user_id,
        "github_url": slots["Github URL"],
        "number_of_instances": slots["Number of Instances"],
        "docker_image_name": slots["Docker Image Name"],
        "container_port": slots["Container Port"],
        "cluster_name": slots["Cluster Name"],
        "healthcheck_endpoint": slots["Healthcheck Endpoint"],
        "cpu": slots["CPU (in CPU units)"],
        "memory": slots["Memory (in MB)"],
        }

    elif(intent == 'Get your exisitng ECS Clusters'):
        data_payload = {
        "username": user_id
        }
    
    elif(intent == 'Delete an ECS Cluster'):
        data_payload = {
        #"username": user_id,
        "resource_name": slots["Resource Name"]
        }

    # Call external API endpoints
    auth_response = requests.post(
        f"{API_BASE_URL}/api/token/",
        json={"username": USERNAME, "password": PASSWORD},
        verify=False
    )

    auth_response.raise_for_status()
    token = auth_response.json().get('access')
    
    headers_auth = {"Authorization": f"Bearer {token}"}

    print("method:", method)
    print("endpoint", endpoint)
    print("Payload", data_payload)

    try:
        api_response  = requests.request(
            method=method.upper(),
            url=f"{API_BASE_URL}{endpoint}",
            json=data_payload,
            headers=headers_auth,
            verify=False
            )
        api_response.raise_for_status()
    except Exception as e:
        print("Error occurred during API request:", str(e))
        update_session(user_id, session_id)
        return {
            'statusCode': 200,
            'headers': headers,
            'body': json.dumps({
                'response': f"Seems like your request to {intent} failed."
            })
        }

    

    print("Response Status Code:", api_response.status_code)

    #For DELETE requests
    if(api_response.status_code == 204):
        resource = slots["Resource Name"]
        update_session(user_id, session_id)
        return {
            'statusCode': 200,
            'headers': headers,
            'body': json.dumps({
                'response': f"Successfully deleted {resource}"
            })
        }

    #For POST requests
    if(api_response.status_code == 201):
        api_result = api_response.json()
        print("API POST Results:", api_result)
        key_id = api_result['key_id']
        update_key_id(session_id, user_id, key_id)
        update_session(user_id, session_id)
        # Clears the session intent and session_id once fullfilled
        # Ready to accept new requests
        return {
            'statusCode': 200,
            'headers': headers,
            'body': json.dumps({
                'response': "Thank you! Your request has been processed successfully. Please wait while we fetch the resources. You should get a notification when the resources are up"
            })
        }
    
    #For GET requests
    if(api_response.status_code == 200):
        api_result = api_response.json()
        print("API GET Results:", api_result)
        resource_data = api_result['data']['resource_names']
        update_session(user_id, session_id)
        return {
            'statusCode': 200,
            'headers': headers,
            'body': json.dumps({
                'response': f"Here are your resources: {resource_data}"
            })
        }

    return {
            'statusCode': 200,
            'headers': headers,
            'body': json.dumps({
                'response': "Oops how did you end up here?"
            })
    }

def update_key_id(session_id, user_id, key_id):
    """Update session state in DynamoDB."""
    key_session_mapping = {
        'key_id': {'S': key_id},
        'session_id': {'S': session_id},
        'user_id': {'S': user_id}
    }
    dynamodb.put_item(TableName=KEY_MAP_TABLE, Item=key_session_mapping)

def validate_slot(slot, value):
    """Validate the input for a slot."""
    # Add validation logic for slots based on type, format, etc.
    return True

def retrieve_and_generate_rag(user_input):
    """
    Implements a RAG workflow using both template and related documents.
    """
    # Step 1: Generate embedding for user input
    response = client.embeddings.create(
                    input=user_input,
                    model="text-embedding-ada-002",  
                    encoding_format="float"
                )
    query_embedding = response.data[0].embedding

    # Step 2: Retrieve the most relevant template
    template_response = supabase_client.rpc(
        "match_template",  # Postgres function for template similarity search
        {"query_embedding": query_embedding}
    ).execute()

    if not template_response.data or len(template_response.data) == 0:
        return "No matching template found."

    # Step 3: Retrieve related documents
    docs_response = supabase_client.rpc(
        "match_docs",  # Postgres function for document similarity search
        {"query_embedding": query_embedding}
    ).execute()



    # Retrieve context from template
    retrieved_template = template_response.data[0]
    template_text = retrieved_template['template']
    required_slots = retrieved_template['required_slots']

    # Retrieve related documents
    related_docs = []
    if docs_response.data and len(docs_response.data) > 0:
        related_docs = [doc['content'] for doc in docs_response.data]

    base_template = """
    resource "aws_instance" "ec2_compute_instance" {
    ami           = "ami-09d56f8956ab235b3"
    instance_type = "t3.small"
    tags = {
        Name = "meghnasavit"
    }
    lifecycle {
        ignore_changes = [ami]
    }
    }
    """
    # Step 4: Augment the prompt with retrieved context and related documents
    #f"Required Slots: {required_slots}\n\n"
    augmented_prompt = (
        f"User Input: {user_input}\n\n"
        f"Base Template:\n{base_template}\n\n"
        f"Related Documentation:\n{''.join(related_docs)}\n\n"
        "Based on the user input, retrieve base template, and additional related information, reuse key values or defaults from the template when possible\n"
        "- Try to generate for the exact task in user prompt and only that"
        "- Add new resource group if and only if necessary.\n"
        "- You may remove existing resources if necessary.\n"
        "- Refine the template to match the user's request.\n"
        "- Make sure that all module names are unique by appending a random uuid at the end.\n" 
        "- Dont using variables in the file, instead put in values as string literals in the resource block"
    )


    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You are a helpful assistant. Only generate terraform template without additional text"},
            {
                "role": "user",
                "content": augmented_prompt
            }
        ]
    )
    
    generated_template = response.choices[0].message.content

    print("Generated:",generated_template)

    pattern1 = r"```hcl\n(.*?)\n```"
    pattern2 = r"```terraform\n(.*?)\n```"
    # Search and extract the payload
    match = re.search(pattern1, generated_template, re.DOTALL)
    payload1 = match.group(1).strip() if match else None
  
    match = re.search(pattern2, generated_template, re.DOTALL)
    payload2 = match.group(1).strip() if match else None

    if(payload1):
        print("Payload1", payload1)
        return payload1
    else:
        print("Payload2", payload2)
        return payload2
    



def send_rag_post_req(data_payload, method, endpoint, user_id, session_id):
    # Call external API endpoints
    print("Dat Payload:", data_payload)
    auth_response = requests.post(
        f"{API_BASE_URL}/api/token/",
        json={"username": USERNAME, "password": PASSWORD},
        verify=False
    )
    auth_response.raise_for_status()
    token = auth_response.json().get('access')
    
    headers_auth = {"Authorization": f"Bearer {token}"}

    print("method:", method)
    print("endpoint", endpoint)
    print("Payload", data_payload)

    try:
        api_response  = requests.request(
            method=method.upper(),
            url=f"{API_BASE_URL}{endpoint}",
            json=data_payload,
            headers=headers_auth,
            verify=False
            )
        api_response.raise_for_status()
    except Exception as e:
        print("Error occurred during API request:", str(e))
        update_session(user_id, session_id)
        return {
            'statusCode': 200,
            'headers': headers,
            'body': json.dumps({
                'response': f"Seems like your request to {intent} failed."
            })
        }
    
    # Clears the session intent and session_id once fullfilled
    update_session(user_id, session_id)

    if(api_response.status_code == 201):
        api_result = api_response.json()
        print("API POST Results:", api_result)
        
        # Ready to accept new requests
        return {
            'statusCode': 200,
            'headers': headers,
            'body': json.dumps({
                'response': "Thank you! Your request has been processed successfully. "
            })
        }
    




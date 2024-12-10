import os
import json
import requests
from supabase import create_client
from openai import OpenAI
import hashlib
import uuid

def chunk_text(text, chunk_size=4000, overlap=500):
    """
    Split text into overlapping chunks
    
    Args:
        text (str): Input text to chunk
        chunk_size (int): Size of each chunk
        overlap (int): Number of characters to overlap between chunks
    
    Returns:
        list: List of text chunks
    """
    chunks = []
    for i in range(0, len(text), chunk_size - overlap):
        chunks.append(text[i:i + chunk_size])
    return chunks

def generate_unique_hash(content, source_file, chunk_index):
    """
    Generate a unique hash for each document chunk
    
    Args:
        content (str): The text content of the chunk
        source_file (str): The source file name
        chunk_index (int): The index of the chunk
    
    Returns:
        str: A unique hash identifier
    """
    hash_input = f"{content}|{source_file}|{chunk_index}|{str(uuid.uuid4())}"
    return hashlib.sha256(hash_input.encode('utf-8')).hexdigest()

def delete_embeddings_for_files(supabase, file_paths, TABLE_NAME):
    """
    Remove embeddings for specific files from Supabase.
    
    Args:
        supabase: Supabase client instance
        file_paths (list): List of file paths whose embeddings need to be removed
    """
    for file_path in file_paths:
        try:
            supabase.table(TABLE_NAME).delete().eq('source_file', file_path).execute()
            print(f"Removed embeddings for file: {file_path}")
        except Exception as e:
            print(f"Failed to remove embeddings for {file_path}: {e}")

def process_files(client, supabase, raw_base_url, file_paths, TABLE_NAME, operation="add"):
    """
    Process files to add or update embeddings.
    
    Args:
        event: AWS Lambda event
        supabase: Supabase client instance
        raw_base_url (str): Base URL for raw files
        file_paths (list): List of file paths to process
        operation (str): 'add' or 'update' for respective operations
    """
    for file_path in file_paths:
        try:
            # Download the file content
            raw_url = raw_base_url + file_path
            response = requests.get(raw_url)
            response.raise_for_status()
            file_content = response.text

            # Chunk text
            chunks = chunk_text(file_content)
            
            for i, chunk in enumerate(chunks):
                # Generate embedding
                response = client.embeddings.create(
                    input=chunk,
                    model="text-embedding-ada-002",  
                    encoding_format="float"
                )
                embedding = response.data[0].embedding

                # Generate unique hash
                unique_id = generate_unique_hash(chunk, file_path, i)
                
                # Prepare data for Supabase
                data = {
                    'id': unique_id,
                    'content': chunk,
                    'embedding': embedding,
                    'source_file': file_path,
                    'chunk_index': i
                }
                
                # Add or update embeddings in Supabase
                if operation == "add":
                    supabase.table(TABLE_NAME).insert(data).execute()
                elif operation == "update":
                    supabase.table(TABLE_NAME).upsert(data).execute()
                    
                print(f"{operation.capitalize()}ed embedding for chunk {i} of {file_path}")
        
        except requests.exceptions.RequestException as e:
            print(f"Failed to download file {file_path}: {e}")
        except Exception as e:
            print(f"Error processing file {file_path}: {e}")

def lambda_handler(event, context):
    print(event)
    # Set up API keys and Supabase client
    client = OpenAI(
        api_key=os.environ.get("OPENAI_API_KEY"),  # This is the default and can be omitted
    )    
    supabase = create_client(os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY"))
    TABLE_NAME = os.environ.get("TABLE_NAME")

    # Parse the webhook payload
    body = json.loads(event["body"])
    repo_name = body["repository"]["full_name"]
    commit_id = body["head_commit"]["id"]
    added_files = body["head_commit"]["added"]
    modified_files = body["head_commit"]["modified"]
    removed_files = body["head_commit"]["removed"]

    # Base URL for raw files
    raw_base_url = f"https://raw.githubusercontent.com/{repo_name}/{commit_id}/"

    # Remove embeddings for removed files
    delete_embeddings_for_files(supabase, removed_files, TABLE_NAME)

    # Remove embeddings for modified files
    delete_embeddings_for_files(supabase, modified_files, TABLE_NAME)

    # Add embeddings for added files
    process_files(client, supabase, raw_base_url, added_files, TABLE_NAME, operation="add")

    # Update embeddings for modified files
    process_files(client, supabase, raw_base_url, modified_files, TABLE_NAME, operation="update")

    return {
        "statusCode": 200,
        "body": json.dumps({"message": "Processed added, modified, and removed files"})
    }
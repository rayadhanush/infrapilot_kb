import json
import uuid
from datetime import datetime

def lambda_handler(event, context):
    try:
        # Generate a unique session ID
        session_id = str(uuid.uuid4())
        
        # Add session ID to the user's claims
        event['response'] = {
            'claimsOverrideDetails': {
                'claimsToAddOrOverride': {
                    'session_id': session_id,
                    'session_created': datetime.now().isoformat()
                }
            }
        }
        
        print(f"Created session {session_id} for user {event['request']['userAttributes']['sub']}")
        
        return event
        
    except Exception as e:
        print(f"Error creating session: {e}")
        return event  # Return event even on error to not block auth flow
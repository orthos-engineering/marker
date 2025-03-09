import os
from common import SignalHandler, process_message, ensure_directories_exist, process_message_org, process_message_contract_vault
from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
from dotenv import load_dotenv
import json
# Add parent directory to Python path
import sys
sys.path.insert(1, '/home/adel/marker')
from marker.models import create_model_dict
import time
import multiprocessing as mp

load_dotenv()

PROJECT_ID = os.environ["GCP_PROJECT_ID"]
SUBSCRIPTION_NAME = os.environ["PUBSUB_SUBSCRIPTION"]

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_NAME)

def is_permanent_failure(error_msg: str) -> bool:
    """Determine if an error is permanent and shouldn't be retried."""
    permanent_errors = [
        "EOF marker not found",  # Corrupted PDF - exact match needed
        "Invalid URL format",
        "#scrollbar=0",  # Invalid URL ending
        "content-type",  # Wrong content type
        "404",  # Not found
        "403",  # Forbidden
        "Expected key.size(1) == value.size(1) to be true, but got false.",
        "string indices must be integers"
    ]
    # Add debug logging
    print(f"Checking if error is permanent: {error_msg}")
    for err in permanent_errors:
        if err in error_msg:
            print(f"Matched permanent error: {err}")
            return True
    return True

def publish_to_failed_messages(message_data: dict, error: str):
    try:
        publisher = pubsub_v1.PublisherClient()
        failed_topic_path = publisher.topic_path(
            PROJECT_ID, 
            "pdf-tranformer-failed-pdfs"
        )
        
        # Add error details to message
        message_data['error'] = str(error)
        message_data['failed_at'] = time.time()
        
        message_bytes = json.dumps(message_data).encode('utf-8')
        publisher.publish(failed_topic_path, message_bytes)
        print(f"Published to failed-messages topic: {error}")
    except Exception as e:
        print(f"Failed to publish to failed-messages topic: {e}")

def callback(message):
    """Handle incoming Pub/Sub message."""
    # print(f"Received message: {message.data}")
    try:
        message_data = json.loads(message.data.decode('utf-8'))
        if message_data.get('case_management'):
            success = process_message_org(message_data, model_dict)
        elif message_data.get('contract_vault'):
            success = process_message_contract_vault(message_data, model_dict)
        else:
            # Pre-validate the message before processing
            download_url = message_data.get('download_url', '')
            if not download_url:
                print(f"Permanent failure - Invalid URL: {download_url}")
                message.ack()
                publish_to_failed_messages(message_data, "Invalid URL")
                return
                
            success = process_message(message_data, model_dict)
        
        if success:
            message.ack()
        else:
            print(f"Process message failed. Has last_error: {hasattr(process_message, 'last_error')}")
            if hasattr(process_message, 'last_error'):
                error_msg = process_message.last_error
                print(f"Last error: {error_msg}")
                print(f"Is permanent failure? {is_permanent_failure(error_msg)}")
            
            if hasattr(process_message, 'last_error') and is_permanent_failure(process_message.last_error):
                print(f"Permanent failure detected: {process_message.last_error}")
                message.ack()
                publish_to_failed_messages(message_data, process_message.last_error)
            else:
                print("Temporary failure, will retry")
                message.nack()
            
    except Exception as e:
        print(f"Exception while processing message: {repr(e)}")
        if is_permanent_failure(str(e)):
            print(f"Permanent failure detected: {e}")
            message.ack()  # Ack permanent failures
            publish_to_failed_messages(message_data, str(e))
        else:
            message.nack()  # Only retry temporary failures

if __name__ == "__main__":
    signal_handler = SignalHandler()
    print("Initializing models...")
    
    try:
        mp.set_start_method('spawn')
    except RuntimeError:
        pass
        
    model_dict = create_model_dict()
    for k, v in model_dict.items():
        v.model.share_memory()
    
    # Pre-initialize the process pool
    from common import initialize_process_pool
    initialize_process_pool(model_dict)
    print("Process pool initialized")
    
    # Ensure directories exist at startup
    ensure_directories_exist()
    
    print("Ready to receive messages.")
    
    # Configure flow control for ordered delivery
    flow_control = pubsub_v1.types.FlowControl(
        max_messages=1,  # Process one message at a time
    )
    
    streaming_pull_future = subscriber.subscribe(
        subscription_path, 
        callback=callback,
        flow_control=flow_control
    )
    
    try:
        # Keep the main thread alive
        while not signal_handler.received_signal:
            try:
                streaming_pull_future.result(timeout=5)
            except TimeoutError:
                continue
    finally:
        streaming_pull_future.cancel()
        subscriber.close()
        # Clean up models
        del model_dict

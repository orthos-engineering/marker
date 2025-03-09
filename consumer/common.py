import time
from signal import SIGINT, SIGTERM, signal
import sys
sys.path.insert(1, '/home/adel/marker')
from cleanup import process_markdown_file
import os
import shutil
import json
from google.cloud import storage
from split_large_pdf import split_pdf, merge_markdown_files
from marker.config.parser import ConfigParser
from marker.converters.pdf import PdfConverter
from marker.models import create_model_dict
from marker.output import save_output
# from marker.convert import main as convert_main
import requests
from urllib.parse import urlparse
import urllib3
from google.cloud import pubsub_v1
from db_utils import store_regulation
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
storage_client = storage.Client()
import gc
import torch.multiprocessing as mp
import math
from concurrent.futures import ProcessPoolExecutor
from PyPDF2 import PdfReader, PdfWriter
import tempfile

# Set required environment variables
os.environ["GRPC_VERBOSITY"] = "ERROR"
os.environ["GLOG_minloglevel"] = "2"
os.environ["PYTORCH_ENABLE_MPS_FALLBACK"] = "1"
os.environ["IN_STREAMLIT"] = "true"

# At module level, create a pre-initialized pool
process_pool = None

def initialize_process_pool(model_dict):
    global process_pool
    process_pool = ProcessPoolExecutor(
        max_workers=20,
        initializer=init_worker,
        initargs=(model_dict,)
    )

def init_worker(model_dict):
    # Initialize worker process
    global worker_model_dict
    worker_model_dict = model_dict
    print("Worker process initialized")

def empty_directory(directory_path):
    for filename in os.listdir(directory_path):
        file_path = os.path.join(directory_path, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print(f'Failed to delete {file_path}. Reason: {e}')

def download_from_gcs(bucket_name, object_key):
    """
    Download an object from Google Cloud Storage

    :param bucket_name: String name of the GCS bucket
    :param object_name: String name of the GCS object
    :return: True if object was downloaded, else False
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(object_key)

    file_name = os.path.basename(object_key)
    try:
        blob.download_to_filename(f'tmp/data/{file_name}')
        return True
    except Exception as e:
        print(f"Error downloading object {object_key} from bucket {bucket_name}. {e}")
        return False

def upload_to_gcs(bucket_name, object_key, file_path):
    """
    Upload an object to Google Cloud Storage

    :param bucket_name: String name of the GCS bucket
    :param object_name: String name of the GCS object
    :return: True if object was uploaded, else False
    """
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(f'{object_key.removesuffix(".pdf")}.md')

    file_name = os.path.basename(object_key)
    try:
        blob.upload_from_filename(file_path)
        return True
    except Exception as e:
        print(f"Error uploading object {object_key} to bucket {bucket_name}. {e}")
        return False

def download_from_url(url: str, save_path: str) -> bool:
    """
    Download a file from a URL and save it locally

    :param url: URL to download from
    :param save_path: Path where to save the downloaded file
    :return: True if successful, False otherwise
    """
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
        }
        
        # Using stream=True with a with-statement to ensure the response is closed properly
        with requests.get(url, headers=headers, allow_redirects=True, verify=False, timeout=30, stream=True) as response:
            response.raise_for_status()
            with open(save_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:  # filter out keep-alive new chunks
                        f.write(chunk)
        return True
    except Exception as e:
        print(f"Error downloading from URL {url}: {e}")
        # Cleanup partially downloaded file if exists
        try:
            if os.path.exists(save_path):
                os.remove(save_path)
        except Exception as cleanup_exception:
            print(f"Error cleaning up file {save_path}: {cleanup_exception}")
        process_message.last_error = str(e)  # Store the error message
        return False

def ensure_directories_exist():
    """Ensure all required temporary directories exist"""
    directories = ['tmp/data', 'tmp/processed_docs', 'tmp/cleaned_data']
    for directory in directories:
        os.makedirs(directory, exist_ok=True)

def process_chunk(chunk_path, config_parser):
    global worker_model_dict  # Use the pre-initialized model_dict
    try:
        converter = PdfConverter(
            config=config_parser.generate_config_dict(),
            artifact_dict=worker_model_dict,
            processor_list=config_parser.get_processors(),
            renderer=config_parser.get_renderer()
        )
        rendered = converter(chunk_path)
        out_folder = config_parser.get_output_folder(chunk_path)
        save_output(rendered, out_folder, config_parser.get_base_filename(chunk_path))
        return True
    except Exception as e:
        print(f"Error processing chunk {chunk_path}: {str(e)}")
        return False

def process_message(message_data: dict, model_dict) -> bool:
    start_time = time.time()
    processing_times = {}
    
    def log_timing(stage):
        def decorator(func):
            def wrapper(*args, **kwargs):
                stage_start = time.time()
                print(f"Starting {stage}...")
                result = func(*args, **kwargs)
                stage_end = time.time()
                duration = stage_end - stage_start
                processing_times[stage] = duration
                print(f"Completed {stage} in {duration:.2f} seconds")
                return result
            return wrapper
        return decorator

    # Initialize the last_error as None at the start
    process_message.last_error = None
    
    # Add type checking and debug logging
    if not isinstance(message_data, dict):
        error_msg = f"message_data must be a dictionary, got {type(message_data)}"
        print(error_msg)
        process_message.last_error = error_msg
        return False
    
    print(f"processing message: {message_data}")
        
    # Use temporary directories that auto-cleanup
    with tempfile.TemporaryDirectory() as temp_dir:
        try:
            # Create subdirectories in temp dir
            data_dir = os.path.join(temp_dir, 'data')
            processed_dir = os.path.join(temp_dir, 'processed')
            os.makedirs(data_dir, exist_ok=True)
            os.makedirs(processed_dir, exist_ok=True)
            
            jenis = message_data.get('type')
            year = message_data.get('year')
            number = message_data.get('number')
            if not isinstance(number, str):
                raise TypeError(f"Expected number to be string, got {type(number)}")
            number = number.replace('/', '-')
            
            bucket_path = f'{jenis}/{year}/{number}.pdf'
            download_url = message_data.get('download_url')
            
            if not download_url:
                print("Invalid message format: missing download_url")
                return False
                
            # Sanitize the filename to avoid path issues
            file_name = f'{number}.pdf'
            file_path = os.path.join(data_dir, file_name)
            
            @log_timing("PDF Download and Upload")
            def handle_pdf_download_upload():
                if not download_from_url(download_url, file_path):
                    print("Failed to download file from URL")
                    return False

                # Upload the PDF file to GCS
                storage_client = storage.Client()
                bucket = storage_client.bucket(os.environ.get("GCS_LAW_BUCKET"))
                blob = bucket.blob(f'{bucket_path}')
                try:
                    blob.upload_from_filename(file_path)
                    print(f"Successfully uploaded PDF to GCS: {bucket_path}")
                    return True
                except Exception as e:
                    print(f"Failed to upload PDF to GCS: {e}")
                    return False

            if not handle_pdf_download_upload():
                return False
                
            # Force split PDF into 5-page chunks regardless of size
            CHUNK_SIZE = 1  # Process 5 pages per chunk
            
            config_parser = ConfigParser({
                'output_dir': processed_dir,
                'disable_multiprocessing': True,  # We'll handle parallelization ourselves
                'metadata_file': None,
                'min_length': None,
                "output_format": "markdown",
                "disable_image_extraction": True,
                # "layout_batch_size": 64,
                # "recognition_batch_size": 64,
                # "detection_batch_size": 64,
                # "table_rec_batch_size": 64,
                # "texify_batch_size": 32,
            })

            @log_timing("PDF Splitting")
            def split_pdf_into_chunks():
                # Split PDF into chunks
                reader = PdfReader(file_path)
                total_pages = len(reader.pages)
                print(f"Total pages: {total_pages}")
                
                chunk_files = []
                for i in range(0, total_pages, CHUNK_SIZE):
                    writer = PdfWriter()
                    end_page = min(i + CHUNK_SIZE, total_pages)
                    for page_num in range(i, end_page):
                        writer.add_page(reader.pages[page_num])
                        
                    chunk_path = os.path.join(data_dir, f'chunk_{i//CHUNK_SIZE}.pdf')
                    with open(chunk_path, 'wb') as f:
                        writer.write(f)
                    chunk_files.append(chunk_path)
                    
                print(f"Split into {len(chunk_files)} chunks of {CHUNK_SIZE} pages each")
                return chunk_files, total_pages

            chunk_files, total_pages = split_pdf_into_chunks()

            @log_timing("Parallel Processing")
            def process_chunks_parallel():
                # Use the pre-initialized process pool
                global process_pool
                if process_pool is None:
                    initialize_process_pool(model_dict)

                print("Submitting chunks to pre-initialized workers...")
                futures = [
                    process_pool.submit(
                        process_chunk,
                        chunk_path=chunk,
                        config_parser=config_parser
                    ) for chunk in chunk_files
                ]
                return [f.result() for f in futures]

            results = process_chunks_parallel()
            
            if not all(results):
                raise Exception("One or more chunks failed to process")
                
            @log_timing("Results Merging")
            def merge_results():
                # Merge the results
                merge_markdown_files(
                    processed_dir,
                    os.path.join(processed_dir, file_name.removesuffix('.pdf'), f"{file_name.removesuffix('.pdf')}.md")
                )

            merge_results()
            
            # Process the final markdown
            # print("Cleanup started")
            source_md = os.path.join(processed_dir, file_name.removesuffix('.pdf'), f"{file_name.removesuffix('.pdf')}.md")
            # cleaned_md = os.path.join(cleaned_dir, f"{file_name.removesuffix('.pdf')}.md")
            # process_markdown_file(source_md)
            
            # Upload to GCS
            if not upload_to_gcs(os.environ.get("GCS_BUCKET"), bucket_path, source_md):
                print("Failed to upload to GCS")
                return False
                
            # Publish to Pub/Sub topics
            try:
                publisher = pubsub_v1.PublisherClient()
                
                # Prepare topics
                project_id = os.environ.get("GCP_PROJECT_ID")
                topic1_path = publisher.topic_path(project_id, "document-analysis-topic")
                topic2_path = publisher.topic_path(project_id, "qdrant-ingester-topic")
                
                # Encode message
                message_bytes = json.dumps(message_data).encode('utf-8')
                
                # Publish to both topics
                publisher.publish(topic1_path, message_bytes)
                publisher.publish(topic2_path, message_bytes)
                
                print("Successfully published to regulation topics")         
            except Exception as e:
                print(f"Failed to publish to Pub/Sub topics: {e}")
                return False

            # Calculate and print timing statistics
            end_time = time.time()
            total_duration = end_time - start_time
            file_size = os.path.getsize(file_path) / (1024 * 1024)  # Convert to MB
            
            print("\nProcessing Statistics:")
            print("=" * 55)
            print("File Information:")
            print(f"File Name: {file_name}")
            print(f"File Size: {file_size:.2f} MB")
            print(f"Total Pages: {total_pages}")
            print(f"Chunks: {len(chunk_files)} ({CHUNK_SIZE} page(s) per chunk)")
            
            print("\nTiming Breakdown:")
            print(f"{'Stage':<30} {'Duration (s)':<15} {'Percentage':<10}")
            print("-" * 55)
            for stage, duration in processing_times.items():
                percentage = (duration / total_duration) * 100
                print(f"{stage:<30} {duration:>14.2f}s {percentage:>9.1f}%")
            
            print("\nPerformance Metrics:")
            print("-" * 55)
            print(f"Total Processing Time: {total_duration:.2f} seconds")
            print(f"Average Time per Page: {total_duration/total_pages:.2f} seconds")
            print(f"Pages per Second: {total_pages/total_duration:.2f}")
            print(f"MB per Second: {file_size/total_duration:.2f}")
            print("=" * 55)
                
            print(f'Finished processing {file_name}')
            return True
            
        except Exception as e:
            print(f"Error during batch processing: {str(e)}, message_data: {message_data}")
            # Store the error message
            process_message.last_error = str(e)
            return False


def process_message_org(message_data: dict, model_dict) -> bool:
    # Initialize the last_error as None at the start
    process_message.last_error = None
    
    print(f"processing message: {message_data}")
    
    import tempfile
    
    # Use temporary directories that auto-cleanup
    with tempfile.TemporaryDirectory() as temp_dir:
        # Setup configuration before any processing
        config_parser = ConfigParser({
            'output_dir': os.path.join(temp_dir, 'processed'),  # Use temp directory
            'disable_multiprocessing': True,
            'metadata_file': None,
            'min_length': None,
            "output_format": "markdown",
            "disable_image_extraction": True
        })
        
        config_dict = config_parser.generate_config_dict()
        config_dict["pdftext_workers"] = 1
        
        converter = PdfConverter(
            config=config_dict,
            artifact_dict=model_dict,
            processor_list=config_parser.get_processors(),
            renderer=config_parser.get_renderer()
        )
        
        try:
            bucket_path = message_data.get('path')
            bucket_name = message_data.get('bucket_name')
            file_name = message_data.get('file_name')
            download_url = message_data.get('download_url')
            if not download_url:
                print("Invalid message format: missing download_url")
                return False
                
            # Create subdirectories in temp dir
            data_dir = os.path.join(temp_dir, 'data')
            processed_dir = os.path.join(temp_dir, 'processed')
            os.makedirs(data_dir, exist_ok=True)
            os.makedirs(processed_dir, exist_ok=True)
            
            file_path = os.path.join(data_dir, file_name)
            
            if not download_from_url(download_url, file_path):
                print("Failed to download file from URL")
                return False
                
            # Verify file exists after download
            if not os.path.exists(file_path):
                print(f"File not found after download at: {file_path}")
                return False
                
            print(f"File downloaded successfully, size: {os.path.getsize(file_path)} bytes")
            
            if split_pdf(file_path, data_dir):
                # Process split files
                print(f"Original file exists before removal: {os.path.exists(file_path)}")
                os.remove(file_path)  # Remove original file
                split_files = [f for f in os.listdir(data_dir) if f.startswith('split_')]
                print(f"Found {len(split_files)} split files")
                
                for split_file in split_files:
                    split_path = os.path.join(data_dir, split_file)
                    if not os.path.exists(split_path):
                        print(f"Split file not found at: {split_path}")
                        return False
                        
                    print(f"Processing split file at: {split_path}, size: {os.path.getsize(split_path)} bytes")
                    rendered = converter(split_path)
                    out_folder = config_parser.get_output_folder(split_path)
                    save_output(rendered, out_folder, config_parser.get_base_filename(split_path))
                
                # Merge the results
                merge_markdown_files(
                    processed_dir,
                    f'{processed_dir}/{file_name.removesuffix(".pdf")}/{file_name.removesuffix(".pdf")}.md'
                )
            else:
                # Process single file
                print(f"Processing single file at: {file_path}, size: {os.path.getsize(file_path)} bytes")
                rendered = converter(file_path)
                out_folder = config_parser.get_output_folder(file_path)
                save_output(rendered, out_folder, config_parser.get_base_filename(file_path))
            
            # Process the final markdown
            source_md = f'{processed_dir}/{file_name.removesuffix(".pdf")}/{file_name.removesuffix(".pdf")}.md'
            if not os.path.exists(source_md):
                print(f"Source markdown file not found at expected path: {source_md}")
                return False
                
            
            # Upload directly to GCS from processed markdown
            try:
                bucket = storage_client.bucket(bucket_name)
                blob = bucket.blob(f'{bucket_path.removesuffix(".pdf")}.md')
                blob.upload_from_filename(source_md)
            except Exception as e:
                print(f"Error uploading object {bucket_path} to bucket {bucket_name}. {e}")
                return False
                
            # Publish to Pub/Sub topics
            try:
                publisher = pubsub_v1.PublisherClient()
                
                construct_message_data = {
                    "path": bucket_path,
                    "bucket_name": bucket_name,
                    "file_name": file_name,
                    "case_management": True,
                    "metadata": {
                        "file_name": file_name,
                        "file_path": bucket_path,
                    },
                    "collection_name": message_data.get("collectionName"),
                    "project_id": message_data.get("projectId"),
                    "file_id": message_data.get("fileId")
                }
                # Publish to qdrant-ingester-topic
                qdrant_topic_path = publisher.topic_path(
                    os.environ.get("GCP_PROJECT_ID"), 
                    "qdrant-ingester-topic"
                )
                message_bytes = json.dumps(construct_message_data).encode('utf-8')
                publisher.publish(qdrant_topic_path, message_bytes)
                print("Successfully published to qdrant-ingester-topic")
                
            except Exception as e:
                print(f"Failed to publish to Pub/Sub topics: {e}")
                return False
                
            print(f'Finished processing {file_name}')
            return True
            
        except Exception as e:
            print(f"Error during batch processing: {str(e)}")
            process_message.last_error = str(e)
            return False
    # Temporary directory is automatically cleaned up here


def process_message_contract_vault(message_data: dict, model_dict) -> bool:
    # Initialize the last_error as None at the start
    process_message.last_error = None
    
    print(f"processing message: {message_data}")
    
    import tempfile
    
    # Use temporary directories that auto-cleanup
    with tempfile.TemporaryDirectory() as temp_dir:
        # Setup configuration before any processing
        config_parser = ConfigParser({
            'output_dir': os.path.join(temp_dir, 'processed'),  # Use temp directory
            'disable_multiprocessing': True,
            'metadata_file': None,
            'min_length': None,
            "output_format": "markdown",
            "disable_image_extraction": True
        })
        
        config_dict = config_parser.generate_config_dict()
        config_dict["pdftext_workers"] = 1
        
        converter = PdfConverter(
            config=config_dict,
            artifact_dict=model_dict,
            processor_list=config_parser.get_processors(),
            renderer=config_parser.get_renderer()
        )
        
        try:
            bucket_path = message_data.get('path')
            bucket_name = message_data.get('bucket_name')
            file_name = message_data.get('file_name')
            download_url = message_data.get('download_url')
            if not download_url:
                print("Invalid message format: missing download_url")
                return False
                
            # Create subdirectories in temp dir
            data_dir = os.path.join(temp_dir, 'data')
            processed_dir = os.path.join(temp_dir, 'processed')
            os.makedirs(data_dir, exist_ok=True)
            os.makedirs(processed_dir, exist_ok=True)
            
            file_path = os.path.join(data_dir, file_name)
            
            if not download_from_url(download_url, file_path):
                print("Failed to download file from URL")
                return False
                
            # Verify file exists after download
            if not os.path.exists(file_path):
                print(f"File not found after download at: {file_path}")
                return False
                
            print(f"File downloaded successfully, size: {os.path.getsize(file_path)} bytes")
            
            if split_pdf(file_path, data_dir):
                # Process split files
                print(f"Original file exists before removal: {os.path.exists(file_path)}")
                os.remove(file_path)  # Remove original file
                split_files = [f for f in os.listdir(data_dir) if f.startswith('split_')]
                print(f"Found {len(split_files)} split files")
                
                for split_file in split_files:
                    split_path = os.path.join(data_dir, split_file)
                    if not os.path.exists(split_path):
                        print(f"Split file not found at: {split_path}")
                        return False
                        
                    print(f"Processing split file at: {split_path}, size: {os.path.getsize(split_path)} bytes")
                    rendered = converter(split_path)
                    out_folder = config_parser.get_output_folder(split_path)
                    save_output(rendered, out_folder, config_parser.get_base_filename(split_path))
                
                # Merge the results
                merge_markdown_files(
                    processed_dir,
                    f'{processed_dir}/{file_name.removesuffix(".pdf")}/{file_name.removesuffix(".pdf")}.md'
                )
            else:
                # Process single file
                print(f"Processing single file at: {file_path}, size: {os.path.getsize(file_path)} bytes")
                rendered = converter(file_path)
                out_folder = config_parser.get_output_folder(file_path)
                save_output(rendered, out_folder, config_parser.get_base_filename(file_path))
            
            # Process the final markdown
            source_md = f'{processed_dir}/{file_name.removesuffix(".pdf")}/{file_name.removesuffix(".pdf")}.md'
            if not os.path.exists(source_md):
                print(f"Source markdown file not found at expected path: {source_md}")
                return False
                
            
            # Upload directly to GCS from processed markdown
            try:
                bucket = storage_client.bucket(bucket_name)
                blob = bucket.blob(f'{bucket_path.removesuffix(".pdf")}.md')
                blob.upload_from_filename(source_md)
            except Exception as e:
                print(f"Error uploading object {bucket_path} to bucket {bucket_name}. {e}")
                return False
                
            # Publish to Pub/Sub topics
            try:
                publisher = pubsub_v1.PublisherClient()
                
                construct_message_data = {
                    "file_path": bucket_path,
                    "bucket_name": bucket_name,
                    "file_name": file_name,
                    "documentId": message_data.get("fileId")
                }
                # Publish to processed-docs-topic
                qdrant_topic_path = publisher.topic_path(
                    os.environ.get("GCP_PROJECT_ID"), 
                    "processed-docs-topic"
                )
                message_bytes = json.dumps(construct_message_data).encode('utf-8')
                publisher.publish(qdrant_topic_path, message_bytes)
                print("Successfully published to processed-docs-topic")
                
            except Exception as e:
                print(f"Failed to publish to Pub/Sub topics: {e}")
                return False
                
            print(f'Finished processing {file_name}')
            return True
            
        except Exception as e:
            print(f"Error during batch processing: {str(e)}")
            process_message.last_error = str(e)
            return False
    # Temporary directory is automatically cleaned up here

class SignalHandler:
    def __init__(self):
        self.received_signal = False
        signal(SIGINT, self._signal_handler)
        signal(SIGTERM, self._signal_handler)

    def _signal_handler(self, signal, frame):
        print(f"handling signal {signal}, exiting gracefully")
        self.received_signal = True


def wait(seconds: int):
    def decorator(fun):
        last_run = time.monotonic()

        def new_fun(*args, **kwargs):
            nonlocal last_run
            now = time.monotonic()
            if time.monotonic() - last_run > seconds:
                last_run = now
                return fun(*args, **kwargs)

        return new_fun


    return decorator


@wait(seconds=15)
def send_queue_metrics(sqs_queue) -> None:
    print("sending queue metrics")
    statsd.gauge(
        "sqs.queue.message_count",
        queue_length(sqs_queue),
        tags=[f"queue:{queue_name(sqs_queue)}"],
    )


def queue_length(sqs_queue) -> int:
    sqs_queue.load()
    return int(sqs_queue.attributes["ApproximateNumberOfMessages"])


def queue_name(sqs_queue) -> str:
    sqs_queue.load()
    return sqs_queue.attributes["QueueArn"].split(":")[-1]

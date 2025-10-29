import pandas as pd
from google import genai
from google.genai import types
import time
from typing import Optional, Dict, Any
from tqdm import tqdm
import os


def submit_batch_inline(client, requests, model:str, config:dict):
    return client.batches.create(
        model=model,
        src=requests,
        config=config
    )

def build_request(df, base_prompt, input_colname):
    # Prepare batch requests with index tracking
    inline_requests = []
    index_mapping = {}  # Maps request index to dataframe index
    
    for idx, (df_idx, row) in tqdm(enumerate(df.iterrows()), total = len(df)):
        # Combine base prompt with the specific text
        full_prompt = f"{base_prompt}\n\nInput: {row[input_colname]}"
        # Create request in the required format
        request = {
            'contents': [{
                'parts': [{'text': full_prompt}],
                'role': 'user'
            }]
        }
        inline_requests.append(request)
        index_mapping[idx] = df_idx
    
    return inline_requests

def monitor_job(client, job):
    job_name = job.name
    print()
    print(f"Polling status for job: {job_name}")
    while True:
        batch_job_inline = client.batches.get(name=job_name)
        if batch_job_inline.state.name in ('JOB_STATE_SUCCEEDED', 'JOB_STATE_FAILED', 'JOB_STATE_CANCELLED', 'JOB_STATE_EXPIRED'):
            break
        print(f"Job not finished. Current state: {batch_job_inline.state.name}. Waiting 30 seconds...")
        time.sleep(30)
    print(f"Job finished with state: {batch_job_inline.state.name}")
    if batch_job_inline.state.name == 'JOB_STATE_SUCCEEDED':
        return True
    else:
        return False

def retrieve(job):

    for i, inline_response in enumerate(job.dest.inlined_responses, start=1):
        print(f"\n--- Response {i} ---")

        # Check for a successful response
        if inline_response.response:
            # The .text property is a shortcut to the generated text.
            print(inline_response.response.text)

        return [inline_response.response.text for i, inline_response in enumerate(job.dest.inlined_responses, start=1)]


def process_batch_with_gemini_api(
    df: pd.DataFrame,
    base_prompt: str,
    api_key_env_var: str,
    input_colname: str,
    output_colname: str,
    model_name: str = "models/gemini-2.5-flash",
    batch_name: str = "dataframe-batch-job",
    timeout_seconds: int = 300,
    poll_interval: int = 5
) -> pd.DataFrame:
    """
    Process a dataframe using Gemini's batch API.
    
    Args:
        df: Input dataframe with 'indications_text' column
        base_prompt: Base prompt to use as context
        api_key: Google AI API key
        input_colname: name of the input column
        output_colname: Name for the output column
        model_name: Gemini model to use (e.g., "models/gemini-2.0-flash-exp")
        batch_name: Display name for the batch job
        temperature: Model temperature for response generation
        max_output_tokens: Maximum tokens in response
        timeout_seconds: Maximum time to wait for batch completion
        poll_interval: Seconds between status checks
    
    Returns:
        DataFrame with added output column containing model responses
    """
    
    # Initialize client with API key
    client = genai.Client(api_key=os.getenv(api_key_env_var))
    
    # Setup config
    config = {'display_name':batch_name}

    # Create a copy of the dataframe
    result_df = df.copy()
    print(f"Creating batch job with {len(df)} requests...")
    inline_requests = build_request(df, base_prompt, input_colname)
    print("created job")
    print("submitting batch job")
    batch_job = submit_batch_inline(client, inline_requests, model_name, config)
    print("submitted batch job")
    name = batch_job.name
    success = monitor_job(client, batch_job)
    
    if success:
        batch_job_inline = client.batches.get(name=name)
        results = retrieve(batch_job_inline)
    else:
        results = []
        # do not do

    
    # # Add responses to dataframe
    result_df[output_colname] = results
    return result_df


def process_large_dataframe_in_batches(
    df: pd.DataFrame,
    base_prompt: str,
    api_key: str,
    output_colname: str,
    batch_size: int = 1000,
    **kwargs
) -> pd.DataFrame:
    """
    Process large dataframe by splitting into multiple batch jobs.
    
    Args:
        df: Input dataframe with 'indications_text' column
        base_prompt: Base prompt to use as context
        api_key: Google AI API key
        output_colname: Name for the output column
        batch_size: Number of rows per batch job (Gemini may have limits)
        **kwargs: Additional arguments for process_batch_with_gemini_api
    
    Returns:
        DataFrame with added output column containing model responses
    """
    
    result_chunks = []
    total_batches = (len(df) - 1) // batch_size + 1
    
    for i in range(0, len(df), batch_size):
        batch_num = i // batch_size + 1
        print(f"\nProcessing batch {batch_num}/{total_batches}")
        
        chunk = df.iloc[i:i+batch_size]
        
        # Update batch name to include batch number
        batch_kwargs = kwargs.copy()
        batch_kwargs['batch_name'] = f"{kwargs.get('batch_name', 'dataframe-batch')}-{batch_num}"
        
        processed_chunk = process_batch_with_gemini_api(
            df=chunk,
            base_prompt=base_prompt,
            api_key=api_key,
            output_colname=output_colname,
            **batch_kwargs
        )
        
        result_chunks.append(processed_chunk)
        
        # Optional: Add delay between batches to avoid rate limiting
        if batch_num < total_batches:
            print("Waiting before next batch...")
            time.sleep(5)
    
    return pd.concat(result_chunks, ignore_index=True)


# Helper function to check batch job status
def check_batch_status(api_key: str, batch_name: Optional[str] = None):
    """
    Check the status of batch jobs.
    
    Args:
        api_key: Google AI API key
        batch_name: Optional specific batch name to check
    """
    client = genai.Client(api_key=api_key)
    
    try:
        batches = client.batches.list()
        
        for batch in batches:
            if batch_name is None or batch.name == batch_name:
                print(f"\nBatch: {batch.name}")
                print(f"  Display Name: {batch.display_name if hasattr(batch, 'display_name') else 'N/A'}")
                print(f"  State: {batch.state}")
                print(f"  Created: {batch.create_time if hasattr(batch, 'create_time') else 'N/A'}")
                
                if hasattr(batch, 'request_count'):
                    print(f"  Total Requests: {batch.request_count}")
                if hasattr(batch, 'completed_request_count'):
                    print(f"  Completed: {batch.completed_request_count}")
                
                if batch_name:
                    break
    except Exception as e:
        print(f"Error checking batch status: {str(e)}")


# Example usage
if __name__ == "__main__":
    # Sample dataframe
    sample_df = pd.DataFrame({
        'indications_text': [
            'Patient presents with fever and cough',
            'Chronic back pain for 3 months',
            'Headache and dizziness',
            'Shortness of breath and chest pain',
            'Skin rash and itching'
        ]
    })
    
    # Example base prompt
    base_prompt = """You are a medical assistant. Analyze the following medical indication 
    and provide a brief assessment of potential conditions to consider. Keep response under 100 words."""
    
    # Your API key
    api_key = "YOUR_API_KEY_HERE"
    
    # Process the dataframe using batch API
    result = process_batch_with_gemini_api(
        df=sample_df,
        base_prompt=base_prompt,
        api_key=api_key,
        output_colname="gemini_response",
        model_name="models/gemini-2.0-flash-exp",
        batch_name="medical-indications-batch",
        temperature=0.7,
        max_output_tokens=150,
        timeout_seconds=300
    )
    
    # Display results
    print("\nResults:")
    for idx, row in result.iterrows():
        print(f"\nIndication: {row['indications_text']}")
        print(f"Response: {row['gemini_response']}")
        print("-" * 50)
    
    # Optional: Check batch status
    # check_batch_status(api_key=api_key)
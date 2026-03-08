"""
AWS Lambda Stream Processor for Fraud Detection using Google Gemini AI

This module implements a Lambda function that processes streaming transaction data from AWS Kinesis,
enriches it with fraud detection scores using Google's Gemini 2.5 Flash API, and persists the
results to AWS S3 in a partitioned format.

The Lambda function:
1. Decodes and parses incoming Kinesis records
2. Sends transactions to Google Gemini API for fraud analysis
3. Enriches records with fraud scores and AI reasoning
4. Stores enriched records in S3 with date-based partitioning (YYYY/MM/DD format)

Environment Variables Required:
    - GEMINI_API_KEY: Google Generative Language API key for Gemini access
    - AWS credentials (typically provided via Lambda execution role)

Exceptions:
    - HTTPError: API request failures are logged and gracefully handled
    - General exceptions: Caught and logged to prevent Lambda failure
"""

import json
import base64
import boto3
import os
import urllib.request
import urllib.error
from datetime import datetime

# Initialize S3 client for writing enriched records
s3_client = boto3.client('s3')

# MUST MATCH YOUR EXACT BUCKET NAME - Update this to your S3 bucket
BUCKET_NAME = "your-unique-bucket-name"

# Retrieve Gemini API key from environment variables
# Strip whitespace to prevent authentication issues
GEMINI_API_KEY = os.environ.get('GEMINI_API_KEY', '').strip()

def lambda_handler(event, context):
    """
    AWS Lambda entry point for processing Kinesis stream records.
    
    This handler processes a batch of records from a Kinesis stream, enriches them with
    fraud detection scores from Google Gemini API, and persists the enriched data to S3.
    
    Args:
        event (dict): Lambda event containing Kinesis record(s). Structure:
            {
                'Records': [
                    {
                        'kinesis': {
                            'data': '<base64-encoded-json-payload>'
                        },
                        ...
                    }
                ]
            }
        context (LambdaContext): Lambda context object (unused but required by AWS Lambda)
    
    Returns:
        dict: Response object with statusCode and body containing a message
            {
                'statusCode': 200,
                'body': 'AI Enrichment successful.' or error message
            }
    
    Note:
        - Empty record batches return successfully (no-op)
        - API failures are logged but don't cause Lambda to fail
        - Records are stored in S3 with automatic timestamp and batching
    """
    
    # Extract and decode Kinesis records
    # Each record's data is base64-encoded JSON that must be decoded
    raw_records = []
    
    for record in event['Records']:
        payload_bytes = base64.b64decode(record['kinesis']['data'])
        payload = json.loads(payload_bytes.decode('utf-8'))
        raw_records.append(payload)

    # Return early if no records to process (prevents unnecessary API calls)
    if not raw_records:
        return {'statusCode': 200, 'body': 'No records found.'}

    # Construct prompt for Gemini AI fraud detection analysis
    # The prompt explicitly requests structured JSON output to ensure parsing compatibility
    prompt_text = f"""
    You are an expert fraud detection AI. Analyze the following {len(raw_records)} transactions.
    Return ONLY a raw JSON array of objects. Do NOT wrap the response in ```json or any markdown blocks.
    Each object must contain exactly three keys:
    - "transaction_id": (string, matching the input)
    - "fraud_score": (integer from 0 to 100, where 100 is highly anomalous)
    - "ai_reason": (string, a strict 1-sentence explanation of why it is normal or suspicious)
    
    Transactions to analyze:
    {json.dumps(raw_records)}
    """

    # Construct the Google Generative Language API endpoint URL with API key
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={GEMINI_API_KEY}"
    
    # Build the API request payload following Google's generative AI format
    api_payload = {
        "contents": [{"parts": [{"text": prompt_text}]}]
    }
    
    # Create HTTP request with proper headers and JSON-encoded payload
    req = urllib.request.Request(
        url, 
        data=json.dumps(api_payload).encode('utf-8'), 
        headers={'Content-Type': 'application/json'}
    )
    
    # Call Google Gemini API and parse the response
    # Includes error handling for both API errors and JSON parsing failures
    try:
        # Execute the API request and parse the response
        with urllib.request.urlopen(req) as response:
            result = json.loads(response.read().decode('utf-8'))
            # Extract the AI-generated text from the nested response structure
            ai_response_text = result['candidates'][0]['content']['parts'][0]['text']
            
            # Clean markdown code block formatting if present in response
            # Gemini sometimes wraps JSON in ```json markers despite instructions
            cleaned_text = ai_response_text.replace('```json', '').replace('```', '').strip()
            ai_scores = json.loads(cleaned_text) 
            
    except urllib.error.HTTPError as e:
        # Handle HTTP errors from the Gemini API (authentication, rate limits, etc.)
        # THIS IS THE MAGIC DIAGNOSTIC LINE - reads error body for debugging
        error_details = e.read().decode('utf-8')
        print(f"❌ Google API Rejected Request: {error_details}")
        ai_scores = [] 
    except Exception as e:
        # Catch-all for JSON parsing errors, network issues, or other unexpected failures
        print(f"❌ General Error: {e}")
        ai_scores = [] 

    # Enrich raw records with AI fraud detection results
    enriched_records = []
    
    # Create a lookup dictionary mapping transaction IDs to their AI scores
    # Provides O(1) lookup performance for enrichment matching
    ai_lookup = {item.get('transaction_id'): item for item in ai_scores if isinstance(item, dict)}

    # Iterate through each raw record and add AI enrichment data
    for record in raw_records:
        tx_id = record['transaction_id']
        # Retrieve AI data for this transaction, or use defaults if AI evaluation failed
        ai_data = ai_lookup.get(tx_id, {"fraud_score": 0, "ai_reason": "AI evaluation failed"})
        
        # Add fraud detection fields to the record
        record['fraud_score'] = ai_data.get('fraud_score', 0)
        record['ai_reason'] = ai_data.get('ai_reason', 'None')
        # Timestamp the record with ingestion time for audit trails
        record['ingested_at'] = datetime.utcnow().isoformat()
        
        enriched_records.append(record)

    # Write enriched records to S3 for downstream analysis and archival
    if enriched_records:
        # Convert records to JSONL format (one JSON object per line)
        # This format is efficient for streaming data warehouses like Athena
        file_content = "\n".join([json.dumps(rec) for rec in enriched_records])
        
        # Generate timestamp and create partitioned S3 path
        # Partitioning by date enables efficient queries and data lifecycle policies
        now = datetime.utcnow()
        partition_path = f"enriched-stream/year={now.year}/month={now.strftime('%m')}/day={now.strftime('%d')}/"
        # Include microseconds in filename to ensure uniqueness across rapid Lambda invocations
        file_name = f"{partition_path}batch_{now.strftime('%H%M%S%f')}.jsonl"
        
        # Upload the enriched batch to S3
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=file_name,
            Body=file_content
        )
        # Log successful completion with record count for monitoring
        print(f"✅ AI successfully evaluated and wrote {len(enriched_records)} enriched records.")
        
    # Return success response to Lambda orchestration
    return {
        'statusCode': 200,
        'body': json.dumps('AI Enrichment successful.')
    }
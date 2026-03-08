"""
Advanced Fraud-Scenario Transaction Producer for AWS Kinesis

This module generates synthetic financial transaction data with realistic fraud patterns
and streams it to an AWS Kinesis data stream for real-time processing and analysis.

Key Features:
    - Generates mixed normal and suspicious transaction patterns
    - 25% fraudulent scenarios to provide balanced training data
    - Simulates realistic merchant categories, locations, and transaction amounts
    - Streams transactions to Kinesis at 1 transaction per second
    - Emulates both standard business hours and off-hours transactions

Configuration:
    - AWS Region: ap-south-1 (Mumbai)
    - Stream Name: financial-transactions-stream
    - Data Format: JSON records with transaction details

Usage:
    python advanced_producer.py
    Press Ctrl+C to stop the producer

Dependencies:
    - boto3: AWS SDK for Python
    - datetime: Python standard library for timestamp generation
"""

import json
import time
import random
import uuid
import boto3
from datetime import datetime, timedelta

# Initialize AWS Kinesis client for the Mumbai region
# This client will be used to stream transaction data to the Kinesis stream
kinesis_client = boto3.client('kinesis', region_name='ap-south-1')

# Name of the Kinesis data stream where transactions will be published
# Must match the stream created by create_stream.py
stream_name = 'financial-transactions-stream'

def generate_transaction():
    """
    Generates a synthetic financial transaction record with realistic fraud patterns.
    
    This function creates transaction data that mimics real-world payment scenarios,
    including both normal everyday purchases and suspicious high-risk transactions.
    The mix of patterns allows machine learning models to learn fraud indicators.
    
    Fraud Scenarios (25% probability):
        - Large transaction amounts ($3,000-$10,000)
        - High-risk merchant categories (electronics, jewelry, crypto, international wire)
        - Unusual locations (international, unknown IP)
        - Off-hours timestamps (1-4 AM)
    
    Normal Scenarios (75% probability):
        - Typical transaction amounts ($5-$150)
        - Everyday merchant categories (groceries, coffee, gas, subscriptions)
        - Common domestic locations (India-based cities)
        - Current timestamp (business hours)
    
    Returns:
        dict: A transaction record with the following keys:
            - transaction_id (str): Unique UUID for the transaction
            - customer_id (str): Anonymized customer identifier in format CUST-XXXX
            - amount (float): Transaction amount in base currency
            - merchant_category (str): Type of merchant/business
            - location (str): Geographic location or origin of transaction
            - timestamp (str): ISO8601 formatted timestamp
            - payment_method (str): Payment type (credit card, debit card, UPI)
    
    Example:
        {
            'transaction_id': '550e8400-e29b-41d4-a716-446655440000',
            'customer_id': 'CUST-5432',
            'amount': 125.50,
            'merchant_category': 'Groceries',
            'location': 'Pune, IN',
            'timestamp': '2026-03-08T10:30:45.123456',
            'payment_method': 'Credit Card'
        }
    """
    
    # 75% chance of generating a fraudulent scenario, 25% chance of normal transaction
    # This ratio provides a realistic sample for fraud detection model training
    is_fraud_scenario = random.random() < 0.25 
    
    # Generate anonymized customer ID (4-digit numeric identifier)
    customer_id = f"CUST-{random.randint(1000, 9999)}"
    
    if is_fraud_scenario:
        # FRAUDULENT TRANSACTION PROFILE
        # High-value amount indicative of unusual purchases
        amount = round(random.uniform(3000.0, 10000.0), 2)
        
        # Merchant categories associated with fraudulent activity or money laundering
        merchant_category = random.choice([
            "High-End Electronics",      # High-value resale items
            "Jewelry",                  # Portable valuables
            "International Wire",       # Cross-border fund transfers
            "Luxury Travel",            # High-ticket purchases
            "Cryptocurrency Exchange"   # Difficult to trace transactions
        ])
        
        # Geographic locations that raise red flags (international, unknown origin)
        location = random.choice([
            "Moscow, RU",               # Geopolitical risk indicator
            "Lagos, NG",                # High-crime region
            "Unknown IP",               # VPN/proxy usage
            "San Francisco, US",        # Tech hub (potential money laundering)
            "Tokyo, JP"                 # International jurisdiction
        ])
        
        # Off-hours transactions (1-4 AM) are statistically more suspicious
        # Generate historical timestamp to simulate past fraudulent activity
        tx_time = (datetime.utcnow() - timedelta(hours=random.randint(1, 4))).isoformat() 
    else:
        # NORMAL TRANSACTION PROFILE
        # Typical consumer spending pattern (daily purchases)
        amount = round(random.uniform(5.0, 150.0), 2)
        
        # Common merchant categories for legitimate retail/subscription transactions
        merchant_category = random.choice([
            "Groceries",               # Essential goods
            "Coffee Shop",             # Small discretionary spending
            "Gas Station",             # Routine fuel purchases
            "Streaming Subscription"   # Regular subscription service
        ])
        
        # Domestic Indian locations - consistent with normal cardholder profile
        location = random.choice([
            "Pune, IN",                # Regular cardholder residence area
            "Mumbai, IN",              # Major metropolitan hub
            "Bangalore, IN"            # Tech industry hub
        ])
        
        # Current timestamp - transactions appear in real-time
        tx_time = datetime.utcnow().isoformat()

    # Return structured transaction record with all required fields
    return {
        "transaction_id": str(uuid.uuid4()),        # Globally unique transaction identifier
        "customer_id": customer_id,                  # Anonymized customer reference
        "amount": amount,                            # Transaction amount (currency units)
        "merchant_category": merchant_category,      # Business category classification
        "location": location,                        # Geographic or network origin
        "timestamp": tx_time,                        # ISO8601 formatted transaction time
        "payment_method": random.choice([
            "Credit Card",                           # Traditional credit payment
            "Debit Card",                            # Direct account debit
            "UPI"                                    # India-specific digital payment
        ])
    }

# ============================================================================
# MAIN EXECUTION - Continuous Transaction Stream Producer
# ============================================================================

print(f"Starting Advanced Fraud Generator... targeting stream: {stream_name}")
print("Press Ctrl+C to stop.")
print("-" * 80)

try:
    # Continuous loop to generate and stream transactions
    while True:
        # Generate a synthetic transaction record
        live_data = generate_transaction()
        
        # Serialize transaction to JSON and encode as UTF-8 bytes for Kinesis
        data_bytes = json.dumps(live_data).encode('utf-8')
        
        # Send record to Kinesis stream
        # PartitionKey routes records to shards based on customer_id for ordering
        response = kinesis_client.put_record(
            StreamName=stream_name,
            Data=data_bytes,
            PartitionKey=live_data['customer_id']
        )
        
        # Display transaction summary for real-time monitoring
        # Quick visual indicator helps identify fraud patterns in live output
        status_flag = "🚨 SUSPICIOUS" if live_data['amount'] > 1000 else "✅ NORMAL"
        print(
            f"{status_flag} | "
            f"Sent ${live_data['amount']:7.2f} at {live_data['merchant_category']:25} | "
            f"ID: {live_data['transaction_id'][:8]}"
        )
        
        # Enforce 1 transaction per second rate to simulate realistic traffic
        time.sleep(1)

except KeyboardInterrupt:
    # Graceful shutdown when user presses Ctrl+C
    print("\n" + "-" * 80)
    print("Advanced generator stopped by user.")
except Exception as e:
    # Handle unexpected errors with helpful troubleshooting message
    print("\n" + "-" * 80)
    print(f"❌ Error: {e}")
    print("\n   Troubleshooting:")
    print("   1. Ensure create_stream.py has been run to create the Kinesis stream")
    print("   2. Verify AWS credentials are configured (.aws/credentials or environment)")
    print("   3. Check that stream name matches: {}".format(stream_name))
    print("   4. Confirm IAM permissions allow kinesis:PutRecord action")
#!/usr/bin/env python3
"""
Test webhook locally before deploying to Vercel
"""

import json
import os
import sys

# Add the api directory to the path
sys.path.append('/Users/iso_mac2/Documents/GitHub/THF_2025/api')

# Set environment variables for testing
os.environ['APIFY_TOKEN'] = os.environ.get('APIFY_TOKEN', 'your_apify_api_token_placeholder')
os.environ['NOTION_TOKEN'] = os.environ.get('NOTION_TOKEN', 'your_notion_api_token_placeholder')

from webhook_enrichment import WebhookEnrichmentProcessor

def test_webhook_processor():
    """Test the webhook processor with Matt Stevens data"""
    
    print("🧪 TESTING WEBHOOK PROCESSOR LOCALLY")
    print("=" * 50)
    
    # Test payload simulating Notion webhook
    test_payload = {
        "object": "page",
        "id": "258c2a32-df0d-80f3-944f-cf819718d96a",  # Matt Stevens ID from People DB
        "properties": {
            "Name": {
                "title": [{"plain_text": "Matt Stevens"}]
            },
            "Status": {
                "status": {"name": "Working"}
            },
            "Primary Email": {
                "email": "matt@honor.org"
            },
            "LinkedIn Profile": {
                "url": "https://www.linkedin.com/in/matthewpstevens/"
            },
            "Employer": {
                "rich_text": [{"plain_text": "The Honor Foundation"}]
            },
            "Position": {
                "rich_text": [{"plain_text": "CEO"}]
            },
            "Industry": {
                "rich_text": [{"plain_text": "Non-Profit/Veteran Services"}]
            },
            "Military": {
                "checkbox": True
            }
        }
    }
    
    print(f"📋 Test Payload:")
    print(f"   Name: Matt Stevens")
    print(f"   Status: Working")
    print(f"   Email: matt@honor.org")
    print(f"   LinkedIn: https://www.linkedin.com/in/matthewpstevens/")
    print(f"   Company: The Honor Foundation")
    
    # Initialize processor
    processor = WebhookEnrichmentProcessor()
    
    print(f"\n🚀 Processing webhook...")
    
    # Process the webhook
    result = processor.process_webhook(test_payload)
    
    # Display results
    print(f"\n📊 WEBHOOK PROCESSING RESULTS")
    print(f"=" * 50)
    
    if result.get('success'):
        print(f"✅ Overall Success: YES")
        print(f"   Person: {result.get('person_name')}")
        print(f"   Status: {result.get('enrichment_status')}")
        print(f"   Apollo: {'✅' if result.get('apollo_success') else '❌'}")
        print(f"   LinkedIn: {'✅' if result.get('linkedin_success') else '❌'}")
        print(f"   Storage: {'✅' if result.get('storage_success') else '❌'}")
        print(f"   Timestamp: {result.get('timestamp')}")
    else:
        print(f"❌ Overall Success: NO")
        print(f"   Error: {result.get('error')}")
        print(f"   Status Code: {result.get('status')}")
    
    return result

def test_status_trigger():
    """Test that webhook only triggers on 'Working' status"""
    
    print(f"\n🔍 TESTING STATUS TRIGGER LOGIC")
    print(f"-" * 40)
    
    processor = WebhookEnrichmentProcessor()
    
    # Test with different statuses
    statuses_to_test = ["Working", "Completed", "Failed", "Not Started"]
    
    for status in statuses_to_test:
        test_payload = {
            "object": "page",
            "id": "test-id",
            "properties": {
                "Name": {"title": [{"plain_text": "Test Person"}]},
                "Status": {"status": {"name": status}}
            }
        }
        
        person_info = processor._extract_person_from_webhook(test_payload)
        
        if person_info and status == "Working":
            print(f"   ✅ Status '{status}': Triggers enrichment")
        elif not person_info and status != "Working":
            print(f"   ✅ Status '{status}': Correctly skipped")
        else:
            print(f"   ❌ Status '{status}': Unexpected behavior")

if __name__ == "__main__":
    # Test the webhook processor
    result = test_webhook_processor()
    
    # Test status trigger logic
    test_status_trigger()
    
    print(f"\n🎉 Local webhook testing complete!")
    
    if result.get('success'):
        print(f"✅ Ready to deploy to Vercel")
    else:
        print(f"⚠️  Fix issues before deploying")
#!/usr/bin/env python3
"""
Test if we can manually trigger what Notion should send
"""

import requests
import json

def test_webhook_endpoint():
    """Test the webhook endpoint with simulated Notion payload"""
    
    print("🧪 TESTING NOTION WEBHOOK TRIGGER")
    print("=" * 50)
    
    # Simulated Notion webhook payload
    notion_payload = {
        "object": "page",
        "id": "258c2a32-df0d-80f3-944f-cf819718d96a",  # Matt Stevens' actual ID
        "created_time": "2025-08-23T16:51:00.000Z",
        "last_edited_time": "2025-08-23T17:45:00.000Z",
        "properties": {
            "Name": {
                "id": "title",
                "type": "title",
                "title": [
                    {
                        "type": "text",
                        "text": {"content": "Matt Stevens"},
                        "plain_text": "Matt Stevens"
                    }
                ]
            },
            "Status": {
                "id": "status",
                "type": "status",
                "status": {
                    "id": "working-id",
                    "name": "Working",
                    "color": "yellow"
                }
            },
            "Primary Email": {
                "id": "email",
                "type": "email",
                "email": "matt@honor.org"
            },
            "LinkedIn Profile": {
                "id": "url",
                "type": "url", 
                "url": "https://www.linkedin.com/in/matthewpstevens/"
            },
            "Employer": {
                "id": "rich_text",
                "type": "rich_text",
                "rich_text": [
                    {
                        "type": "text",
                        "text": {"content": "The Honor Foundation"},
                        "plain_text": "The Honor Foundation"
                    }
                ]
            },
            "Position": {
                "id": "rich_text", 
                "type": "rich_text",
                "rich_text": [
                    {
                        "type": "text",
                        "text": {"content": "CEO"},
                        "plain_text": "CEO"
                    }
                ]
            }
        }
    }
    
    # Test endpoints
    endpoints = [
        "https://thf-2025.vercel.app/api/webhook",
        "https://thf-2025.vercel.app/api/webhook.js"
    ]
    
    for endpoint in endpoints:
        print(f"\n🎯 Testing endpoint: {endpoint}")
        
        try:
            # Test GET first
            print("   Testing GET request...")
            get_response = requests.get(endpoint, timeout=10)
            print(f"   GET Status: {get_response.status_code}")
            if get_response.status_code == 200:
                print(f"   GET Response: {get_response.text[:200]}")
            
            # Test POST with payload
            print("   Testing POST request...")
            post_response = requests.post(
                endpoint,
                json=notion_payload,
                headers={"Content-Type": "application/json"},
                timeout=30
            )
            print(f"   POST Status: {post_response.status_code}")
            print(f"   POST Response: {post_response.text[:500]}")
            
            if post_response.status_code == 200:
                print("   ✅ Endpoint is working!")
                return True
                
        except requests.exceptions.RequestException as e:
            print(f"   ❌ Request failed: {e}")
    
    return False

def test_local_webhook():
    """Test sending webhook to local server for debugging"""
    
    print(f"\n🏠 TESTING LOCAL WEBHOOK (if running)")
    print("-" * 40)
    
    # Try localhost
    try:
        response = requests.post(
            "http://localhost:3000/api/webhook",
            json={"test": "data"},
            timeout=5
        )
        print(f"✅ Local server responded: {response.status_code}")
    except:
        print("⚠️  No local server running")

def check_notion_webhook_url():
    """Check if the webhook URL is reachable"""
    
    print(f"\n🔗 CHECKING WEBHOOK URL ACCESSIBILITY")
    print("-" * 40)
    
    urls_to_check = [
        "https://thf-2025.vercel.app",
        "https://thf-2025.vercel.app/api/webhook",
        "https://thf-2025.vercel.app/api/webhook.js"
    ]
    
    for url in urls_to_check:
        try:
            response = requests.get(url, timeout=10)
            print(f"   {url}: {response.status_code}")
        except Exception as e:
            print(f"   {url}: ❌ {str(e)}")

if __name__ == "__main__":
    # Check URL accessibility
    check_notion_webhook_url()
    
    # Test webhook endpoints
    webhook_working = test_webhook_endpoint()
    
    # Test local (optional)
    test_local_webhook()
    
    print(f"\n📋 SUMMARY:")
    if webhook_working:
        print("✅ Webhook endpoint is accessible and working")
        print("🎯 Next: Set up Notion automation to send to this URL")
    else:
        print("❌ Webhook endpoints not working")
        print("🔧 Need to fix Vercel deployment")
    
    print(f"\n📡 WEBHOOK URL FOR NOTION:")
    print(f"https://thf-2025.vercel.app/api/webhook")
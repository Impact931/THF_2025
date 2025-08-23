#!/usr/bin/env python3
"""
Find and list all databases accessible to the integration
"""

import requests
import json

def find_databases(integration_token):
    """Find all databases accessible to the integration"""
    headers = {
        "Authorization": f"Bearer {integration_token}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json"
    }
    
    # Search for databases
    url = "https://api.notion.com/v1/search"
    payload = {
        "filter": {
            "property": "object",
            "value": "database"
        }
    }
    
    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        results = response.json()
        
        print("Found databases:")
        print("="*50)
        
        for db in results.get('results', []):
            title = db.get('title', [{}])[0].get('plain_text', 'Untitled') if db.get('title') else 'Untitled'
            db_id = db.get('id')
            url = db.get('url')
            
            print(f"Title: {title}")
            print(f"ID: {db_id}")
            print(f"URL: {url}")
            print("-" * 30)
            
        if not results.get('results'):
            print("No databases found. The integration might not have access to any databases.")
            print("\nTo fix this:")
            print("1. Go to your Notion workspace")
            print("2. Find the 'People DB' database")
            print("3. Click '...' > 'Add connections' > Select your integration")
            
    except requests.exceptions.RequestException as e:
        print(f"Error searching for databases: {e}")

def main():
    INTEGRATION_TOKEN = "your_notion_token_here"
    find_databases(INTEGRATION_TOKEN)

if __name__ == "__main__":
    main()
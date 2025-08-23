#!/usr/bin/env python3
"""
THF Professional Network Intelligence Tool
Notion API Client for accessing People DB
"""

import requests
import json
import sys
from typing import Dict, List, Any, Optional

class NotionClient:
    def __init__(self, integration_token: str):
        self.integration_token = integration_token
        self.base_url = "https://api.notion.com/v1"
        self.headers = {
            "Authorization": f"Bearer {integration_token}",
            "Notion-Version": "2022-06-28",
            "Content-Type": "application/json"
        }
    
    def get_database(self, database_id: str) -> Optional[Dict[str, Any]]:
        """Retrieve database schema and properties"""
        url = f"{self.base_url}/databases/{database_id}"
        try:
            response = requests.get(url, headers=self.headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error retrieving database: {e}")
            return None
    
    def query_database(self, database_id: str, filter_criteria: Optional[Dict] = None, 
                      sorts: Optional[List[Dict]] = None, page_size: int = 100) -> Optional[Dict[str, Any]]:
        """Query database for records"""
        url = f"{self.base_url}/databases/{database_id}/query"
        
        payload = {
            "page_size": page_size
        }
        
        if filter_criteria:
            payload["filter"] = filter_criteria
        
        if sorts:
            payload["sorts"] = sorts
            
        try:
            response = requests.post(url, headers=self.headers, json=payload)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Error querying database: {e}")
            return None
    
    def analyze_database_structure(self, database_id: str) -> None:
        """Analyze and display database structure"""
        print(f"Analyzing database structure for: {database_id}")
        
        db_info = self.get_database(database_id)
        if not db_info:
            return
            
        print(f"\nDatabase Title: {db_info.get('title', [{}])[0].get('plain_text', 'Unknown')}")
        print(f"Database ID: {db_info.get('id')}")
        print(f"Created: {db_info.get('created_time')}")
        print(f"Last Edited: {db_info.get('last_edited_time')}")
        
        print("\nDatabase Properties:")
        properties = db_info.get('properties', {})
        
        for prop_name, prop_info in properties.items():
            prop_type = prop_info.get('type')
            print(f"  • {prop_name}: {prop_type}")
            
            # Show additional details for specific property types
            if prop_type == 'select':
                options = prop_info.get('select', {}).get('options', [])
                if options:
                    option_names = [opt.get('name') for opt in options]
                    print(f"    Options: {', '.join(option_names)}")
            
            elif prop_type == 'multi_select':
                options = prop_info.get('multi_select', {}).get('options', [])
                if options:
                    option_names = [opt.get('name') for opt in options]
                    print(f"    Options: {', '.join(option_names)}")
            
            elif prop_type == 'relation':
                database_id = prop_info.get('relation', {}).get('database_id')
                if database_id:
                    print(f"    Related to database: {database_id}")

def main():
    # THF Notion Integration Details
    INTEGRATION_TOKEN = "your_notion_token_here"
    PEOPLE_DB_ID = "258c2a32-df0d-80f3-944f-cf819718d96a"
    
    # Initialize client
    client = NotionClient(INTEGRATION_TOKEN)
    
    # Analyze the People DB structure
    client.analyze_database_structure(PEOPLE_DB_ID)
    
    # Get sample records
    print("\n" + "="*50)
    print("SAMPLE RECORDS")
    print("="*50)
    
    results = client.query_database(PEOPLE_DB_ID, page_size=5)
    if results and results.get('results'):
        for i, record in enumerate(results['results'][:3], 1):
            print(f"\nRecord {i}:")
            properties = record.get('properties', {})
            
            for prop_name, prop_value in properties.items():
                prop_type = prop_value.get('type')
                
                # Extract readable value based on property type
                if prop_type == 'title':
                    value = prop_value.get('title', [{}])[0].get('plain_text', 'N/A') if prop_value.get('title') else 'N/A'
                elif prop_type == 'rich_text':
                    value = prop_value.get('rich_text', [{}])[0].get('plain_text', 'N/A') if prop_value.get('rich_text') else 'N/A'
                elif prop_type == 'select':
                    value = prop_value.get('select', {}).get('name', 'N/A') if prop_value.get('select') else 'N/A'
                elif prop_type == 'multi_select':
                    values = [item.get('name') for item in prop_value.get('multi_select', [])]
                    value = ', '.join(values) if values else 'N/A'
                elif prop_type == 'email':
                    value = prop_value.get('email', 'N/A')
                elif prop_type == 'phone_number':
                    value = prop_value.get('phone_number', 'N/A')
                elif prop_type == 'url':
                    value = prop_value.get('url', 'N/A')
                else:
                    value = str(prop_value)
                
                print(f"  {prop_name}: {value}")

if __name__ == "__main__":
    main()
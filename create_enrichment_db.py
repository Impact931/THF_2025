#!/usr/bin/env python3
"""
Create a separate Enrichment Database in Notion for Apollo/LinkedIn data
This keeps the main People DB clean and allows for better data management
"""

import requests
import json
from datetime import datetime

class EnrichmentDatabaseCreator:
    def __init__(self, notion_token: str):
        self.notion_token = notion_token
        self.base_url = "https://api.notion.com/v1"
        self.headers = {
            "Authorization": f"Bearer {notion_token}",
            "Notion-Version": "2022-06-28",
            "Content-Type": "application/json"
        }
    
    def create_enrichment_database(self, parent_page_id: str = None) -> dict:
        """Create the THF Enrichment Database with all required fields"""
        
        # If no parent page specified, we'll need the workspace ID
        if not parent_page_id:
            print("❌ Need parent page ID. Please provide the THF_2025 workspace page ID")
            return None
        
        database_schema = {
            "parent": {"page_id": parent_page_id},
            "title": [{"type": "text", "text": {"content": "THF Enrichment Data"}}],
            "properties": {
                # Core identification
                "Person Name": {"title": {}},
                "Original Record ID": {"rich_text": {}},
                "Enrichment Date": {"date": {}},
                "Data Sources": {"multi_select": {
                    "options": [
                        {"name": "Apollo", "color": "blue"},
                        {"name": "LinkedIn", "color": "green"},
                        {"name": "Manual Research", "color": "yellow"},
                        {"name": "Web Scraping", "color": "orange"}
                    ]
                }},
                
                # Apollo Contact Data
                "Apollo Email": {"email": {}},
                "Apollo Personal Email": {"email": {}},
                "Apollo Phone": {"phone_number": {}},
                "Apollo Mobile": {"phone_number": {}},
                "Apollo Email Verified": {"checkbox": {}},
                "Apollo Phone Verified": {"checkbox": {}},
                
                # Apollo Professional Data  
                "Apollo Title": {"rich_text": {}},
                "Apollo Company": {"rich_text": {}},
                "Apollo Company Size": {"select": {
                    "options": [
                        {"name": "1-10", "color": "gray"},
                        {"name": "11-50", "color": "brown"},
                        {"name": "51-200", "color": "orange"},
                        {"name": "201-500", "color": "yellow"},
                        {"name": "501-1000", "color": "green"},
                        {"name": "1000+", "color": "blue"}
                    ]
                }},
                "Apollo Industry": {"rich_text": {}},
                "Apollo Department": {"rich_text": {}},
                "Apollo Seniority": {"select": {
                    "options": [
                        {"name": "Entry", "color": "gray"},
                        {"name": "Junior", "color": "brown"},
                        {"name": "Senior", "color": "orange"},
                        {"name": "Manager", "color": "yellow"},
                        {"name": "Director", "color": "green"},
                        {"name": "VP", "color": "blue"},
                        {"name": "C-Level", "color": "purple"}
                    ]
                }},
                
                # Apollo Location Data
                "Apollo City": {"rich_text": {}},
                "Apollo State": {"rich_text": {}},
                "Apollo Country": {"rich_text": {}},
                "Apollo LinkedIn URL": {"url": {}},
                
                # LinkedIn Profile Data
                "LinkedIn Headline": {"rich_text": {}},
                "LinkedIn Summary": {"rich_text": {}},
                "LinkedIn Location": {"rich_text": {}},
                "LinkedIn Industry": {"rich_text": {}},
                "LinkedIn Connections": {"number": {}},
                "LinkedIn Followers": {"number": {}},
                "LinkedIn Public Profile URL": {"url": {}},
                
                # LinkedIn Professional Data
                "LinkedIn Current Position": {"rich_text": {}},
                "LinkedIn Current Company": {"rich_text": {}},
                "LinkedIn Experience Count": {"number": {}},
                "LinkedIn Education Count": {"number": {}},
                "LinkedIn Top Skills": {"rich_text": {}},  # Top 10 skills
                "LinkedIn Certifications": {"rich_text": {}},
                "LinkedIn Languages": {"rich_text": {}},
                
                # LinkedIn Activity Metrics
                "LinkedIn Last Activity": {"date": {}},
                "LinkedIn Post Frequency": {"select": {
                    "options": [
                        {"name": "Daily", "color": "green"},
                        {"name": "Weekly", "color": "yellow"},
                        {"name": "Monthly", "color": "orange"},
                        {"name": "Rarely", "color": "red"},
                        {"name": "Unknown", "color": "gray"}
                    ]
                }},
                "LinkedIn Influencer Score": {"select": {
                    "options": [
                        {"name": "High", "color": "green"},
                        {"name": "Medium", "color": "yellow"},
                        {"name": "Low", "color": "red"},
                        {"name": "Unknown", "color": "gray"}
                    ]
                }},
                
                # Data Quality & Metadata
                "Enrichment Status": {"select": {
                    "options": [
                        {"name": "Not Started", "color": "gray"},
                        {"name": "In Progress", "color": "yellow"},
                        {"name": "Completed", "color": "green"},
                        {"name": "Failed", "color": "red"},
                        {"name": "Partial", "color": "orange"}
                    ]
                }},
                "Data Confidence": {"select": {
                    "options": [
                        {"name": "High", "color": "green"},
                        {"name": "Medium", "color": "yellow"},
                        {"name": "Low", "color": "red"}
                    ]
                }},
                "Completeness Score": {"number": {}},  # 0-100%
                "Apollo Credits Used": {"number": {}},
                "LinkedIn API Calls": {"number": {}},
                
                # Research Notes
                "Enrichment Notes": {"rich_text": {}},
                "Data Conflicts": {"rich_text": {}},  # Where Apollo/LinkedIn disagree
                "Manual Overrides": {"rich_text": {}},
                "Next Review Date": {"date": {}},
                
                # Detailed Data Storage (JSON)
                "Apollo Raw Data": {"rich_text": {}},  # JSON string
                "LinkedIn Raw Data": {"rich_text": {}},  # JSON string
                "LinkedIn Experience": {"rich_text": {}},  # JSON formatted
                "LinkedIn Education": {"rich_text": {}},  # JSON formatted
                
                # Relationship to Main DB
                "Main Record Link": {"url": {}},  # Link back to People DB record
                "Sync Status": {"select": {
                    "options": [
                        {"name": "Synced", "color": "green"},
                        {"name": "Pending", "color": "yellow"},
                        {"name": "Conflict", "color": "red"},
                        {"name": "Manual Review", "color": "orange"}
                    ]
                }},
                
                # Timestamps
                "Created Time": {"created_time": {}},
                "Last Updated": {"last_edited_time": {}},
                "Last Apollo Sync": {"date": {}},
                "Last LinkedIn Sync": {"date": {}}
            }
        }
        
        try:
            response = requests.post(f"{self.base_url}/databases", 
                                   headers=self.headers, 
                                   json=database_schema)
            response.raise_for_status()
            
            db_data = response.json()
            print(f"✅ Created THF Enrichment Database!")
            print(f"   Database ID: {db_data['id']}")
            print(f"   URL: {db_data['url']}")
            
            return db_data
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Failed to create database: {e}")
            if hasattr(e.response, 'text'):
                print(f"   Response: {e.response.text}")
            return None
    
    def create_people_db_relation(self, enrichment_db_id: str, people_db_id: str):
        """Add a relation field to People DB pointing to Enrichment DB"""
        
        # Add "Enrichment Record" relation field to People DB
        relation_property = {
            "Enrichment Record": {
                "relation": {
                    "database_id": enrichment_db_id,
                    "single_property": {}
                }
            }
        }
        
        try:
            response = requests.patch(f"{self.base_url}/databases/{people_db_id}",
                                    headers=self.headers,
                                    json={"properties": relation_property})
            response.raise_for_status()
            print("✅ Added Enrichment Record relation to People DB")
            return True
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Failed to add relation: {e}")
            return False

def main():
    print("🎖️  THF ENRICHMENT DATABASE CREATOR")
    print("=" * 50)
    
    NOTION_TOKEN = "your_notion_token_here"
    PEOPLE_DB_ID = "258c2a32-df0d-80f3-944f-cf819718d96a"
    
    # You'll need to provide the THF_2025 workspace page ID
    # This is the page that contains your People DB
    WORKSPACE_PAGE_ID = input("Enter your THF_2025 workspace page ID (where People DB lives): ").strip()
    
    if not WORKSPACE_PAGE_ID:
        print("❌ Workspace page ID is required")
        print("   Find it in the URL of your THF_2025 Notion page")
        print("   Example: https://notion.so/workspace/PAGE_ID_HERE")
        return
    
    creator = EnrichmentDatabaseCreator(NOTION_TOKEN)
    
    print("\n🔧 Creating Enrichment Database...")
    enrichment_db = creator.create_enrichment_database(WORKSPACE_PAGE_ID)
    
    if enrichment_db:
        enrichment_db_id = enrichment_db['id']
        
        print("\n🔗 Adding relation to People DB...")
        creator.create_people_db_relation(enrichment_db_id, PEOPLE_DB_ID)
        
        print(f"\n✅ Setup Complete!")
        print(f"   Enrichment DB ID: {enrichment_db_id}")
        print(f"   People DB ID: {PEOPLE_DB_ID}")
        
        print(f"\n📋 Next Steps:")
        print(f"1. View your new Enrichment Database at: {enrichment_db['url']}")
        print(f"2. Update enrichment scripts to use new database")
        print(f"3. Test enrichment with Matt Stevens")
        
        # Save IDs for future use
        with open('database_config.json', 'w') as f:
            json.dump({
                'people_db_id': PEOPLE_DB_ID,
                'enrichment_db_id': enrichment_db_id,
                'notion_token': NOTION_TOKEN,
                'created_date': datetime.now().isoformat()
            }, f, indent=2)
        
        print(f"4. Database IDs saved to database_config.json")

if __name__ == "__main__":
    main()
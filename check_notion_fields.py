#!/usr/bin/env python3
"""
Check if required enrichment fields exist in Notion People DB
"""

import requests
import json

def check_notion_fields():
    """Check current fields in Notion database"""
    
    NOTION_TOKEN = "your_notion_token_here"
    PEOPLE_DB_ID = "258c2a32-df0d-80f3-944f-cf819718d96a"
    
    headers = {
        "Authorization": f"Bearer {NOTION_TOKEN}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json"
    }
    
    url = f"https://api.notion.com/v1/databases/{PEOPLE_DB_ID}"
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        db_info = response.json()
        
        print("🔍 Current Notion Database Fields:")
        print("=" * 50)
        
        properties = db_info.get('properties', {})
        existing_fields = set(properties.keys())
        
        # Required enrichment fields
        required_apollo_fields = {
            'apollo_email', 'apollo_personal_email', 'apollo_phone', 'apollo_mobile',
            'apollo_title', 'apollo_company', 'apollo_linkedin',
            'apollo_city', 'apollo_state', 'apollo_country'
        }
        
        required_linkedin_fields = {
            'linkedin_headline', 'linkedin_summary', 'linkedin_location',
            'linkedin_experience', 'linkedin_education', 'linkedin_skills',
            'linkedin_email', 'linkedin_connections', 'linkedin_followers'
        }
        
        required_metadata_fields = {
            'enrichment_status', 'last_enriched', 'enrichment_score',
            'enrichment_notes', 'data_confidence', 'verified_email', 'verified_phone'
        }
        
        all_required_fields = required_apollo_fields | required_linkedin_fields | required_metadata_fields
        
        print("📋 Existing Fields:")
        for field_name in sorted(existing_fields):
            field_type = properties[field_name].get('type', 'unknown')
            print(f"  ✓ {field_name}: {field_type}")
        
        print(f"\n📊 Field Analysis:")
        print(f"  • Total existing fields: {len(existing_fields)}")
        print(f"  • Required enrichment fields: {len(all_required_fields)}")
        
        missing_apollo = required_apollo_fields - existing_fields
        missing_linkedin = required_linkedin_fields - existing_fields  
        missing_metadata = required_metadata_fields - existing_fields
        
        total_missing = missing_apollo | missing_linkedin | missing_metadata
        
        if total_missing:
            print(f"\n❌ Missing Required Fields ({len(total_missing)}):")
            
            if missing_apollo:
                print(f"\n  Apollo Fields ({len(missing_apollo)}):")
                for field in sorted(missing_apollo):
                    print(f"    • {field}")
            
            if missing_linkedin:
                print(f"\n  LinkedIn Fields ({len(missing_linkedin)}):")
                for field in sorted(missing_linkedin):
                    print(f"    • {field}")
            
            if missing_metadata:
                print(f"\n  Metadata Fields ({len(missing_metadata)}):")
                for field in sorted(missing_metadata):
                    print(f"    • {field}")
            
            print(f"\n⚠️  IMPORTANT: Add these fields to your Notion database before running enrichment")
            print(f"   See NOTION_DATABASE_FIELDS.md for field types and configuration")
            
            return False
        else:
            print(f"\n✅ All required fields are present!")
            return True
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Error checking Notion database: {e}")
        return False

def main():
    print("🎖️  THF NOTION DATABASE FIELD CHECKER")
    print("=" * 50)
    
    fields_ready = check_notion_fields()
    
    if fields_ready:
        print("\n🚀 Ready to proceed with enrichment!")
        print("   Run: APIFY_TOKEN='your_token' python3 apify_enrichment.py")
    else:
        print("\n📝 Next Steps:")
        print("1. Add missing fields to your Notion People DB")
        print("2. Use field types specified in NOTION_DATABASE_FIELDS.md")
        print("3. Re-run this checker to verify setup")
        print("4. Then proceed with enrichment testing")

if __name__ == "__main__":
    main()
#!/usr/bin/env python3
"""
Test script for THF Data Enrichment Service
Run this after setting up your Apify token to test the integration
"""

import sys
import os
from apify_enrichment import ApifyEnrichmentService
from thf_intelligence import THFIntelligence

def test_apify_connection(apify_token: str):
    """Test basic Apify API connection"""
    import requests
    
    print("🔧 Testing Apify API connection...")
    
    headers = {"Authorization": f"Bearer {apify_token}"}
    
    try:
        # Test with a simple API call to list actors
        response = requests.get("https://api.apify.com/v2/acts", headers=headers)
        response.raise_for_status()
        
        actors = response.json().get('data', {}).get('items', [])
        print(f"✅ Apify connection successful! Found {len(actors)} actors in your account.")
        return True
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Apify connection failed: {e}")
        return False

def test_actor_availability(apify_token: str):
    """Test if the specific actors we need are available"""
    import requests
    
    print("\n🎭 Testing actor availability...")
    
    headers = {"Authorization": f"Bearer {apify_token}"}
    
    # Test Apollo Scraper
    apollo_actor_id = "jljBwyyQakqrL1wae"
    linkedin_actor_id = "PEgClm7RgRD7YO94b"
    
    for actor_name, actor_id in [("Apollo Scraper", apollo_actor_id), ("LinkedIn Scraper", linkedin_actor_id)]:
        try:
            response = requests.get(f"https://api.apify.com/v2/acts/{actor_id}", headers=headers)
            
            if response.status_code == 200:
                actor_info = response.json().get('data', {})
                print(f"✅ {actor_name} ({actor_id}) is accessible")
                print(f"   Title: {actor_info.get('title', 'Unknown')}")
            elif response.status_code == 404:
                print(f"❌ {actor_name} ({actor_id}) not found - may need different actor ID")
            else:
                print(f"⚠️  {actor_name} ({actor_id}) returned status {response.status_code}")
                
        except requests.exceptions.RequestException as e:
            print(f"❌ Error testing {actor_name}: {e}")

def test_notion_connection():
    """Test Notion connection and data retrieval"""
    print("\n📔 Testing Notion connection...")
    
    NOTION_TOKEN = "your_notion_api_token_placeholder"
    PEOPLE_DB_ID = "258c2a32-df0d-80f3-944f-cf819718d96a"
    
    try:
        thf_intel = THFIntelligence(NOTION_TOKEN, PEOPLE_DB_ID)
        people = thf_intel.get_all_people()
        
        print(f"✅ Notion connection successful! Found {len(people)} people in database.")
        
        if people:
            first_person = thf_intel.extract_person_data(people[0])
            print(f"   Sample person: {first_person.get('name', 'Unknown')}")
            print(f"   Has LinkedIn: {'Yes' if first_person.get('linkedin') else 'No'}")
            print(f"   Has Email: {'Yes' if first_person.get('primary_email') else 'No'}")
            return first_person
        
        return None
        
    except Exception as e:
        print(f"❌ Notion connection failed: {e}")
        return None

def test_dry_run_enrichment(apify_token: str, person_data: dict):
    """Test enrichment setup without actually running the scrapers"""
    print("\n🧪 Testing enrichment setup (dry run)...")
    
    NOTION_TOKEN = "your_notion_api_token_placeholder"
    PEOPLE_DB_ID = "258c2a32-df0d-80f3-944f-cf819718d96a"
    
    try:
        enrichment_service = ApifyEnrichmentService(apify_token, NOTION_TOKEN, PEOPLE_DB_ID)
        
        print(f"✅ Enrichment service initialized for: {person_data.get('name', 'Unknown')}")
        
        # Test Apollo input preparation
        apollo_input = enrichment_service._prepare_apollo_input(person_data)
        if apollo_input:
            print("✅ Apollo input prepared successfully")
            print(f"   Search URL: {apollo_input.get('searchUrl', 'N/A')}")
        else:
            print("⚠️  Apollo input could not be prepared (missing required data)")
        
        # Test LinkedIn input preparation
        if person_data.get('linkedin'):
            print("✅ LinkedIn URL available for scraping")
            print(f"   URL: {person_data.get('linkedin')}")
        else:
            print("⚠️  No LinkedIn URL available")
        
        return True
        
    except Exception as e:
        print(f"❌ Enrichment setup failed: {e}")
        return False

def main():
    print("🎖️  THF DATA ENRICHMENT - CONNECTION TEST")
    print("=" * 55)
    
    # Get Apify token from environment or user input
    apify_token = os.environ.get('APIFY_TOKEN')
    
    print(f"Environment token found: {'Yes' if apify_token else 'No'}")
    if apify_token:
        print(f"Token preview: {apify_token[:20]}...")
    
    if not apify_token:
        print("Please provide your Apify API token.")
        print("You can either:")
        print("1. Set environment variable: export APIFY_TOKEN='your_token_here'")
        print("2. Get it from: https://console.apify.com/account/integrations")
        print()
        
        print("Skipping interactive input in non-interactive mode")
        apify_token = ""
        
        if not apify_token:
            print("⚠️  Skipping Apify tests - no token provided")
        else:
            os.environ['APIFY_TOKEN'] = apify_token
    
    # Test Notion connection first (doesn't require Apify)
    person_data = test_notion_connection()
    
    if not person_data:
        print("\n❌ Cannot proceed without Notion connection")
        return
    
    # Test Apify if token is available
    if apify_token:
        if test_apify_connection(apify_token):
            test_actor_availability(apify_token)
            test_dry_run_enrichment(apify_token, person_data)
        else:
            print("\n❌ Cannot proceed with Apify tests - connection failed")
    
    # Summary and next steps
    print("\n" + "=" * 55)
    print("📋 NEXT STEPS:")
    print("1. Add new fields to your Notion People DB (see NOTION_DATABASE_FIELDS.md)")
    print("2. Set your Apify token as environment variable")
    print("3. Run: python3 apify_enrichment.py")
    print("4. Set up Notion automation triggers")
    
    if apify_token:
        print(f"\n🔑 Your Apify token is set and ready to use")
    else:
        print(f"\n⚠️  Remember to get your Apify token from the console")

if __name__ == "__main__":
    main()
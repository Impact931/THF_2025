#!/usr/bin/env python3
"""
Basic Enrichment Test - Focus on connections and data flow only
No analytics, just ensuring Apollo/LinkedIn data flows correctly into Notion
"""

import requests
import json
import time
from typing import Dict, List, Any, Optional
from datetime import datetime
from thf_intelligence import THFIntelligence

class BasicEnrichmentTest:
    def __init__(self, apify_token: str, notion_token: str, people_db_id: str, enrichment_db_id: str):
        self.apify_token = apify_token
        self.notion_token = notion_token
        self.people_db_id = people_db_id
        self.enrichment_db_id = enrichment_db_id
        
        # API configurations
        self.apify_base_url = "https://api.apify.com/v2"
        self.apify_headers = {"Authorization": f"Bearer {apify_token}"}
        
        self.notion_base_url = "https://api.notion.com/v1"
        self.notion_headers = {
            "Authorization": f"Bearer {notion_token}",
            "Notion-Version": "2022-06-28",
            "Content-Type": "application/json"
        }
        
        # Actor IDs
        self.apollo_actor_id = "jljBwyyQakqrL1wae"
        self.linkedin_actor_id = "PEgClm7RgRD7YO94b"
        
        # THF Intelligence
        self.thf_intel = THFIntelligence(notion_token, people_db_id)
    
    def test_enrichment_flow(self, person_id: str) -> Dict[str, Any]:
        """
        Basic test of enrichment data flow from Apify to Notion
        Focus: Connections working, data flowing, no errors
        """
        print(f"🧪 Testing basic enrichment flow for: {person_id}")
        
        # Get person data
        people = self.thf_intel.get_all_people()
        person_raw = next((p for p in people if p.get('id') == person_id), None)
        
        if not person_raw:
            return {"error": "Person not found", "success": False}
        
        person_data = self.thf_intel.extract_person_data(person_raw)
        person_name = person_data.get('name', 'Unknown')
        
        print(f"   Target: {person_name}")
        
        results = {
            "person_name": person_name,
            "person_id": person_id,
            "apollo_success": False,
            "linkedin_success": False,
            "notion_storage_success": False,
            "apollo_data": None,
            "linkedin_data": None,
            "errors": []
        }
        
        # Test 1: Apollo enrichment
        print("   📧 Testing Apollo data retrieval...")
        if person_data.get('primary_email') or person_data.get('employer'):
            try:
                apollo_data = self._test_apollo_connection(person_data)
                if apollo_data:
                    results["apollo_success"] = True
                    results["apollo_data"] = apollo_data
                    print("   ✅ Apollo data retrieved successfully")
                else:
                    print("   ⚠️  Apollo returned no data")
            except Exception as e:
                error_msg = f"Apollo test failed: {str(e)}"
                results["errors"].append(error_msg)
                print(f"   ❌ {error_msg}")
        else:
            print("   ⏭️  Skipping Apollo (no email/employer)")
        
        # Test 2: LinkedIn enrichment
        print("   🔗 Testing LinkedIn data retrieval...")
        if person_data.get('linkedin'):
            try:
                linkedin_data = self._test_linkedin_connection(person_data)
                if linkedin_data:
                    results["linkedin_success"] = True
                    results["linkedin_data"] = linkedin_data
                    print("   ✅ LinkedIn data retrieved successfully")
                else:
                    print("   ⚠️  LinkedIn returned no data")
            except Exception as e:
                error_msg = f"LinkedIn test failed: {str(e)}"
                results["errors"].append(error_msg)
                print(f"   ❌ {error_msg}")
        else:
            print("   ⏭️  Skipping LinkedIn (no profile URL)")
        
        # Test 3: Notion storage
        print("   💾 Testing Notion database storage...")
        try:
            stored_successfully = self._test_notion_storage(results, person_data)
            if stored_successfully:
                results["notion_storage_success"] = True
                print("   ✅ Data stored in Notion successfully")
            else:
                print("   ❌ Notion storage failed")
        except Exception as e:
            error_msg = f"Notion storage test failed: {str(e)}"
            results["errors"].append(error_msg)
            print(f"   ❌ {error_msg}")
        
        # Summary
        success_count = sum([results["apollo_success"], results["linkedin_success"], results["notion_storage_success"]])
        results["overall_success"] = success_count >= 2  # At least 2 out of 3 should work
        
        print(f"   📊 Test Results: {success_count}/3 components working")
        
        return results
    
    def _test_apollo_connection(self, person_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Test Apollo API connection and data retrieval"""
        
        # Simple Apollo input for testing
        apollo_input = {
            "searchUrl": self._build_apollo_search_url(person_data),
            "maxResults": 5,
            "includeEmails": True,
            "includePhoneNumbers": True
        }
        
        print(f"      🔍 Apollo search URL: {apollo_input['searchUrl']}")
        
        # Run Apollo actor
        run_response = self._run_apify_actor(self.apollo_actor_id, apollo_input)
        if not run_response:
            return None
        
        print(f"      ⏳ Apollo actor started, waiting for results...")
        
        # Get results
        results = self._get_actor_results(run_response['id'], max_wait_time=120)  # 2 minute timeout
        
        if results and len(results) > 0:
            # Process first result for basic data
            first_result = results[0]
            return {
                "email": first_result.get('email'),
                "phone": first_result.get('phone_number'),
                "title": first_result.get('title'),
                "company": first_result.get('organization_name'),
                "city": first_result.get('city'),
                "state": first_result.get('state'),
                "raw_count": len(results)
            }
        
        return None
    
    def _test_linkedin_connection(self, person_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Test LinkedIn API connection and data retrieval"""
        
        linkedin_url = person_data.get('linkedin')
        if not linkedin_url:
            return None
        
        # Simple LinkedIn input for testing
        linkedin_input = {
            "profileUrls": [linkedin_url],
            "includeContacts": True,
            "includeSkills": True,
            "includeExperience": True,
            "includeEducation": True
        }
        
        print(f"      🔍 LinkedIn profile: {linkedin_url}")
        
        # Run LinkedIn actor
        run_response = self._run_apify_actor(self.linkedin_actor_id, linkedin_input)
        if not run_response:
            return None
        
        print(f"      ⏳ LinkedIn actor started, waiting for results...")
        
        # Get results
        results = self._get_actor_results(run_response['id'], max_wait_time=120)  # 2 minute timeout
        
        if results and len(results) > 0:
            # Process first result for basic data
            profile = results[0]
            return {
                "headline": profile.get('headline'),
                "location": profile.get('location'),
                "connections": profile.get('connections'),
                "experience_count": len(profile.get('experience', [])),
                "education_count": len(profile.get('education', [])),
                "skills": profile.get('skills', [])[:5],  # First 5 skills
                "raw_profile_data": bool(profile)
            }
        
        return None
    
    def _test_notion_storage(self, test_results: Dict[str, Any], person_data: Dict[str, Any]) -> bool:
        """Test storing data in Notion enrichment database"""
        
        # Prepare basic properties for storage test
        properties = {
            "Name": {"title": [{"text": {"content": test_results["person_name"]}}]},
            "Original Record ID": {"rich_text": [{"text": {"content": test_results["person_id"]}}]},
            "Enrichment Date": {"date": {"start": datetime.now().isoformat()}},
            "Enrichment Status": {"select": {"name": "Completed" if test_results.get("apollo_success") or test_results.get("linkedin_success") else "Failed"}}
        }
        
        # Add Apollo data if available
        if test_results.get("apollo_data"):
            apollo_data = test_results["apollo_data"]
            
            if apollo_data.get('email'):
                properties["Apollo Email"] = {"email": apollo_data['email']}
            
            if apollo_data.get('phone'):
                properties["Apollo Phone"] = {"phone_number": apollo_data['phone']}
            
            if apollo_data.get('title'):
                properties["Apollo Title"] = {"rich_text": [{"text": {"content": apollo_data['title']}}]}
            
            if apollo_data.get('company'):
                properties["Apollo Company"] = {"rich_text": [{"text": {"content": apollo_data['company']}}]}
            
            properties["Data Sources"] = {"multi_select": [{"name": "Apollo"}]}
        
        # Add LinkedIn data if available
        if test_results.get("linkedin_data"):
            linkedin_data = test_results["linkedin_data"]
            
            if linkedin_data.get('headline'):
                properties["LinkedIn Headline"] = {"rich_text": [{"text": {"content": linkedin_data['headline']}}]}
            
            if linkedin_data.get('location'):
                properties["LinkedIn Location"] = {"rich_text": [{"text": {"content": linkedin_data['location']}}]}
            
            if linkedin_data.get('connections'):
                properties["LinkedIn Connections"] = {"number": linkedin_data['connections']}
            
            # Update data sources
            current_sources = properties.get("Data Sources", {"multi_select": []})["multi_select"]
            current_sources.append({"name": "LinkedIn"})
            properties["Data Sources"] = {"multi_select": current_sources}
        
        # Add error notes if any
        if test_results.get("errors"):
            error_text = "; ".join(test_results["errors"])
            properties["Enrichment Notes"] = {"rich_text": [{"text": {"content": error_text[:2000]}}]}
        
        # Create the page
        page_data = {
            "parent": {"database_id": self.enrichment_db_id},
            "properties": properties
        }
        
        try:
            response = requests.post(f"{self.notion_base_url}/pages", 
                                   headers=self.notion_headers, 
                                   json=page_data)
            response.raise_for_status()
            
            page_info = response.json()
            print(f"      ✅ Test data stored in Notion: {page_info['id']}")
            return True
            
        except requests.exceptions.RequestException as e:
            print(f"      ❌ Notion storage failed: {e}")
            return False
    
    def _build_apollo_search_url(self, person_data: Dict[str, Any]) -> str:
        """Build simple Apollo search URL"""
        
        params = []
        
        if person_data.get('name'):
            name_parts = person_data['name'].split(' ', 1)
            if len(name_parts) >= 2:
                params.append(f"first_name={name_parts[0]}")
                params.append(f"last_name={name_parts[1]}")
        
        if person_data.get('employer'):
            params.append(f"organization_names[]={person_data['employer']}")
        
        if person_data.get('position'):
            params.append(f"person_titles[]={person_data['position']}")
        
        base_url = "https://app.apollo.io/#/people"
        return f"{base_url}?{'&'.join(params)}" if params else base_url
    
    def _run_apify_actor(self, actor_id: str, input_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Run Apify actor"""
        
        url = f"{self.apify_base_url}/acts/{actor_id}/runs"
        
        try:
            response = requests.post(url, headers=self.apify_headers, json=input_data)
            response.raise_for_status()
            return response.json()['data']
        except requests.exceptions.RequestException as e:
            print(f"        ❌ Failed to run actor {actor_id}: {e}")
            return None
    
    def _get_actor_results(self, run_id: str, max_wait_time: int = 120) -> Optional[List[Dict[str, Any]]]:
        """Get actor results with shorter timeout for testing"""
        
        start_time = time.time()
        check_interval = 10  # Check every 10 seconds
        
        while (time.time() - start_time) < max_wait_time:
            try:
                # Check run status
                status_url = f"{self.apify_base_url}/actor-runs/{run_id}"
                status_response = requests.get(status_url, headers=self.apify_headers)
                status_response.raise_for_status()
                
                run_data = status_response.json()['data']
                status = run_data.get('status')
                
                print(f"        ⏳ Status: {status}")
                
                if status == 'SUCCEEDED':
                    # Get results
                    dataset_id = run_data['defaultDatasetId']
                    results_url = f"{self.apify_base_url}/datasets/{dataset_id}/items"
                    
                    results_response = requests.get(results_url, headers=self.apify_headers)
                    results_response.raise_for_status()
                    
                    return results_response.json()
                
                elif status in ['FAILED', 'ABORTED', 'TIMED-OUT']:
                    print(f"        ❌ Actor run failed with status: {status}")
                    return None
                
                # Wait before checking again
                time.sleep(check_interval)
                
            except requests.exceptions.RequestException as e:
                print(f"        ❌ Error checking run status: {e}")
                return None
        
        print(f"        ⏰ Actor run timed out after {max_wait_time} seconds")
        return None

def main():
    """Run basic enrichment connection test"""
    
    print("🎖️  THF BASIC ENRICHMENT CONNECTION TEST")
    print("=" * 50)
    
    # Configuration
    try:
        with open('database_config.json', 'r') as f:
            config = json.load(f)
            
        PEOPLE_DB_ID = config['people_db_id']
        ENRICHMENT_DB_ID = config['enrichment_db_id']
        NOTION_TOKEN = config['notion_token']
        
        print(f"✅ Database config loaded")
        print(f"   People DB: {PEOPLE_DB_ID}")
        print(f"   Enrichment DB: {ENRICHMENT_DB_ID}")
        
    except FileNotFoundError:
        print("❌ Database configuration not found")
        print("   Create enrichment database first")
        return
    
    # Get Apify token
    import os
    APIFY_TOKEN = os.environ.get('APIFY_TOKEN')
    
    if not APIFY_TOKEN:
        print("❌ APIFY_TOKEN not set")
        print("   Run: export APIFY_TOKEN='your_token_here'")
        return
    
    print(f"✅ Apify token loaded")
    
    # Initialize test service
    test_service = BasicEnrichmentTest(APIFY_TOKEN, NOTION_TOKEN, PEOPLE_DB_ID, ENRICHMENT_DB_ID)
    
    # Get test person
    people = test_service.thf_intel.get_all_people()
    if not people:
        print("❌ No people found in People DB")
        return
    
    first_person = people[0]
    person_data = test_service.thf_intel.extract_person_data(first_person)
    
    print(f"\n🎯 Testing with: {person_data.get('name', 'Unknown')}")
    print(f"   Has email: {'✅' if person_data.get('primary_email') else '❌'}")
    print(f"   Has LinkedIn: {'✅' if person_data.get('linkedin') else '❌'}")
    print(f"   Has employer: {'✅' if person_data.get('employer') else '❌'}")
    
    # Run the test
    print(f"\n🧪 Starting connection tests...")
    results = test_service.test_enrichment_flow(first_person['id'])
    
    # Summary
    print(f"\n📋 TEST RESULTS SUMMARY")
    print(f"=" * 30)
    print(f"Overall Success: {'✅ PASS' if results['overall_success'] else '❌ FAIL'}")
    print(f"Apollo Connection: {'✅' if results['apollo_success'] else '❌'}")
    print(f"LinkedIn Connection: {'✅' if results['linkedin_success'] else '❌'}")
    print(f"Notion Storage: {'✅' if results['notion_storage_success'] else '❌'}")
    
    if results.get('errors'):
        print(f"\nErrors encountered:")
        for error in results['errors']:
            print(f"  • {error}")
    
    if results['overall_success']:
        print(f"\n🎉 Basic enrichment flow is working!")
        print(f"   Ready for production enrichment runs")
    else:
        print(f"\n⚠️  Fix connection issues before proceeding")
        print(f"   Check API tokens and database configuration")

if __name__ == "__main__":
    main()
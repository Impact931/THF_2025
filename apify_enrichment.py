#!/usr/bin/env python3
"""
THF Data Enrichment Service using Apify
Integrates Apollo Scraper and LinkedIn Scraper for comprehensive contact enrichment
"""

import requests
import json
import time
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from notion_client import NotionClient
from thf_intelligence import THFIntelligence

@dataclass
class EnrichmentResult:
    """Data class for enrichment results"""
    person_id: str
    apollo_data: Optional[Dict[str, Any]] = None
    linkedin_data: Optional[Dict[str, Any]] = None
    enriched_fields: Optional[Dict[str, Any]] = None
    errors: Optional[List[str]] = None

class ApifyEnrichmentService:
    def __init__(self, apify_token: str, notion_token: str, people_db_id: str):
        self.apify_token = apify_token
        self.notion_token = notion_token
        self.people_db_id = people_db_id
        
        # Apify API endpoints
        self.apify_base_url = "https://api.apify.com/v2"
        self.apify_headers = {"Authorization": f"Bearer {apify_token}"}
        
        # Actor IDs from the provided links
        self.apollo_actor_id = "jljBwyyQakqrL1wae"  # Apollo Scraper
        self.linkedin_actor_id = "PEgClm7RgRD7YO94b"  # LinkedIn Scraper
        
        # Initialize THF Intelligence for Notion operations
        self.thf_intel = THFIntelligence(notion_token, people_db_id)
        
    def enrich_person(self, person_data: Dict[str, Any]) -> EnrichmentResult:
        """
        Enrich a single person's data using Apollo and LinkedIn scrapers
        """
        person_id = person_data.get('id')
        print(f"🔍 Enriching data for: {person_data.get('name', 'Unknown')}")
        
        result = EnrichmentResult(person_id=person_id, errors=[])
        
        try:
            # Step 1: Enrich with Apollo (if we have email or company info)
            if person_data.get('primary_email') or person_data.get('employer'):
                print("  📧 Running Apollo enrichment...")
                result.apollo_data = self._run_apollo_enrichment(person_data)
            
            # Step 2: Enrich with LinkedIn (if we have LinkedIn profile)
            if person_data.get('linkedin'):
                print("  🔗 Running LinkedIn enrichment...")
                result.linkedin_data = self._run_linkedin_enrichment(person_data)
            
            # Step 3: Merge and process enriched data
            result.enriched_fields = self._merge_enrichment_data(person_data, result)
            
            print(f"  ✅ Enrichment completed for {person_data.get('name')}")
            
        except Exception as e:
            error_msg = f"Enrichment failed for {person_data.get('name')}: {str(e)}"
            print(f"  ❌ {error_msg}")
            result.errors.append(error_msg)
        
        return result
    
    def _run_apollo_enrichment(self, person_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Run Apollo scraper to enrich contact data
        """
        # Prepare Apollo scraper input
        apollo_input = self._prepare_apollo_input(person_data)
        
        if not apollo_input:
            return None
        
        # Run Apollo actor
        run_response = self._run_apify_actor(self.apollo_actor_id, apollo_input)
        
        if not run_response:
            return None
        
        # Wait for completion and get results
        return self._get_actor_results(run_response['id'])
    
    def _run_linkedin_enrichment(self, person_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Run LinkedIn scraper to enrich profile data
        """
        linkedin_url = person_data.get('linkedin')
        if not linkedin_url:
            return None
        
        # Prepare LinkedIn scraper input
        linkedin_input = {
            "profileUrls": [linkedin_url],
            "includeContacts": True,
            "includeSkills": True,
            "includeExperience": True,
            "includeEducation": True
        }
        
        # Run LinkedIn actor
        run_response = self._run_apify_actor(self.linkedin_actor_id, linkedin_input)
        
        if not run_response:
            return None
        
        # Wait for completion and get results
        return self._get_actor_results(run_response['id'])
    
    def _prepare_apollo_input(self, person_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Prepare input for Apollo scraper based on available person data
        """
        # Try to create a search query based on available information
        search_params = []
        
        if person_data.get('name'):
            # Split name into first and last
            name_parts = person_data['name'].split(' ', 1)
            first_name = name_parts[0] if name_parts else ""
            last_name = name_parts[1] if len(name_parts) > 1 else ""
            
            if first_name:
                search_params.append(f"first_name={first_name}")
            if last_name:
                search_params.append(f"last_name={last_name}")
        
        if person_data.get('employer'):
            search_params.append(f"organization_names[]={person_data['employer']}")
        
        if person_data.get('position'):
            search_params.append(f"person_titles[]={person_data['position']}")
        
        if not search_params:
            return None
        
        # Create Apollo search URL (this would need to be adapted based on actual Apollo API)
        base_url = "https://app.apollo.io/#/people"
        search_url = f"{base_url}?{'&'.join(search_params)}"
        
        return {
            "searchUrl": search_url,
            "maxResults": 10,  # Limit results to avoid excessive API usage
            "includeEmails": True,
            "includePhoneNumbers": True,
            "includeContactInfo": True
        }
    
    def _run_apify_actor(self, actor_id: str, input_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Run an Apify actor and return the run information
        """
        url = f"{self.apify_base_url}/acts/{actor_id}/runs"
        
        try:
            response = requests.post(url, 
                                   headers=self.apify_headers,
                                   json=input_data)
            response.raise_for_status()
            return response.json()['data']
        except requests.exceptions.RequestException as e:
            print(f"    ❌ Failed to run actor {actor_id}: {e}")
            return None
    
    def _get_actor_results(self, run_id: str, max_wait_time: int = 300) -> Optional[Dict[str, Any]]:
        """
        Wait for actor run to complete and retrieve results
        """
        start_time = time.time()
        
        while (time.time() - start_time) < max_wait_time:
            # Check run status
            status_url = f"{self.apify_base_url}/actor-runs/{run_id}"
            
            try:
                status_response = requests.get(status_url, headers=self.apify_headers)
                status_response.raise_for_status()
                run_data = status_response.json()['data']
                
                status = run_data.get('status')
                print(f"    ⏳ Actor run status: {status}")
                
                if status == 'SUCCEEDED':
                    # Get the results
                    results_url = f"{self.apify_base_url}/datasets/{run_data['defaultDatasetId']}/items"
                    results_response = requests.get(results_url, headers=self.apify_headers)
                    results_response.raise_for_status()
                    return results_response.json()
                
                elif status in ['FAILED', 'ABORTED', 'TIMED-OUT']:
                    print(f"    ❌ Actor run failed with status: {status}")
                    return None
                
                # Wait before checking again
                time.sleep(10)
                
            except requests.exceptions.RequestException as e:
                print(f"    ❌ Error checking run status: {e}")
                return None
        
        print(f"    ⏰ Actor run timed out after {max_wait_time} seconds")
        return None
    
    def _merge_enrichment_data(self, original_data: Dict[str, Any], 
                              enrichment_result: EnrichmentResult) -> Dict[str, Any]:
        """
        Merge enriched data from Apollo and LinkedIn into a structured format
        """
        enriched = {}
        
        # Process Apollo data
        if enrichment_result.apollo_data:
            apollo_fields = self._extract_apollo_fields(enrichment_result.apollo_data)
            enriched.update(apollo_fields)
        
        # Process LinkedIn data
        if enrichment_result.linkedin_data:
            linkedin_fields = self._extract_linkedin_fields(enrichment_result.linkedin_data)
            enriched.update(linkedin_fields)
        
        return enriched
    
    def _extract_apollo_fields(self, apollo_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Extract relevant fields from Apollo scraper results
        """
        if not apollo_data or len(apollo_data) == 0:
            return {}
        
        # Take the first result (most relevant match)
        person = apollo_data[0]
        
        extracted = {}
        
        # Contact information
        if person.get('email'):
            extracted['apollo_email'] = person['email']
        if person.get('personal_email'):
            extracted['apollo_personal_email'] = person['personal_email']
        if person.get('phone_number'):
            extracted['apollo_phone'] = person['phone_number']
        if person.get('mobile_phone_number'):
            extracted['apollo_mobile'] = person['mobile_phone_number']
        
        # Professional information
        if person.get('title'):
            extracted['apollo_title'] = person['title']
        if person.get('organization_name'):
            extracted['apollo_company'] = person['organization_name']
        if person.get('linkedin_url'):
            extracted['apollo_linkedin'] = person['linkedin_url']
        
        # Additional details
        if person.get('city'):
            extracted['apollo_city'] = person['city']
        if person.get('state'):
            extracted['apollo_state'] = person['state']
        if person.get('country'):
            extracted['apollo_country'] = person['country']
        
        return extracted
    
    def _extract_linkedin_fields(self, linkedin_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Extract relevant fields from LinkedIn scraper results
        """
        if not linkedin_data or len(linkedin_data) == 0:
            return {}
        
        # Take the first result (the requested profile)
        profile = linkedin_data[0]
        
        extracted = {}
        
        # Basic information
        if profile.get('headline'):
            extracted['linkedin_headline'] = profile['headline']
        if profile.get('summary'):
            extracted['linkedin_summary'] = profile['summary']
        if profile.get('location'):
            extracted['linkedin_location'] = profile['location']
        
        # Professional experience
        if profile.get('experience'):
            extracted['linkedin_experience'] = json.dumps(profile['experience'][:3])  # Top 3 experiences
        
        # Education
        if profile.get('education'):
            extracted['linkedin_education'] = json.dumps(profile['education'])
        
        # Skills
        if profile.get('skills'):
            extracted['linkedin_skills'] = ', '.join(profile['skills'][:10])  # Top 10 skills
        
        # Contact information
        if profile.get('email'):
            extracted['linkedin_email'] = profile['email']
        
        # Additional metrics
        if profile.get('connections'):
            extracted['linkedin_connections'] = profile['connections']
        if profile.get('followers'):
            extracted['linkedin_followers'] = profile['followers']
        
        return extracted
    
    def update_notion_record(self, person_id: str, enriched_fields: Dict[str, Any]) -> bool:
        """
        Update a Notion record with enriched data
        """
        if not enriched_fields:
            return True
        
        try:
            # Prepare properties for Notion update
            properties = {}
            
            for field_name, value in enriched_fields.items():
                if value and str(value).strip():
                    # Map field types appropriately for Notion
                    if field_name.endswith('_email'):
                        properties[field_name] = {"email": str(value)}
                    elif field_name.endswith('_phone') or field_name.endswith('_mobile'):
                        properties[field_name] = {"phone_number": str(value)}
                    elif field_name.endswith('_linkedin') or 'url' in field_name.lower():
                        properties[field_name] = {"url": str(value)}
                    else:
                        properties[field_name] = {"rich_text": [{"text": {"content": str(value)[:2000]}}]}
            
            if not properties:
                return True
            
            # Update the page in Notion
            url = f"https://api.notion.com/v1/pages/{person_id}"
            headers = {
                "Authorization": f"Bearer {self.notion_token}",
                "Notion-Version": "2022-06-28",
                "Content-Type": "application/json"
            }
            
            payload = {"properties": properties}
            
            response = requests.patch(url, headers=headers, json=payload)
            response.raise_for_status()
            
            print(f"  ✅ Updated Notion record with {len(properties)} enriched fields")
            return True
            
        except requests.exceptions.RequestException as e:
            print(f"  ❌ Failed to update Notion record: {e}")
            return False
    
    def enrich_person_by_id(self, person_id: str) -> EnrichmentResult:
        """
        Enrich a specific person by their Notion page ID
        """
        # First get the person data from Notion
        people = self.thf_intel.get_all_people()
        person_raw = next((p for p in people if p.get('id') == person_id), None)
        
        if not person_raw:
            return EnrichmentResult(person_id=person_id, errors=["Person not found"])
        
        person_data = self.thf_intel.extract_person_data(person_raw)
        
        # Run enrichment
        result = self.enrich_person(person_data)
        
        # Update Notion record if enrichment was successful
        if result.enriched_fields and not result.errors:
            self.update_notion_record(person_id, result.enriched_fields)
        
        return result

def main():
    """
    Example usage and testing
    """
    # Configuration (you'll need to provide these)
    APIFY_TOKEN = "YOUR_APIFY_TOKEN_HERE"  # Get from Apify console
    NOTION_TOKEN = "your_notion_token_here"
    PEOPLE_DB_ID = "258c2a32-df0d-80f3-944f-cf819718d96a"
    
    if APIFY_TOKEN == "YOUR_APIFY_TOKEN_HERE":
        print("❌ Please set your Apify API token in the script")
        print("Get your token from: https://console.apify.com/account/integrations")
        return
    
    # Initialize the enrichment service
    enrichment_service = ApifyEnrichmentService(APIFY_TOKEN, NOTION_TOKEN, PEOPLE_DB_ID)
    
    print("🎖️  THF DATA ENRICHMENT SERVICE")
    print("=" * 50)
    
    # Get all people from the database
    thf_intel = THFIntelligence(NOTION_TOKEN, PEOPLE_DB_ID)
    people = thf_intel.get_all_people()
    
    if not people:
        print("No people found in the database")
        return
    
    print(f"Found {len(people)} people in the database")
    
    # Enrich the first person as a test
    first_person = thf_intel.extract_person_data(people[0])
    print(f"\n🔍 Testing enrichment with: {first_person.get('name', 'Unknown')}")
    
    result = enrichment_service.enrich_person(first_person)
    
    if result.errors:
        print("❌ Enrichment errors:")
        for error in result.errors:
            print(f"  • {error}")
    else:
        print("✅ Enrichment completed successfully!")
        if result.enriched_fields:
            print("📋 Enriched fields:")
            for field, value in result.enriched_fields.items():
                print(f"  • {field}: {value}")

if __name__ == "__main__":
    main()
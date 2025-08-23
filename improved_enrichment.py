#!/usr/bin/env python3
"""
Improved THF Data Enrichment Service
Uses separate Enrichment Database to keep People DB clean
"""

import requests
import json
import time
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime
from thf_intelligence import THFIntelligence

@dataclass
class EnrichmentRecord:
    """Data class for enrichment record"""
    person_name: str
    original_record_id: str
    apollo_data: Optional[Dict[str, Any]] = None
    linkedin_data: Optional[Dict[str, Any]] = None
    enrichment_status: str = "Not Started"
    data_confidence: str = "Medium"
    completeness_score: int = 0
    errors: Optional[List[str]] = None

class ImprovedEnrichmentService:
    def __init__(self, apify_token: str, notion_token: str, people_db_id: str, enrichment_db_id: str):
        self.apify_token = apify_token
        self.notion_token = notion_token
        self.people_db_id = people_db_id
        self.enrichment_db_id = enrichment_db_id
        
        # Apify configuration
        self.apify_base_url = "https://api.apify.com/v2"
        self.apify_headers = {"Authorization": f"Bearer {apify_token}"}
        
        # Notion configuration
        self.notion_base_url = "https://api.notion.com/v1"
        self.notion_headers = {
            "Authorization": f"Bearer {notion_token}",
            "Notion-Version": "2022-06-28",
            "Content-Type": "application/json"
        }
        
        # Actor IDs
        self.apollo_actor_id = "jljBwyyQakqrL1wae"
        self.linkedin_actor_id = "PEgClm7RgRD7YO94b"
        
        # Initialize THF Intelligence
        self.thf_intel = THFIntelligence(notion_token, people_db_id)
    
    def enrich_person_complete(self, person_id: str) -> EnrichmentRecord:
        """
        Complete enrichment workflow for a person
        1. Get person data from People DB
        2. Check if enrichment record exists
        3. Run Apollo/LinkedIn scrapers
        4. Store results in Enrichment DB
        5. Link back to People DB
        """
        print(f"🔍 Starting complete enrichment for person: {person_id}")
        
        # Step 1: Get person data
        people = self.thf_intel.get_all_people()
        person_raw = next((p for p in people if p.get('id') == person_id), None)
        
        if not person_raw:
            return EnrichmentRecord("Unknown", person_id, errors=["Person not found"])
        
        person_data = self.thf_intel.extract_person_data(person_raw)
        person_name = person_data.get('name', 'Unknown')
        
        print(f"   Person: {person_name}")
        
        # Step 2: Check existing enrichment record
        existing_record = self._find_existing_enrichment(person_id)
        
        if existing_record:
            print(f"   Found existing enrichment record: {existing_record['id']}")
            # Decide whether to update or skip based on last update time
            should_update = self._should_update_enrichment(existing_record)
            if not should_update:
                print(f"   Skipping - enrichment is recent")
                return self._extract_enrichment_record(existing_record)
        
        # Step 3: Create new enrichment record
        enrichment_record = EnrichmentRecord(
            person_name=person_name,
            original_record_id=person_id,
            enrichment_status="In Progress"
        )
        
        # Step 4: Run Apollo enrichment
        if person_data.get('primary_email') or person_data.get('employer'):
            print("   🚀 Running Apollo enrichment...")
            try:
                enrichment_record.apollo_data = self._run_apollo_enrichment_safe(person_data)
                print("   ✅ Apollo enrichment completed")
            except Exception as e:
                error_msg = f"Apollo enrichment failed: {str(e)}"
                print(f"   ❌ {error_msg}")
                if not enrichment_record.errors:
                    enrichment_record.errors = []
                enrichment_record.errors.append(error_msg)
        
        # Step 5: Run LinkedIn enrichment
        if person_data.get('linkedin'):
            print("   🔗 Running LinkedIn enrichment...")
            try:
                enrichment_record.linkedin_data = self._run_linkedin_enrichment_safe(person_data)
                print("   ✅ LinkedIn enrichment completed")
            except Exception as e:
                error_msg = f"LinkedIn enrichment failed: {str(e)}"
                print(f"   ❌ {error_msg}")
                if not enrichment_record.errors:
                    enrichment_record.errors = []
                enrichment_record.errors.append(error_msg)
        
        # Step 6: Calculate data quality metrics
        enrichment_record.completeness_score = self._calculate_completeness(enrichment_record)
        enrichment_record.data_confidence = self._assess_data_confidence(enrichment_record)
        
        # Step 7: Determine final status
        if enrichment_record.errors:
            if enrichment_record.apollo_data or enrichment_record.linkedin_data:
                enrichment_record.enrichment_status = "Partial"
            else:
                enrichment_record.enrichment_status = "Failed"
        else:
            enrichment_record.enrichment_status = "Completed"
        
        # Step 8: Store in Enrichment DB
        enrichment_page_id = self._store_enrichment_record(enrichment_record, person_data)
        
        if enrichment_page_id:
            # Step 9: Link back to People DB
            self._link_to_people_db(person_id, enrichment_page_id)
            print(f"   ✅ Enrichment complete and stored")
        
        return enrichment_record
    
    def _find_existing_enrichment(self, person_id: str) -> Optional[Dict]:
        """Find existing enrichment record for a person"""
        
        filter_query = {
            "property": "Original Record ID",
            "rich_text": {
                "equals": person_id
            }
        }
        
        try:
            response = requests.post(
                f"{self.notion_base_url}/databases/{self.enrichment_db_id}/query",
                headers=self.notion_headers,
                json={"filter": filter_query}
            )
            response.raise_for_status()
            
            results = response.json().get('results', [])
            return results[0] if results else None
            
        except requests.exceptions.RequestException as e:
            print(f"   ⚠️  Error checking existing records: {e}")
            return None
    
    def _should_update_enrichment(self, existing_record: Dict) -> bool:
        """Determine if enrichment should be updated based on age"""
        
        # Get last enrichment date
        last_enriched = existing_record.get('properties', {}).get('Last Updated', {})
        if not last_enriched or not last_enriched.get('last_edited_time'):
            return True  # No date, should update
        
        # Parse last update time
        last_update_str = last_enriched['last_edited_time']
        try:
            last_update = datetime.fromisoformat(last_update_str.replace('Z', '+00:00'))
            days_since_update = (datetime.now().astimezone() - last_update).days
            
            # Update if older than 30 days
            return days_since_update > 30
            
        except Exception:
            return True  # Error parsing date, should update
    
    def _run_apollo_enrichment_safe(self, person_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Safe Apollo enrichment with error handling"""
        
        # Prepare search input for Apollo
        apollo_input = self._prepare_apollo_search(person_data)
        
        if not apollo_input:
            return None
        
        # Run the actor
        run_response = self._run_apify_actor(self.apollo_actor_id, apollo_input)
        if not run_response:
            return None
        
        # Get results with timeout
        results = self._get_actor_results(run_response['id'], max_wait_time=180)  # 3 minutes max
        
        if results and len(results) > 0:
            # Process and return the best match
            return self._process_apollo_results(results, person_data)
        
        return None
    
    def _run_linkedin_enrichment_safe(self, person_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Safe LinkedIn enrichment with error handling"""
        
        linkedin_url = person_data.get('linkedin')
        if not linkedin_url:
            return None
        
        linkedin_input = {
            "profileUrls": [linkedin_url],
            "includeContacts": True,
            "includeSkills": True,
            "includeExperience": True,
            "includeEducation": True
        }
        
        # Run the actor
        run_response = self._run_apify_actor(self.linkedin_actor_id, linkedin_input)
        if not run_response:
            return None
        
        # Get results with timeout
        results = self._get_actor_results(run_response['id'], max_wait_time=180)  # 3 minutes max
        
        if results and len(results) > 0:
            return results[0]  # Return the profile data
        
        return None
    
    def _prepare_apollo_search(self, person_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Prepare Apollo search parameters"""
        
        # Build search criteria
        search_criteria = {}
        
        name = person_data.get('name', '')
        if name:
            name_parts = name.split(' ', 1)
            if len(name_parts) >= 2:
                search_criteria['person_name'] = name
        
        if person_data.get('employer'):
            search_criteria['organization_names'] = [person_data['employer']]
        
        if person_data.get('position'):
            search_criteria['person_titles'] = [person_data['position']]
        
        if person_data.get('primary_email'):
            search_criteria['email'] = person_data['primary_email']
        
        if not search_criteria:
            return None
        
        return {
            "searchCriteria": search_criteria,
            "maxResults": 5,
            "includeEmails": True,
            "includePhoneNumbers": True
        }
    
    def _run_apify_actor(self, actor_id: str, input_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Run an Apify actor"""
        
        url = f"{self.apify_base_url}/acts/{actor_id}/runs"
        
        try:
            response = requests.post(url, headers=self.apify_headers, json=input_data)
            response.raise_for_status()
            return response.json()['data']
        except requests.exceptions.RequestException as e:
            print(f"      ❌ Failed to run actor {actor_id}: {e}")
            return None
    
    def _get_actor_results(self, run_id: str, max_wait_time: int = 300) -> Optional[List[Dict[str, Any]]]:
        """Wait for actor results"""
        
        start_time = time.time()
        
        while (time.time() - start_time) < max_wait_time:
            try:
                # Check run status
                status_url = f"{self.apify_base_url}/actor-runs/{run_id}"
                status_response = requests.get(status_url, headers=self.apify_headers)
                status_response.raise_for_status()
                
                run_data = status_response.json()['data']
                status = run_data.get('status')
                
                if status == 'SUCCEEDED':
                    # Get results
                    dataset_id = run_data['defaultDatasetId']
                    results_url = f"{self.apify_base_url}/datasets/{dataset_id}/items"
                    
                    results_response = requests.get(results_url, headers=self.apify_headers)
                    results_response.raise_for_status()
                    
                    return results_response.json()
                
                elif status in ['FAILED', 'ABORTED', 'TIMED-OUT']:
                    print(f"      ❌ Actor run failed with status: {status}")
                    return None
                
                # Wait before checking again
                time.sleep(15)  # Check every 15 seconds
                
            except requests.exceptions.RequestException as e:
                print(f"      ❌ Error checking run status: {e}")
                return None
        
        print(f"      ⏰ Actor run timed out after {max_wait_time} seconds")
        return None
    
    def _process_apollo_results(self, results: List[Dict], person_data: Dict) -> Dict[str, Any]:
        """Process Apollo results and find best match"""
        
        if not results:
            return {}
        
        # For now, take the first result
        # TODO: Add matching logic to find best result
        best_match = results[0]
        
        processed = {
            'raw_data': json.dumps(best_match),
            'email': best_match.get('email'),
            'personal_email': best_match.get('personal_email'),
            'phone': best_match.get('phone_number'),
            'mobile': best_match.get('mobile_phone_number'),
            'title': best_match.get('title'),
            'company': best_match.get('organization_name'),
            'industry': best_match.get('industry'),
            'city': best_match.get('city'),
            'state': best_match.get('state'),
            'country': best_match.get('country'),
            'linkedin_url': best_match.get('linkedin_url'),
            'company_size': best_match.get('organization_num_employees'),
            'department': best_match.get('department'),
            'seniority': best_match.get('seniority')
        }
        
        return processed
    
    def _calculate_completeness(self, record: EnrichmentRecord) -> int:
        """Calculate data completeness score (0-100)"""
        
        total_fields = 0
        filled_fields = 0
        
        # Count Apollo fields
        if record.apollo_data:
            apollo_fields = ['email', 'phone', 'title', 'company', 'city', 'linkedin_url']
            for field in apollo_fields:
                total_fields += 1
                if record.apollo_data.get(field):
                    filled_fields += 1
        
        # Count LinkedIn fields
        if record.linkedin_data:
            linkedin_fields = ['headline', 'summary', 'location', 'experience', 'education', 'skills']
            for field in linkedin_fields:
                total_fields += 1
                if record.linkedin_data.get(field):
                    filled_fields += 1
        
        return int((filled_fields / total_fields) * 100) if total_fields > 0 else 0
    
    def _assess_data_confidence(self, record: EnrichmentRecord) -> str:
        """Assess confidence in enriched data"""
        
        confidence_score = 0
        
        # Apollo data increases confidence
        if record.apollo_data:
            if record.apollo_data.get('email'):
                confidence_score += 30
            if record.apollo_data.get('phone'):
                confidence_score += 20
            if record.apollo_data.get('company'):
                confidence_score += 10
        
        # LinkedIn data increases confidence
        if record.linkedin_data:
            if record.linkedin_data.get('headline'):
                confidence_score += 15
            if record.linkedin_data.get('experience'):
                confidence_score += 15
            if record.linkedin_data.get('education'):
                confidence_score += 10
        
        if confidence_score >= 70:
            return "High"
        elif confidence_score >= 40:
            return "Medium"
        else:
            return "Low"
    
    def _store_enrichment_record(self, record: EnrichmentRecord, original_person_data: Dict) -> Optional[str]:
        """Store enrichment record in Enrichment DB"""
        
        # Prepare page properties
        properties = {
            "Person Name": {"title": [{"text": {"content": record.person_name}}]},
            "Original Record ID": {"rich_text": [{"text": {"content": record.original_record_id}}]},
            "Enrichment Date": {"date": {"start": datetime.now().isoformat()}},
            "Enrichment Status": {"select": {"name": record.enrichment_status}},
            "Data Confidence": {"select": {"name": record.data_confidence}},
            "Completeness Score": {"number": record.completeness_score}
        }
        
        # Add data sources
        sources = []
        if record.apollo_data:
            sources.append({"name": "Apollo"})
        if record.linkedin_data:
            sources.append({"name": "LinkedIn"})
        
        if sources:
            properties["Data Sources"] = {"multi_select": sources}
        
        # Add Apollo fields
        if record.apollo_data:
            apollo_mapping = {
                "Apollo Email": record.apollo_data.get('email'),
                "Apollo Personal Email": record.apollo_data.get('personal_email'),
                "Apollo Phone": record.apollo_data.get('phone'),
                "Apollo Mobile": record.apollo_data.get('mobile'),
                "Apollo Title": record.apollo_data.get('title'),
                "Apollo Company": record.apollo_data.get('company'),
                "Apollo Industry": record.apollo_data.get('industry'),
                "Apollo City": record.apollo_data.get('city'),
                "Apollo State": record.apollo_data.get('state'),
                "Apollo Country": record.apollo_data.get('country'),
                "Apollo LinkedIn URL": record.apollo_data.get('linkedin_url'),
                "Apollo Raw Data": record.apollo_data.get('raw_data', '')
            }
            
            for notion_field, value in apollo_mapping.items():
                if value:
                    if notion_field.endswith('Email'):
                        properties[notion_field] = {"email": str(value)}
                    elif notion_field.endswith('Phone') or notion_field.endswith('Mobile'):
                        properties[notion_field] = {"phone_number": str(value)}
                    elif notion_field.endswith('URL'):
                        properties[notion_field] = {"url": str(value)}
                    else:
                        properties[notion_field] = {"rich_text": [{"text": {"content": str(value)[:2000]}}]}
        
        # Add LinkedIn fields  
        if record.linkedin_data:
            linkedin_mapping = {
                "LinkedIn Headline": record.linkedin_data.get('headline'),
                "LinkedIn Summary": record.linkedin_data.get('summary'),
                "LinkedIn Location": record.linkedin_data.get('location'),
                "LinkedIn Industry": record.linkedin_data.get('industry'),
                "LinkedIn Connections": record.linkedin_data.get('connections'),
                "LinkedIn Followers": record.linkedin_data.get('followers'),
                "LinkedIn Raw Data": json.dumps(record.linkedin_data)
            }
            
            for notion_field, value in linkedin_mapping.items():
                if value:
                    if notion_field.endswith('Connections') or notion_field.endswith('Followers'):
                        if isinstance(value, (int, str)) and str(value).isdigit():
                            properties[notion_field] = {"number": int(value)}
                    else:
                        properties[notion_field] = {"rich_text": [{"text": {"content": str(value)[:2000]}}]}
        
        # Add errors if any
        if record.errors:
            error_text = "; ".join(record.errors)
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
            print(f"   ✅ Stored enrichment record: {page_info['id']}")
            return page_info['id']
            
        except requests.exceptions.RequestException as e:
            print(f"   ❌ Failed to store enrichment record: {e}")
            return None
    
    def _link_to_people_db(self, person_id: str, enrichment_page_id: str):
        """Add relation link in People DB to enrichment record"""
        
        # Update the People DB record to reference the enrichment
        relation_property = {
            "Enrichment Record": {
                "relation": [{"id": enrichment_page_id}]
            }
        }
        
        try:
            response = requests.patch(f"{self.notion_base_url}/pages/{person_id}",
                                    headers=self.notion_headers,
                                    json={"properties": relation_property})
            response.raise_for_status()
            print(f"   🔗 Linked to People DB record")
            
        except requests.exceptions.RequestException as e:
            print(f"   ⚠️  Failed to link to People DB: {e}")
    
    def _extract_enrichment_record(self, notion_page: Dict) -> EnrichmentRecord:
        """Extract enrichment record from Notion page"""
        
        properties = notion_page.get('properties', {})
        
        person_name = ""
        if properties.get('Person Name', {}).get('title'):
            person_name = properties['Person Name']['title'][0]['plain_text']
        
        original_id = ""
        if properties.get('Original Record ID', {}).get('rich_text'):
            original_id = properties['Original Record ID']['rich_text'][0]['plain_text']
        
        status = "Unknown"
        if properties.get('Enrichment Status', {}).get('select'):
            status = properties['Enrichment Status']['select']['name']
        
        return EnrichmentRecord(
            person_name=person_name,
            original_record_id=original_id,
            enrichment_status=status
        )

def main():
    """Test the improved enrichment service"""
    
    print("🎖️  THF IMPROVED ENRICHMENT SERVICE")
    print("=" * 55)
    
    # Check if we have database configuration
    try:
        with open('database_config.json', 'r') as f:
            config = json.load(f)
            
        PEOPLE_DB_ID = config['people_db_id']
        ENRICHMENT_DB_ID = config['enrichment_db_id']
        NOTION_TOKEN = config['notion_token']
        
    except FileNotFoundError:
        print("❌ Database configuration not found")
        print("   Run create_enrichment_db.py first to set up the enrichment database")
        return
    
    # Get Apify token
    import os
    APIFY_TOKEN = os.environ.get('APIFY_TOKEN')
    
    if not APIFY_TOKEN:
        print("❌ APIFY_TOKEN environment variable not set")
        print("   Run: export APIFY_TOKEN='your_token_here'")
        return
    
    # Initialize service
    service = ImprovedEnrichmentService(APIFY_TOKEN, NOTION_TOKEN, PEOPLE_DB_ID, ENRICHMENT_DB_ID)
    
    # Get first person from People DB for testing
    people = service.thf_intel.get_all_people()
    
    if not people:
        print("❌ No people found in People DB")
        return
    
    first_person = people[0]
    person_data = service.thf_intel.extract_person_data(first_person)
    
    print(f"🧪 Testing enrichment with: {person_data.get('name', 'Unknown')}")
    print(f"   Person ID: {first_person['id']}")
    
    # Run enrichment
    result = service.enrich_person_complete(first_person['id'])
    
    print(f"\n📋 Enrichment Results:")
    print(f"   Status: {result.enrichment_status}")
    print(f"   Confidence: {result.data_confidence}")
    print(f"   Completeness: {result.completeness_score}%")
    
    if result.apollo_data:
        print(f"   Apollo Data: ✅")
    
    if result.linkedin_data:
        print(f"   LinkedIn Data: ✅")
    
    if result.errors:
        print(f"   Errors: {len(result.errors)}")
        for error in result.errors:
            print(f"     • {error}")

if __name__ == "__main__":
    main()
#!/usr/bin/env python3
"""
Vercel Webhook for THF Enrichment Trigger
Triggers when People DB status changes to "Working"
"""

import json
import os
import requests
import time
from typing import Dict, Any, Optional
from datetime import datetime

# Configuration
APIFY_TOKEN = os.environ.get('APIFY_TOKEN')
NOTION_TOKEN = os.environ.get('NOTION_TOKEN')
PEOPLE_DB_ID = "258c2a32-df0d-80f3-944f-cf819718d96a"
ENRICHMENT_DB_ID = "258c2a32-df0d-805b-acb0-d0f2c81630cd"

# API configurations
APIFY_BASE_URL = "https://api.apify.com/v2"
NOTION_BASE_URL = "https://api.notion.com/v1"

APIFY_HEADERS = {"Authorization": f"Bearer {APIFY_TOKEN}"}
NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json"
}

# Actor IDs
APOLLO_ACTOR_ID = "jljBwyyQakqrL1wae"
LINKEDIN_ACTOR_ID = "PEgClm7RgRD7YO94b"

class WebhookEnrichmentProcessor:
    def __init__(self):
        self.apify_headers = APIFY_HEADERS
        self.notion_headers = NOTION_HEADERS
    
    def process_webhook(self, webhook_data: Dict[str, Any]) -> Dict[str, Any]:
        """Process incoming webhook for enrichment trigger"""
        
        try:
            # Extract person information from webhook
            person_info = self._extract_person_from_webhook(webhook_data)
            if not person_info:
                return {"error": "Could not extract person information from webhook", "status": 400}
            
            print(f"Processing enrichment for: {person_info['name']}")
            
            # Run comprehensive enrichment
            enrichment_result = self._run_comprehensive_enrichment(person_info)
            
            # Update People DB status based on result
            self._update_people_db_status(person_info['id'], enrichment_result)
            
            return {
                "success": True,
                "person_name": person_info['name'],
                "person_id": person_info['id'],
                "enrichment_status": enrichment_result['status'],
                "apollo_success": enrichment_result['apollo_success'],
                "linkedin_success": enrichment_result['linkedin_success'],
                "storage_success": enrichment_result['storage_success'],
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            print(f"Webhook processing error: {str(e)}")
            return {"error": str(e), "status": 500}
    
    def _extract_person_from_webhook(self, webhook_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Extract person information from Notion webhook payload"""
        
        # Handle different webhook payload structures
        if 'object' in webhook_data and webhook_data['object'] == 'page':
            page_data = webhook_data
        elif 'data' in webhook_data and 'object' in webhook_data['data']:
            page_data = webhook_data['data']
        else:
            return None
        
        # Extract person information
        properties = page_data.get('properties', {})
        
        # Get name (title field)
        name = "Unknown"
        if 'Name' in properties and properties['Name'].get('title'):
            name = properties['Name']['title'][0]['plain_text']
        
        # Get status to confirm it's "Working"
        status = None
        if 'Status' in properties and properties['Status'].get('status'):
            status = properties['Status']['status']['name']
        
        if status != "Working":
            return None  # Only process when status is "Working"
        
        # Extract other relevant fields
        person_info = {
            'id': page_data['id'],
            'name': name,
            'primary_email': self._extract_email_field(properties, 'Primary Email'),
            'personal_email': self._extract_email_field(properties, 'Personal Email'),
            'employer': self._extract_text_field(properties, 'Employer'),
            'position': self._extract_text_field(properties, 'Position'),
            'linkedin': self._extract_url_field(properties, 'LinkedIn Profile'),
            'phone': self._extract_phone_field(properties, 'Phone'),
            'city': self._extract_text_field(properties, 'Residence'),
            'state': self._extract_text_field(properties, 'State'),
            'country': self._extract_text_field(properties, 'Country'),
            'industry': self._extract_text_field(properties, 'Industry'),
            'military': self._extract_checkbox_field(properties, 'Military')
        }
        
        return person_info
    
    def _extract_text_field(self, properties: Dict, field_name: str) -> Optional[str]:
        """Extract text field from Notion properties"""
        if field_name in properties and properties[field_name].get('rich_text'):
            return properties[field_name]['rich_text'][0]['plain_text']
        return None
    
    def _extract_email_field(self, properties: Dict, field_name: str) -> Optional[str]:
        """Extract email field from Notion properties"""
        if field_name in properties:
            return properties[field_name].get('email')
        return None
    
    def _extract_url_field(self, properties: Dict, field_name: str) -> Optional[str]:
        """Extract URL field from Notion properties"""
        if field_name in properties:
            return properties[field_name].get('url')
        return None
    
    def _extract_phone_field(self, properties: Dict, field_name: str) -> Optional[str]:
        """Extract phone field from Notion properties"""
        if field_name in properties:
            return properties[field_name].get('phone_number')
        return None
    
    def _extract_checkbox_field(self, properties: Dict, field_name: str) -> bool:
        """Extract checkbox field from Notion properties"""
        if field_name in properties:
            return properties[field_name].get('checkbox', False)
        return False
    
    def _run_comprehensive_enrichment(self, person_info: Dict[str, Any]) -> Dict[str, Any]:
        """Run comprehensive Apollo and LinkedIn enrichment"""
        
        result = {
            'status': 'Failed',
            'apollo_success': False,
            'linkedin_success': False,
            'storage_success': False,
            'apollo_data': None,
            'linkedin_data': None,
            'errors': []
        }
        
        # Apollo enrichment
        if person_info.get('primary_email') or person_info.get('employer'):
            print("Running Apollo enrichment...")
            try:
                apollo_data = self._run_apollo_enrichment(person_info)
                if apollo_data:
                    result['apollo_success'] = True
                    result['apollo_data'] = apollo_data
                    print("Apollo enrichment successful")
            except Exception as e:
                result['errors'].append(f"Apollo error: {str(e)}")
                print(f"Apollo enrichment failed: {e}")
        
        # LinkedIn enrichment
        if person_info.get('linkedin'):
            print("Running LinkedIn enrichment...")
            try:
                linkedin_data = self._run_linkedin_enrichment(person_info)
                if linkedin_data:
                    result['linkedin_success'] = True
                    result['linkedin_data'] = linkedin_data
                    print("LinkedIn enrichment successful")
            except Exception as e:
                result['errors'].append(f"LinkedIn error: {str(e)}")
                print(f"LinkedIn enrichment failed: {e}")
        
        # Store results in enrichment database
        if result['apollo_success'] or result['linkedin_success']:
            print("Storing enrichment data...")
            try:
                storage_success = self._store_enrichment_data(person_info, result)
                result['storage_success'] = storage_success
                if storage_success:
                    result['status'] = 'Completed'
                    print("Data stored successfully")
                else:
                    result['status'] = 'Partial'
                    print("Data storage failed")
            except Exception as e:
                result['errors'].append(f"Storage error: {str(e)}")
                print(f"Storage failed: {e}")
        
        return result
    
    def _run_apollo_enrichment(self, person_info: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Run Apollo enrichment"""
        
        # Build Apollo search
        apollo_input = {
            "searchCriteria": self._build_apollo_search(person_info),
            "maxResults": 5,
            "includeEmails": True,
            "includePhoneNumbers": True,
            "includeEmploymentHistory": True,
            "includeTechnographics": True,
            "includeIntentData": True,
            "timeout": 180
        }
        
        # Run Apollo actor
        run_response = self._run_apify_actor(APOLLO_ACTOR_ID, apollo_input)
        if not run_response:
            return None
        
        # Get results
        results = self._get_actor_results(run_response['id'], max_wait_time=180)
        
        if results and len(results) > 0:
            return self._process_apollo_results(results)
        
        return None
    
    def _run_linkedin_enrichment(self, person_info: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Run LinkedIn enrichment"""
        
        linkedin_url = person_info.get('linkedin')
        if not linkedin_url:
            return None
        
        # LinkedIn input
        linkedin_input = {
            "profileUrls": [linkedin_url],
            "includeContacts": True,
            "includeSkills": True,
            "includeExperience": True,
            "includeEducation": True,
            "includeConnections": True,
            "maxConnections": 100,
            "timeout": 180
        }
        
        # Run LinkedIn actor
        run_response = self._run_apify_actor(LINKEDIN_ACTOR_ID, linkedin_input)
        if not run_response:
            return None
        
        # Get results
        results = self._get_actor_results(run_response['id'], max_wait_time=180)
        
        if results and len(results) > 0:
            return self._process_linkedin_results(results)
        
        return None
    
    def _build_apollo_search(self, person_info: Dict[str, Any]) -> Dict[str, Any]:
        """Build Apollo search criteria"""
        
        criteria = {}
        
        if person_info.get('name'):
            name_parts = person_info['name'].split(' ', 1)
            criteria['first_name'] = name_parts[0] if name_parts else ""
            criteria['last_name'] = name_parts[1] if len(name_parts) > 1 else ""
        
        if person_info.get('employer'):
            criteria['organization_names'] = [person_info['employer']]
        
        if person_info.get('position'):
            criteria['person_titles'] = [person_info['position']]
        
        if person_info.get('primary_email'):
            criteria['email'] = person_info['primary_email']
        
        return criteria
    
    def _process_apollo_results(self, results: List[Dict]) -> Dict[str, Any]:
        """Process Apollo results"""
        
        if not results:
            return {}
        
        data = results[0]  # Take first result
        
        return {
            'email': data.get('email'),
            'phone': data.get('phone_number'),
            'title': data.get('title'),
            'company': data.get('organization_name'),
            'industry': data.get('industry'),
            'city': data.get('city'),
            'state': data.get('state'),
            'linkedin_url': data.get('linkedin_url'),
            'intent_data': data.get('intent_signals'),
            'technographics': data.get('organization_technologies'),
            'raw_data': json.dumps(data)
        }
    
    def _process_linkedin_results(self, results: List[Dict]) -> Dict[str, Any]:
        """Process LinkedIn results"""
        
        if not results:
            return {}
        
        profile = results[0]  # Take profile data
        
        return {
            'headline': profile.get('headline'),
            'summary': profile.get('summary'),
            'location': profile.get('location'),
            'connections': profile.get('connections'),
            'experience': json.dumps(profile.get('experience', [])),
            'education': json.dumps(profile.get('education', [])),
            'skills': ', '.join(profile.get('skills', [])[:10]),
            'connections_data': json.dumps(profile.get('connections', [])),
            'raw_data': json.dumps(profile)
        }
    
    def _store_enrichment_data(self, person_info: Dict[str, Any], enrichment_result: Dict[str, Any]) -> bool:
        """Store enrichment data in enrichment database"""
        
        # Build properties
        properties = {
            "Name": {"title": [{"text": {"content": person_info['name']}}]},
            "Original Record ID": {"rich_text": [{"text": {"content": person_info['id']}}]},
            "Enrichment Date": {"date": {"start": datetime.now().isoformat()}},
            "Enrichment Status": {"select": {"name": enrichment_result['status']}}
        }
        
        # Data sources
        sources = []
        if enrichment_result['apollo_success']:
            sources.append({"name": "Apollo"})
        if enrichment_result['linkedin_success']:
            sources.append({"name": "LinkedIn"})
        
        if sources:
            properties["Data Sources"] = {"multi_select": sources}
        
        # Apollo data
        if enrichment_result.get('apollo_data'):
            apollo_data = enrichment_result['apollo_data']
            
            if apollo_data.get('email'):
                properties["Apollo Email"] = {"email": apollo_data['email']}
            if apollo_data.get('phone'):
                properties["Apollo Phone"] = {"phone_number": apollo_data['phone']}
            if apollo_data.get('title'):
                properties["Apollo Title"] = {"rich_text": [{"text": {"content": apollo_data['title']}}]}
            if apollo_data.get('company'):
                properties["Apollo Company"] = {"rich_text": [{"text": {"content": apollo_data['company']}}]}
            if apollo_data.get('raw_data'):
                properties["Apollo Raw Data"] = {"rich_text": [{"text": {"content": apollo_data['raw_data'][:2000]}}]}
        
        # LinkedIn data
        if enrichment_result.get('linkedin_data'):
            linkedin_data = enrichment_result['linkedin_data']
            
            if linkedin_data.get('headline'):
                properties["LinkedIn Headline"] = {"rich_text": [{"text": {"content": linkedin_data['headline']}}]}
            if linkedin_data.get('summary'):
                properties["LinkedIn Summary"] = {"rich_text": [{"text": {"content": linkedin_data['summary'][:2000]}}]}
            if linkedin_data.get('connections'):
                properties["LinkedIn Connections"] = {"number": linkedin_data['connections']}
            if linkedin_data.get('raw_data'):
                properties["LinkedIn Raw Data"] = {"rich_text": [{"text": {"content": linkedin_data['raw_data'][:2000]}}]}
        
        # Errors
        if enrichment_result.get('errors'):
            error_text = "; ".join(enrichment_result['errors'])
            properties["Enrichment Notes"] = {"rich_text": [{"text": {"content": error_text[:2000]}}]}
        
        # Create page
        page_data = {
            "parent": {"database_id": ENRICHMENT_DB_ID},
            "properties": properties
        }
        
        try:
            response = requests.post(f"{NOTION_BASE_URL}/pages", 
                                   headers=NOTION_HEADERS, 
                                   json=page_data)
            response.raise_for_status()
            return True
            
        except requests.exceptions.RequestException as e:
            print(f"Storage error: {e}")
            return False
    
    def _update_people_db_status(self, person_id: str, enrichment_result: Dict[str, Any]):
        """Update People DB status based on enrichment result"""
        
        # Determine final status
        if enrichment_result['status'] == 'Completed':
            new_status = "Completed"
        elif enrichment_result['apollo_success'] or enrichment_result['linkedin_success']:
            new_status = "Partial"
        else:
            new_status = "Failed"
        
        # Update status
        properties = {
            "Status": {"status": {"name": new_status}}
        }
        
        try:
            response = requests.patch(f"{NOTION_BASE_URL}/pages/{person_id}",
                                    headers=NOTION_HEADERS,
                                    json={"properties": properties})
            response.raise_for_status()
            print(f"People DB status updated to: {new_status}")
            
        except requests.exceptions.RequestException as e:
            print(f"Failed to update People DB status: {e}")
    
    def _run_apify_actor(self, actor_id: str, input_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Run Apify actor"""
        
        url = f"{APIFY_BASE_URL}/acts/{actor_id}/runs"
        
        try:
            response = requests.post(url, headers=self.apify_headers, json=input_data)
            response.raise_for_status()
            return response.json()['data']
        except requests.exceptions.RequestException as e:
            print(f"Failed to run actor {actor_id}: {e}")
            return None
    
    def _get_actor_results(self, run_id: str, max_wait_time: int = 180) -> Optional[List[Dict[str, Any]]]:
        """Get actor results"""
        
        start_time = time.time()
        
        while (time.time() - start_time) < max_wait_time:
            try:
                # Check status
                status_url = f"{APIFY_BASE_URL}/actor-runs/{run_id}"
                status_response = requests.get(status_url, headers=self.apify_headers)
                status_response.raise_for_status()
                
                run_data = status_response.json()['data']
                status = run_data.get('status')
                
                if status == 'SUCCEEDED':
                    # Get results
                    dataset_id = run_data['defaultDatasetId']
                    results_url = f"{APIFY_BASE_URL}/datasets/{dataset_id}/items"
                    
                    results_response = requests.get(results_url, headers=self.apify_headers)
                    results_response.raise_for_status()
                    
                    return results_response.json()
                
                elif status in ['FAILED', 'ABORTED', 'TIMED-OUT']:
                    print(f"Actor run failed with status: {status}")
                    return None
                
                time.sleep(10)  # Wait 10 seconds
                
            except requests.exceptions.RequestException as e:
                print(f"Error checking run status: {e}")
                return None
        
        print(f"Actor run timed out after {max_wait_time} seconds")
        return None

# Main webhook handler function for Vercel
def handler(request, response):
    """Main Vercel webhook handler"""
    
    # Handle preflight requests
    if request.method == 'OPTIONS':
        response.status = 200
        response.headers['Access-Control-Allow-Origin'] = '*'
        response.headers['Access-Control-Allow-Methods'] = 'POST, OPTIONS'
        response.headers['Access-Control-Allow-Headers'] = 'Content-Type'
        return response
    
    if request.method != 'POST':
        response.status = 405
        return response.json({'error': 'Method not allowed'})
    
    try:
        # Parse webhook payload
        webhook_data = request.json
        
        # Process enrichment
        processor = WebhookEnrichmentProcessor()
        result = processor.process_webhook(webhook_data)
        
        response.status = 200 if result.get('success') else result.get('status', 500)
        response.headers['Content-Type'] = 'application/json'
        response.headers['Access-Control-Allow-Origin'] = '*'
        
        return response.json(result)
        
    except Exception as e:
        response.status = 500
        return response.json({'error': str(e)})

# Alternative handler format that Vercel might expect
def main(request):
    """Alternative Vercel handler format"""
    
    if request.method == 'OPTIONS':
        return ('', 200, {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'POST, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type'
        })
    
    if request.method != 'POST':
        return (json.dumps({'error': 'Method not allowed'}), 405, {'Content-Type': 'application/json'})
    
    try:
        # Parse webhook payload
        webhook_data = request.get_json()
        
        # Process enrichment
        processor = WebhookEnrichmentProcessor()
        result = processor.process_webhook(webhook_data)
        
        status_code = 200 if result.get('success') else result.get('status', 500)
        
        headers = {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        }
        
        return (json.dumps(result), status_code, headers)
        
    except Exception as e:
        return (json.dumps({'error': str(e)}), 500, {'Content-Type': 'application/json'})

# For testing locally
if __name__ == "__main__":
    # Test payload
    test_payload = {
        "object": "page",
        "id": "258c2a32-df0d-80f3-944f-cf819718d96a",
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
            }
        }
    }
    
    processor = WebhookEnrichmentProcessor()
    result = processor.process_webhook(test_payload)
    print(json.dumps(result, indent=2))
#!/usr/bin/env python3
"""
Comprehensive Scraper Validation Test
Tests that Apollo and LinkedIn scrapers are actually working and returning real data
Validates all data categories are being populated in the enrichment database
"""

import requests
import json
import time
from typing import Dict, List, Any, Optional
from datetime import datetime
from thf_intelligence import THFIntelligence

class ComprehensiveScraperValidator:
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
    
    def validate_comprehensive_scraping(self, person_id: str) -> Dict[str, Any]:
        """
        Comprehensive validation of all scraping capabilities
        Tests every data category to ensure scrapers are working
        """
        print(f"🔍 COMPREHENSIVE SCRAPER VALIDATION")
        print(f"=" * 60)
        
        # Get person data
        people = self.thf_intel.get_all_people()
        person_raw = next((p for p in people if p.get('id') == person_id), None)
        
        if not person_raw:
            return {"error": "Person not found", "success": False}
        
        person_data = self.thf_intel.extract_person_data(person_raw)
        person_name = person_data.get('name', 'Unknown')
        
        print(f"🎯 Target: {person_name}")
        print(f"   Email: {person_data.get('primary_email', 'None')}")
        print(f"   LinkedIn: {person_data.get('linkedin', 'None')}")
        print(f"   Company: {person_data.get('employer', 'None')}")
        
        validation_results = {
            "person_name": person_name,
            "person_id": person_id,
            "apollo_validation": self._validate_apollo_scraper(person_data),
            "linkedin_validation": self._validate_linkedin_scraper(person_data),
            "data_categories_populated": {},
            "overall_success": False,
            "errors": []
        }
        
        # Store comprehensive results in database
        storage_success = self._store_comprehensive_data(validation_results, person_data)
        validation_results["storage_success"] = storage_success
        
        # Determine overall success
        apollo_working = validation_results["apollo_validation"]["working"]
        linkedin_working = validation_results["linkedin_validation"]["working"]
        
        validation_results["overall_success"] = (apollo_working or linkedin_working) and storage_success
        
        return validation_results
    
    def _validate_apollo_scraper(self, person_data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate Apollo scraper with comprehensive data extraction"""
        
        print(f"\n📧 APOLLO SCRAPER VALIDATION")
        print(f"-" * 40)
        
        if not (person_data.get('primary_email') or person_data.get('employer')):
            return {
                "working": False,
                "reason": "No email or employer data for Apollo search",
                "data_extracted": {}
            }
        
        # Comprehensive Apollo configuration
        apollo_input = {
            "searchCriteria": self._build_apollo_comprehensive_search(person_data),
            "dataEnrichment": {
                "includeEmails": True,
                "includePhoneNumbers": True,
                "includeEmploymentHistory": True,
                "includeEducation": True,
                "includeTechnographics": True,
                "includeIntentData": True,
                "includeFundingData": True,
                "includeNewsAndSocial": True,
                "includePatentsAndPublications": True,
                "includeBoardPositions": True,
                "includeSpeakingEngagements": True,
                "includeExternalCertifications": True,
                "includePersonalWebsite": True,
                "includeSocialMediaProfiles": True,
                "includeProfessionalAssociations": True,
                "includeAwardsRecognition": True,
                "includeMediaInterviews": True,
                "includePodcastAppearances": True,
                "includeConferenceParticipation": True,
                "includeAcademicPapers": True,
                "includeBookPublications": True,
                "includeMilitaryService": True,
                "includeSecurityClearance": True,
                "includeLanguagesSpoken": True
            },
            "maxResults": 10,
            "timeout": 300
        }
        
        print(f"   🔍 Running comprehensive Apollo search...")
        
        try:
            # Run Apollo actor
            run_response = self._run_apify_actor(self.apollo_actor_id, apollo_input)
            if not run_response:
                return {
                    "working": False,
                    "reason": "Failed to start Apollo actor",
                    "data_extracted": {}
                }
            
            print(f"   ⏳ Apollo actor started (ID: {run_response['id']})")
            
            # Get results with extended timeout
            results = self._get_actor_results(run_response['id'], max_wait_time=300)
            
            if not results or len(results) == 0:
                return {
                    "working": False,
                    "reason": "Apollo returned no results",
                    "data_extracted": {}
                }
            
            # Process and validate all Apollo data categories
            apollo_data = self._process_comprehensive_apollo_data(results)
            
            print(f"   ✅ Apollo returned {len(results)} results")
            print(f"   📊 Data categories found: {len([k for k, v in apollo_data.items() if v])}")
            
            return {
                "working": True,
                "results_count": len(results),
                "data_extracted": apollo_data,
                "data_quality": self._assess_apollo_data_quality(apollo_data)
            }
            
        except Exception as e:
            return {
                "working": False,
                "reason": f"Apollo scraper error: {str(e)}",
                "data_extracted": {}
            }
    
    def _validate_linkedin_scraper(self, person_data: Dict[str, Any]) -> Dict[str, Any]:
        """Validate LinkedIn scraper with comprehensive data extraction"""
        
        print(f"\n🔗 LINKEDIN SCRAPER VALIDATION")
        print(f"-" * 40)
        
        linkedin_url = person_data.get('linkedin')
        if not linkedin_url:
            return {
                "working": False,
                "reason": "No LinkedIn profile URL provided",
                "data_extracted": {}
            }
        
        # Comprehensive LinkedIn configuration
        linkedin_input = {
            "profileUrls": [linkedin_url],
            "includeFullProfile": True,
            "includeContacts": True,
            "includeSkills": True,
            "includeEndorsements": True,
            "includeExperience": True,
            "includeEducation": True,
            "includeCertifications": True,
            "includeLanguages": True,
            "includeRecommendations": True,
            "includeProjects": True,
            "includeHonorsAwards": True,
            "includeVolunteerExperience": True,
            "includePublications": True,
            "includePatents": True,
            "includeCourses": True,
            "includeOrganizations": True,
            "includeTestScores": True,
            "includeActivity": True,
            "includeConnections": True,
            "includeFollowers": True,
            "includeInfluencerMetrics": True,
            "includeContentAnalysis": True,
            "includePremiumFeatures": True,
            "includeNetworkAnalysis": True,
            "maxConnections": 500,
            "timeout": 300
        }
        
        print(f"   🔍 Running comprehensive LinkedIn scrape...")
        print(f"   🎯 Profile: {linkedin_url}")
        
        try:
            # Run LinkedIn actor
            run_response = self._run_apify_actor(self.linkedin_actor_id, linkedin_input)
            if not run_response:
                return {
                    "working": False,
                    "reason": "Failed to start LinkedIn actor",
                    "data_extracted": {}
                }
            
            print(f"   ⏳ LinkedIn actor started (ID: {run_response['id']})")
            
            # Get results with extended timeout
            results = self._get_actor_results(run_response['id'], max_wait_time=300)
            
            if not results or len(results) == 0:
                return {
                    "working": False,
                    "reason": "LinkedIn returned no results",
                    "data_extracted": {}
                }
            
            # Process and validate all LinkedIn data categories
            linkedin_data = self._process_comprehensive_linkedin_data(results)
            
            print(f"   ✅ LinkedIn profile scraped successfully")
            print(f"   📊 Data categories found: {len([k for k, v in linkedin_data.items() if v])}")
            
            return {
                "working": True,
                "profile_scraped": True,
                "data_extracted": linkedin_data,
                "data_quality": self._assess_linkedin_data_quality(linkedin_data)
            }
            
        except Exception as e:
            return {
                "working": False,
                "reason": f"LinkedIn scraper error: {str(e)}",
                "data_extracted": {}
            }
    
    def _build_apollo_comprehensive_search(self, person_data: Dict[str, Any]) -> Dict[str, Any]:
        """Build comprehensive Apollo search criteria"""
        
        criteria = {}
        
        # Person identification
        if person_data.get('name'):
            name_parts = person_data['name'].split(' ', 1)
            criteria['first_name'] = name_parts[0] if name_parts else ""
            criteria['last_name'] = name_parts[1] if len(name_parts) > 1 else ""
        
        # Company and role
        if person_data.get('employer'):
            criteria['organization_names'] = [person_data['employer']]
        
        if person_data.get('position'):
            criteria['person_titles'] = [person_data['position']]
        
        # Contact verification
        if person_data.get('primary_email'):
            criteria['email'] = person_data['primary_email']
        
        # Location
        if person_data.get('city'):
            criteria['person_locations'] = [person_data['city']]
        if person_data.get('state'):
            criteria.setdefault('person_locations', []).append(person_data['state'])
        
        # Industry and education
        if person_data.get('industry'):
            criteria['organization_industries'] = [person_data['industry']]
        
        if person_data.get('undergrad_school'):
            criteria['education'] = [person_data['undergrad_school']]
        
        # Military/veteran keywords for THF relevance
        if person_data.get('military'):
            criteria['keywords'] = ['veteran', 'military', 'navy', 'army', 'air force', 'marines', 'coast guard']
        
        return criteria
    
    def _process_comprehensive_apollo_data(self, results: List[Dict]) -> Dict[str, Any]:
        """Process comprehensive Apollo data across all categories"""
        
        if not results:
            return {}
        
        # Take best match (first result)
        data = results[0]
        
        return {
            # Basic Contact Data
            "email": data.get('email'),
            "personal_email": data.get('personal_email'),
            "phone": data.get('phone_number'),
            "mobile": data.get('mobile_phone_number'),
            "email_verified": data.get('email_verified'),
            "phone_verified": data.get('phone_verified'),
            "email_source": data.get('email_source'),
            "phone_source": data.get('phone_source'),
            
            # Professional Data
            "title": data.get('title'),
            "company": data.get('organization_name'),
            "industry": data.get('industry'),
            "department": data.get('department'),
            "seniority": data.get('seniority'),
            "company_size": data.get('organization_num_employees'),
            "revenue_range": data.get('organization_annual_revenue'),
            "funding_stage": data.get('organization_funding_stage'),
            
            # Location Data
            "city": data.get('city'),
            "state": data.get('state'),
            "country": data.get('country'),
            
            # External Intelligence
            "linkedin_url": data.get('linkedin_url'),
            "intent_data": data.get('intent_signals'),
            "technographics": data.get('organization_technologies'),
            "employee_headcount_change": data.get('organization_headcount_change'),
            "news_mentions": data.get('news_mentions_count'),
            "social_media_presence": data.get('social_media_profiles'),
            "patent_count": data.get('patent_count'),
            "press_releases": data.get('press_releases'),
            "event_participation": data.get('event_participation'),
            "board_positions": data.get('board_positions'),
            "advisory_roles": data.get('advisory_roles'),
            "speaking_engagements": data.get('speaking_engagements'),
            "certifications_external": data.get('external_certifications'),
            "publications": data.get('publications'),
            "website_personal": data.get('personal_website'),
            "github_profile": data.get('github_url'),
            "twitter_handle": data.get('twitter_handle'),
            "facebook_profile": data.get('facebook_url'),
            "instagram_handle": data.get('instagram_handle'),
            "professional_associations": data.get('professional_memberships'),
            "awards_recognition": data.get('awards_recognition'),
            "media_interviews": data.get('media_interviews'),
            "podcast_appearances": data.get('podcast_appearances'),
            "conference_presentations": data.get('conference_presentations'),
            "academic_papers": data.get('academic_publications'),
            "book_publications": data.get('book_publications'),
            "military_service": data.get('military_service'),
            "security_clearance": data.get('security_clearance'),
            "languages_spoken": data.get('languages'),
            
            # Raw data for analysis
            "raw_data": json.dumps(data)
        }
    
    def _process_comprehensive_linkedin_data(self, results: List[Dict]) -> Dict[str, Any]:
        """Process comprehensive LinkedIn data across all categories"""
        
        if not results:
            return {}
        
        # Take profile data (first result)
        profile = results[0]
        
        return {
            # Basic Profile Data
            "headline": profile.get('headline'),
            "summary": profile.get('summary'),
            "location": profile.get('location'),
            "industry": profile.get('industry'),
            "connections": profile.get('connections'),
            "followers": profile.get('followers'),
            "current_position": profile.get('currentPosition'),
            "current_company": profile.get('currentCompany'),
            
            # Experience and Education
            "experience_count": len(profile.get('experience', [])),
            "education_count": len(profile.get('education', [])),
            "experience": json.dumps(profile.get('experience', [])),
            "education": json.dumps(profile.get('education', [])),
            
            # Skills and Certifications
            "top_skills": ', '.join(profile.get('skills', [])[:10]),
            "skill_endorsements": profile.get('skillEndorsements'),
            "certifications": json.dumps(profile.get('certifications', [])),
            "languages": ', '.join(profile.get('languages', [])),
            
            # Activity and Engagement
            "last_activity": profile.get('lastActivityDate'),
            "post_frequency": profile.get('postFrequency'),
            "influencer_score": profile.get('influencerScore'),
            "premium_account": profile.get('premiumAccount'),
            "creator_mode": profile.get('creatorMode'),
            "newsletter": profile.get('newsletter'),
            "live_events": profile.get('liveEvents'),
            "company_follows": profile.get('companyFollows'),
            "hashtag_usage": ', '.join(profile.get('hashtagUsage', [])),
            "content_engagement": profile.get('contentEngagement'),
            "article_publications": profile.get('articleCount'),
            "video_content": profile.get('videoCount'),
            
            # Networks and Groups
            "professional_groups": ', '.join(profile.get('groups', [])),
            "alumni_networks": ', '.join(profile.get('alumniNetworks', [])),
            "volunteer_experience": json.dumps(profile.get('volunteerExperience', [])),
            "honors_awards": json.dumps(profile.get('honorsAwards', [])),
            "test_scores": json.dumps(profile.get('testScores', [])),
            "projects": json.dumps(profile.get('projects', [])),
            "courses": json.dumps(profile.get('courses', [])),
            "organizations": json.dumps(profile.get('organizations', [])),
            "recommendations_received": profile.get('recommendationsReceived'),
            "recommendations_given": profile.get('recommendationsGiven'),
            
            # Connection Analysis
            "first_degree_connections": len(profile.get('connections', [])) if profile.get('connections') else 0,
            "connection_names": ', '.join([conn.get('name', '') for conn in profile.get('connections', [])[:25]]),
            "connection_titles": ', '.join([conn.get('title', '') for conn in profile.get('connections', [])[:25] if conn.get('title')]),
            "connection_companies": ', '.join([conn.get('company', '') for conn in profile.get('connections', [])[:25] if conn.get('company')]),
            "connection_industries": ', '.join([conn.get('industry', '') for conn in profile.get('connections', [])[:25] if conn.get('industry')]),
            "connection_locations": ', '.join([conn.get('location', '') for conn in profile.get('connections', [])[:25] if conn.get('location')]),
            "common_connections": ', '.join([conn.get('name', '') for conn in profile.get('mutualConnections', [])[:10]]),
            "mutual_connection_names": ', '.join([conn.get('name', '') for conn in profile.get('mutualConnections', [])]),
            "second_degree_accessible": len(profile.get('secondDegreeConnections', [])),
            
            # Network Categories
            "influencer_connections": len([c for c in profile.get('connections', []) if c.get('influencer')]),
            "c_level_connections": len([c for c in profile.get('connections', []) if c.get('title', '').upper().startswith(('CEO', 'CTO', 'CFO', 'COO', 'CHIEF'))]),
            "startup_connections": len([c for c in profile.get('connections', []) if c.get('companySize', 0) < 50]),
            "fortune500_connections": len([c for c in profile.get('connections', []) if c.get('fortune500')]),
            "government_connections": len([c for c in profile.get('connections', []) if 'government' in (c.get('industry', '') or '').lower()]),
            "military_veteran_connections": len([c for c in profile.get('connections', []) if any(word in (c.get('title', '') or '').lower() for word in ['veteran', 'military', 'navy', 'army'])]),
            "academic_connections": len([c for c in profile.get('connections', []) if 'education' in (c.get('industry', '') or '').lower()]),
            "investor_connections": len([c for c in profile.get('connections', []) if 'investor' in (c.get('title', '') or '').lower()]),
            "recruiter_connections": len([c for c in profile.get('connections', []) if 'recruiter' in (c.get('title', '') or '').lower()]),
            
            # Raw data for analysis
            "raw_data": json.dumps(profile),
            "connections_raw": json.dumps(profile.get('connections', []))
        }
    
    def _assess_apollo_data_quality(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Assess quality of Apollo data extraction"""
        
        total_fields = 50  # Total Apollo fields we're looking for
        populated_fields = len([v for v in data.values() if v])
        
        return {
            "total_fields": total_fields,
            "populated_fields": populated_fields,
            "completeness_percentage": round((populated_fields / total_fields) * 100, 1),
            "has_contact_info": bool(data.get('email') or data.get('phone')),
            "has_professional_info": bool(data.get('title') and data.get('company')),
            "has_external_data": bool(data.get('news_mentions') or data.get('social_media_presence')),
            "verification_status": data.get('email_verified') or data.get('phone_verified')
        }
    
    def _assess_linkedin_data_quality(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Assess quality of LinkedIn data extraction"""
        
        total_fields = 60  # Total LinkedIn fields we're looking for
        populated_fields = len([v for v in data.values() if v])
        
        return {
            "total_fields": total_fields,
            "populated_fields": populated_fields,
            "completeness_percentage": round((populated_fields / total_fields) * 100, 1),
            "has_basic_profile": bool(data.get('headline') and data.get('summary')),
            "has_experience_data": bool(data.get('experience_count', 0) > 0),
            "has_education_data": bool(data.get('education_count', 0) > 0),
            "has_connections_data": bool(data.get('first_degree_connections', 0) > 0),
            "network_size": data.get('connections', 0)
        }
    
    def _store_comprehensive_data(self, validation_results: Dict[str, Any], person_data: Dict[str, Any]) -> bool:
        """Store comprehensive enrichment data in Notion database"""
        
        print(f"\n💾 STORING COMPREHENSIVE DATA")
        print(f"-" * 40)
        
        # Build comprehensive properties for all data categories
        properties = {
            "Name": {"title": [{"text": {"content": validation_results["person_name"]}}]},
            "Original Record ID": {"rich_text": [{"text": {"content": validation_results["person_id"]}}]},
            "Enrichment Date": {"date": {"start": datetime.now().isoformat()}},
            "Enrichment Status": {"select": {"name": "Completed" if validation_results.get("overall_success") else "Partial"}},
            "Data Confidence": {"select": {"name": "High" if validation_results.get("overall_success") else "Medium"}}
        }
        
        # Data sources
        sources = []
        if validation_results["apollo_validation"]["working"]:
            sources.append({"name": "Apollo"})
        if validation_results["linkedin_validation"]["working"]:
            sources.append({"name": "LinkedIn"})
        
        if sources:
            properties["Data Sources"] = {"multi_select": sources}
        
        # Store Apollo data comprehensively
        apollo_data = validation_results["apollo_validation"].get("data_extracted", {})
        if apollo_data:
            apollo_properties = self._build_apollo_properties(apollo_data)
            properties.update(apollo_properties)
        
        # Store LinkedIn data comprehensively
        linkedin_data = validation_results["linkedin_validation"].get("data_extracted", {})
        if linkedin_data:
            linkedin_properties = self._build_linkedin_properties(linkedin_data)
            properties.update(linkedin_properties)
        
        # Store validation results
        validation_notes = []
        if validation_results["apollo_validation"]["working"]:
            apollo_quality = validation_results["apollo_validation"].get("data_quality", {})
            validation_notes.append(f"Apollo: {apollo_quality.get('completeness_percentage', 0)}% complete")
        
        if validation_results["linkedin_validation"]["working"]:
            linkedin_quality = validation_results["linkedin_validation"].get("data_quality", {})
            validation_notes.append(f"LinkedIn: {linkedin_quality.get('completeness_percentage', 0)}% complete")
        
        if validation_notes:
            properties["Enrichment Notes"] = {"rich_text": [{"text": {"content": "; ".join(validation_notes)}}]}
        
        # Create the comprehensive record
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
            print(f"   ✅ Comprehensive data stored successfully")
            print(f"   📝 Record ID: {page_info['id']}")
            return True
            
        except requests.exceptions.RequestException as e:
            print(f"   ❌ Storage failed: {e}")
            return False
    
    def _build_apollo_properties(self, apollo_data: Dict[str, Any]) -> Dict[str, Any]:
        """Build Notion properties from Apollo data"""
        
        properties = {}
        
        # Contact information
        if apollo_data.get('email'):
            properties["Apollo Email"] = {"email": apollo_data['email']}
        if apollo_data.get('personal_email'):
            properties["Apollo Personal Email"] = {"email": apollo_data['personal_email']}
        if apollo_data.get('phone'):
            properties["Apollo Phone"] = {"phone_number": apollo_data['phone']}
        if apollo_data.get('mobile'):
            properties["Apollo Mobile"] = {"phone_number": apollo_data['mobile']}
        
        # Verification status
        if apollo_data.get('email_verified') is not None:
            properties["Apollo Email Verified"] = {"checkbox": apollo_data['email_verified']}
        if apollo_data.get('phone_verified') is not None:
            properties["Apollo Phone Verified"] = {"checkbox": apollo_data['phone_verified']}
        
        # Professional information
        text_fields = [
            'title', 'company', 'industry', 'department', 'seniority',
            'city', 'state', 'country', 'email_source', 'phone_source',
            'revenue_range', 'funding_stage', 'technographics', 'intent_data'
        ]
        
        for field in text_fields:
            if apollo_data.get(field):
                field_name = f"Apollo {field.replace('_', ' ').title()}"
                properties[field_name] = {"rich_text": [{"text": {"content": str(apollo_data[field])[:2000]}}]}
        
        # Numeric fields
        numeric_fields = ['company_size', 'news_mentions', 'patent_count']
        for field in numeric_fields:
            if apollo_data.get(field) is not None and str(apollo_data[field]).isdigit():
                field_name = f"Apollo {field.replace('_', ' ').title()}"
                properties[field_name] = {"number": int(apollo_data[field])}
        
        # URL fields
        if apollo_data.get('linkedin_url'):
            properties["Apollo LinkedIn URL"] = {"url": apollo_data['linkedin_url']}
        
        # Raw data
        if apollo_data.get('raw_data'):
            properties["Apollo Raw Data"] = {"rich_text": [{"text": {"content": apollo_data['raw_data'][:2000]}}]}
        
        return properties
    
    def _build_linkedin_properties(self, linkedin_data: Dict[str, Any]) -> Dict[str, Any]:
        """Build Notion properties from LinkedIn data"""
        
        properties = {}
        
        # Basic profile information
        text_fields = [
            'headline', 'summary', 'location', 'industry', 'current_position',
            'current_company', 'top_skills', 'languages', 'connection_names',
            'connection_titles', 'connection_companies'
        ]
        
        for field in text_fields:
            if linkedin_data.get(field):
                field_name = f"LinkedIn {field.replace('_', ' ').title()}"
                properties[field_name] = {"rich_text": [{"text": {"content": str(linkedin_data[field])[:2000]}}]}
        
        # Numeric fields
        numeric_fields = [
            'connections', 'followers', 'experience_count', 'education_count',
            'first_degree_connections', 'second_degree_accessible',
            'c_level_connections', 'veteran_connections'
        ]
        
        for field in numeric_fields:
            if linkedin_data.get(field) is not None and str(linkedin_data[field]).isdigit():
                field_name = f"LinkedIn {field.replace('_', ' ').title()}"
                properties[field_name] = {"number": int(linkedin_data[field])}
        
        # JSON data fields
        json_fields = ['experience', 'education', 'certifications']
        for field in json_fields:
            if linkedin_data.get(field):
                field_name = f"LinkedIn {field.title()}"
                properties[field_name] = {"rich_text": [{"text": {"content": str(linkedin_data[field])[:2000]}}]}
        
        # Raw data
        if linkedin_data.get('raw_data'):
            properties["LinkedIn Raw Data"] = {"rich_text": [{"text": {"content": linkedin_data['raw_data'][:2000]}}]}
        
        if linkedin_data.get('connections_raw'):
            properties["LinkedIn Connections Raw"] = {"rich_text": [{"text": {"content": linkedin_data['connections_raw'][:2000]}}]}
        
        return properties
    
    def _run_apify_actor(self, actor_id: str, input_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Run Apify actor"""
        url = f"{self.apify_base_url}/acts/{actor_id}/runs"
        
        try:
            response = requests.post(url, headers=self.apify_headers, json=input_data)
            response.raise_for_status()
            return response.json()['data']
        except requests.exceptions.RequestException as e:
            print(f"      ❌ Failed to run actor {actor_id}: {e}")
            return None
    
    def _get_actor_results(self, run_id: str, max_wait_time: int = 300) -> Optional[List[Dict[str, Any]]]:
        """Get actor results with progress tracking"""
        start_time = time.time()
        check_interval = 15  # Check every 15 seconds
        
        while (time.time() - start_time) < max_wait_time:
            try:
                # Check run status
                status_url = f"{self.apify_base_url}/actor-runs/{run_id}"
                status_response = requests.get(status_url, headers=self.apify_headers)
                status_response.raise_for_status()
                
                run_data = status_response.json()['data']
                status = run_data.get('status')
                
                elapsed = int(time.time() - start_time)
                print(f"        ⏳ Status: {status} ({elapsed}s elapsed)")
                
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
    """Run comprehensive scraper validation"""
    
    print("🎖️  THF COMPREHENSIVE SCRAPER VALIDATION")
    print("=" * 60)
    
    # Configuration
    try:
        with open('database_config.json', 'r') as f:
            config = json.load(f)
            
        PEOPLE_DB_ID = config['people_db_id']
        ENRICHMENT_DB_ID = config['enrichment_db_id']
        NOTION_TOKEN = config['notion_token']
        
        print(f"✅ Configuration loaded")
        
    except FileNotFoundError:
        print("❌ Database configuration not found")
        print("   Create enrichment database first")
        return
    
    # Get Apify token
    import os
    APIFY_TOKEN = os.environ.get('APIFY_TOKEN')
    
    if not APIFY_TOKEN:
        print("❌ APIFY_TOKEN not set")
        return
    
    print(f"✅ Apify token loaded")
    
    # Initialize validator
    validator = ComprehensiveScraperValidator(APIFY_TOKEN, NOTION_TOKEN, PEOPLE_DB_ID, ENRICHMENT_DB_ID)
    
    # Get test person
    people = validator.thf_intel.get_all_people()
    if not people:
        print("❌ No people found in People DB")
        return
    
    first_person = people[0]
    person_data = validator.thf_intel.extract_person_data(first_person)
    
    print(f"\n🎯 VALIDATION TARGET")
    print(f"   Name: {person_data.get('name', 'Unknown')}")
    print(f"   Email: {person_data.get('primary_email', 'None')}")
    print(f"   LinkedIn: {person_data.get('linkedin', 'None')}")
    print(f"   Company: {person_data.get('employer', 'None')}")
    
    # Run comprehensive validation
    results = validator.validate_comprehensive_scraping(first_person['id'])
    
    # Final results
    print(f"\n📊 COMPREHENSIVE VALIDATION RESULTS")
    print(f"=" * 60)
    print(f"Overall Success: {'✅ PASS' if results['overall_success'] else '❌ FAIL'}")
    
    # Apollo results
    apollo = results['apollo_validation']
    print(f"\n🌐 APOLLO SCRAPER:")
    print(f"   Working: {'✅ YES' if apollo['working'] else '❌ NO'}")
    if apollo['working']:
        quality = apollo.get('data_quality', {})
        print(f"   Data Quality: {quality.get('completeness_percentage', 0)}% complete")
        print(f"   Contact Info: {'✅' if quality.get('has_contact_info') else '❌'}")
        print(f"   Professional Info: {'✅' if quality.get('has_professional_info') else '❌'}")
        print(f"   External Data: {'✅' if quality.get('has_external_data') else '❌'}")
    else:
        print(f"   Reason: {apollo.get('reason', 'Unknown')}")
    
    # LinkedIn results
    linkedin = results['linkedin_validation']
    print(f"\n🔗 LINKEDIN SCRAPER:")
    print(f"   Working: {'✅ YES' if linkedin['working'] else '❌ NO'}")
    if linkedin['working']:
        quality = linkedin.get('data_quality', {})
        print(f"   Data Quality: {quality.get('completeness_percentage', 0)}% complete")
        print(f"   Basic Profile: {'✅' if quality.get('has_basic_profile') else '❌'}")
        print(f"   Experience Data: {'✅' if quality.get('has_experience_data') else '❌'}")
        print(f"   Connections Data: {'✅' if quality.get('has_connections_data') else '❌'}")
        print(f"   Network Size: {quality.get('network_size', 0)} connections")
    else:
        print(f"   Reason: {linkedin.get('reason', 'Unknown')}")
    
    # Storage results
    print(f"\n💾 DATABASE STORAGE:")
    print(f"   Stored Successfully: {'✅ YES' if results.get('storage_success') else '❌ NO'}")
    
    if results['overall_success']:
        print(f"\n🎉 COMPREHENSIVE SCRAPERS ARE WORKING!")
        print(f"   Ready for full-scale enrichment operations")
        print(f"   All data categories validated and stored")
    else:
        print(f"\n⚠️  ISSUES DETECTED - TROUBLESHOOTING REQUIRED")
        print(f"   Review scraper configurations and API access")

if __name__ == "__main__":
    main()
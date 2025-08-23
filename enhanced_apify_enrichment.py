#!/usr/bin/env python3
"""
Enhanced THF Data Enrichment Service
Comprehensive Apollo external data + LinkedIn 1st-degree connections analysis
"""

import requests
import json
import time
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime
from thf_intelligence import THFIntelligence

@dataclass
class NetworkConnection:
    """Data class for LinkedIn 1st-degree connections"""
    name: str
    title: Optional[str] = None
    company: Optional[str] = None
    industry: Optional[str] = None
    location: Optional[str] = None
    mutual_connections: int = 0
    profile_url: Optional[str] = None

@dataclass
class EnhancedEnrichmentRecord:
    """Enhanced data class for comprehensive enrichment"""
    person_name: str
    original_record_id: str
    apollo_data: Optional[Dict[str, Any]] = None
    linkedin_data: Optional[Dict[str, Any]] = None
    linkedin_connections: Optional[List[NetworkConnection]] = None
    network_analysis: Optional[Dict[str, Any]] = None
    external_data_sources: Optional[List[str]] = None
    enrichment_status: str = "Not Started"
    data_confidence: str = "Medium"
    completeness_score: int = 0
    errors: Optional[List[str]] = None

class EnhancedApifyEnrichmentService:
    def __init__(self, apify_token: str, notion_token: str, people_db_id: str, enrichment_db_id: str):
        self.apify_token = apify_token
        self.notion_token = notion_token
        self.people_db_id = people_db_id
        self.enrichment_db_id = enrichment_db_id
        
        # Apify configuration
        self.apify_base_url = "https://api.apify.com/v2"
        self.apify_headers = {"Authorization": f"Bearer {apify_token}"}
        
        # Enhanced Actor IDs for comprehensive scraping
        self.apollo_actor_id = "jljBwyyQakqrL1wae"  # Apollo Scraper
        self.linkedin_profile_actor_id = "PEgClm7RgRD7YO94b"  # LinkedIn Profile Scraper
        self.linkedin_connections_actor_id = "curious_coder/linkedin-profile-scraper"  # Enhanced connections
        
        # Notion configuration
        self.notion_base_url = "https://api.notion.com/v1"
        self.notion_headers = {
            "Authorization": f"Bearer {notion_token}",
            "Notion-Version": "2022-06-28",
            "Content-Type": "application/json"
        }
        
        # Initialize THF Intelligence
        self.thf_intel = THFIntelligence(notion_token, people_db_id)
    
    def comprehensive_enrich_person(self, person_id: str) -> EnhancedEnrichmentRecord:
        """
        Comprehensive enrichment with external Apollo data and LinkedIn network analysis
        """
        print(f"🔍 Starting comprehensive enrichment for: {person_id}")
        
        # Get person data
        people = self.thf_intel.get_all_people()
        person_raw = next((p for p in people if p.get('id') == person_id), None)
        
        if not person_raw:
            return EnhancedEnrichmentRecord("Unknown", person_id, errors=["Person not found"])
        
        person_data = self.thf_intel.extract_person_data(person_raw)
        person_name = person_data.get('name', 'Unknown')
        
        print(f"   Target: {person_name}")
        
        # Initialize enrichment record
        enrichment_record = EnhancedEnrichmentRecord(
            person_name=person_name,
            original_record_id=person_id,
            enrichment_status="In Progress",
            external_data_sources=[]
        )
        
        # Phase 1: Enhanced Apollo enrichment with external data
        if person_data.get('primary_email') or person_data.get('employer'):
            print("   🌐 Running comprehensive Apollo enrichment...")
            try:
                enrichment_record.apollo_data = self._run_enhanced_apollo_enrichment(person_data)
                enrichment_record.external_data_sources.append("Apollo")
                print("   ✅ Apollo external data enrichment completed")
            except Exception as e:
                self._add_error(enrichment_record, f"Apollo enrichment failed: {str(e)}")
        
        # Phase 2: LinkedIn profile enrichment
        if person_data.get('linkedin'):
            print("   🔗 Running LinkedIn profile enrichment...")
            try:
                enrichment_record.linkedin_data = self._run_enhanced_linkedin_enrichment(person_data)
                enrichment_record.external_data_sources.append("LinkedIn Profile")
                print("   ✅ LinkedIn profile enrichment completed")
            except Exception as e:
                self._add_error(enrichment_record, f"LinkedIn profile enrichment failed: {str(e)}")
        
        # Phase 3: LinkedIn 1st-degree connections analysis
        if person_data.get('linkedin'):
            print("   🤝 Analyzing LinkedIn 1st-degree connections...")
            try:
                enrichment_record.linkedin_connections = self._analyze_linkedin_connections(person_data)
                enrichment_record.network_analysis = self._perform_network_analysis(enrichment_record.linkedin_connections)
                enrichment_record.external_data_sources.append("LinkedIn Network")
                print(f"   ✅ Network analysis completed - {len(enrichment_record.linkedin_connections or [])} connections analyzed")
            except Exception as e:
                self._add_error(enrichment_record, f"LinkedIn network analysis failed: {str(e)}")
        
        # Phase 4: Calculate enhanced metrics
        enrichment_record.completeness_score = self._calculate_enhanced_completeness(enrichment_record)
        enrichment_record.data_confidence = self._assess_enhanced_confidence(enrichment_record)
        
        # Phase 5: Determine final status
        self._determine_enrichment_status(enrichment_record)
        
        # Phase 6: Store comprehensive results
        enrichment_page_id = self._store_enhanced_enrichment_record(enrichment_record, person_data)
        
        if enrichment_page_id:
            self._link_to_people_db(person_id, enrichment_page_id)
            print(f"   ✅ Comprehensive enrichment completed and stored")
        
        return enrichment_record
    
    def _run_enhanced_apollo_enrichment(self, person_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enhanced Apollo enrichment specifically configured for external data sources
        """
        # Configure Apollo for comprehensive external data collection
        apollo_input = {
            "searchCriteria": self._build_comprehensive_apollo_search(person_data),
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
                "includeExternalCertifications": True
            },
            "externalDataSources": {
                "enableCrunchbase": True,
                "enableZoomInfo": True,
                "enableBusinessRegistries": True,
                "enableSocialMediaAggregation": True,
                "enableNewsAPI": True,
                "enablePatentDatabases": True,
                "enableSecFilings": True,
                "enablePressReleases": True
            },
            "verificationLevel": "strict",
            "maxResults": 10
        }
        
        # Run Apollo with enhanced configuration
        run_response = self._run_apify_actor(self.apollo_actor_id, apollo_input)
        if not run_response:
            return {}
        
        results = self._get_actor_results(run_response['id'], max_wait_time=300)
        
        if results and len(results) > 0:
            return self._process_enhanced_apollo_results(results, person_data)
        
        return {}
    
    def _build_comprehensive_apollo_search(self, person_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Build comprehensive search criteria for Apollo external data collection
        """
        criteria = {}
        
        # Person identification
        if person_data.get('name'):
            name_parts = person_data['name'].split(' ', 1)
            criteria['first_name'] = name_parts[0] if name_parts else ""
            criteria['last_name'] = name_parts[1] if len(name_parts) > 1 else ""
        
        # Company and role information
        if person_data.get('employer'):
            criteria['organization_names'] = [person_data['employer']]
        
        if person_data.get('position'):
            criteria['person_titles'] = [person_data['position']]
        
        # Contact information for verification
        if person_data.get('primary_email'):
            criteria['email'] = person_data['primary_email']
        
        # Location data
        if person_data.get('city'):
            criteria['person_locations'] = [person_data['city']]
        
        if person_data.get('state'):
            criteria['person_locations'].append(person_data['state'])
        
        # Industry focus
        if person_data.get('industry'):
            criteria['organization_industries'] = [person_data['industry']]
        
        # Education for additional verification
        if person_data.get('undergrad_school'):
            criteria['education'] = [person_data['undergrad_school']]
        
        # Military background (important for THF)
        if person_data.get('military'):
            criteria['keywords'] = ['veteran', 'military', 'navy', 'army', 'air force', 'marines']
        
        return criteria
    
    def _run_enhanced_linkedin_enrichment(self, person_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enhanced LinkedIn profile enrichment with comprehensive data collection
        """
        linkedin_url = person_data.get('linkedin')
        if not linkedin_url:
            return {}
        
        # Enhanced LinkedIn scraping configuration
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
            "includeInfluencerMetrics": True,
            "maxWaitTime": 300
        }
        
        run_response = self._run_apify_actor(self.linkedin_profile_actor_id, linkedin_input)
        if not run_response:
            return {}
        
        results = self._get_actor_results(run_response['id'], max_wait_time=300)
        
        if results and len(results) > 0:
            return self._process_enhanced_linkedin_results(results[0])
        
        return {}
    
    def _analyze_linkedin_connections(self, person_data: Dict[str, Any]) -> List[NetworkConnection]:
        """
        Analyze LinkedIn 1st-degree connections for network intelligence
        """
        linkedin_url = person_data.get('linkedin')
        if not linkedin_url:
            return []
        
        # Configuration for LinkedIn connections scraping
        connections_input = {
            "profileUrl": linkedin_url,
            "scrapeConnections": True,
            "maxConnections": 500,  # Limit to prevent excessive API usage
            "includeConnectionDetails": True,
            "connectionFields": [
                "name", "title", "company", "location", "industry",
                "profileUrl", "mutualConnections", "connectionDate"
            ],
            "filterCriteria": {
                "minMutualConnections": 1,
                "excludeIncompleteProfiles": True,
                "prioritizeRelevantIndustries": ["Technology", "Non-Profit", "Veteran Services", "Finance"]
            },
            "networkAnalysis": {
                "identifyInfluencers": True,
                "identifyDecisionMakers": True,
                "identifyAlumni": True,
                "identifyIndustryLeaders": True
            }
        }
        
        run_response = self._run_apify_actor(self.linkedin_connections_actor_id, connections_input)
        if not run_response:
            return []
        
        results = self._get_actor_results(run_response['id'], max_wait_time=300)
        
        if results and len(results) > 0:
            return self._process_connections_results(results)
        
        return []
    
    def _perform_network_analysis(self, connections: List[NetworkConnection]) -> Dict[str, Any]:
        """
        Perform comprehensive network analysis on 1st-degree connections
        """
        if not connections:
            return {}
        
        analysis = {
            "total_connections": len(connections),
            "industry_distribution": self._analyze_industry_distribution(connections),
            "company_distribution": self._analyze_company_distribution(connections),
            "location_distribution": self._analyze_location_distribution(connections),
            "seniority_analysis": self._analyze_seniority_levels(connections),
            "mutual_connections_stats": self._analyze_mutual_connections(connections),
            "network_strength_score": self._calculate_network_strength(connections),
            "influence_indicators": self._identify_network_influencers(connections),
            "decision_makers": self._identify_decision_makers(connections),
            "industry_leaders": self._identify_industry_leaders(connections),
            "alumni_network": self._identify_alumni_connections(connections),
            "thf_relevant_connections": self._identify_thf_relevant_connections(connections)
        }
        
        return analysis
    
    def _analyze_industry_distribution(self, connections: List[NetworkConnection]) -> Dict[str, int]:
        """Analyze industry distribution of connections"""
        from collections import Counter
        industries = [conn.industry for conn in connections if conn.industry]
        return dict(Counter(industries).most_common(10))
    
    def _analyze_company_distribution(self, connections: List[NetworkConnection]) -> Dict[str, int]:
        """Analyze company distribution of connections"""
        from collections import Counter
        companies = [conn.company for conn in connections if conn.company]
        return dict(Counter(companies).most_common(15))
    
    def _analyze_location_distribution(self, connections: List[NetworkConnection]) -> Dict[str, int]:
        """Analyze geographic distribution of connections"""
        from collections import Counter
        locations = [conn.location for conn in connections if conn.location]
        return dict(Counter(locations).most_common(10))
    
    def _analyze_seniority_levels(self, connections: List[NetworkConnection]) -> Dict[str, int]:
        """Analyze seniority levels based on titles"""
        seniority_keywords = {
            "C-Level": ["CEO", "CTO", "CFO", "COO", "Chief", "President"],
            "VP": ["VP", "Vice President", "SVP", "EVP"],
            "Director": ["Director", "Dir"],
            "Manager": ["Manager", "Mgr"],
            "Senior": ["Senior", "Sr", "Lead"],
            "Entry": ["Associate", "Analyst", "Coordinator", "Assistant"]
        }
        
        seniority_counts = {level: 0 for level in seniority_keywords.keys()}
        
        for conn in connections:
            if conn.title:
                title_upper = conn.title.upper()
                for level, keywords in seniority_keywords.items():
                    if any(keyword.upper() in title_upper for keyword in keywords):
                        seniority_counts[level] += 1
                        break
        
        return seniority_counts
    
    def _analyze_mutual_connections(self, connections: List[NetworkConnection]) -> Dict[str, Any]:
        """Analyze mutual connections statistics"""
        mutual_counts = [conn.mutual_connections for conn in connections if conn.mutual_connections > 0]
        
        if not mutual_counts:
            return {}
        
        return {
            "average_mutual_connections": sum(mutual_counts) / len(mutual_counts),
            "max_mutual_connections": max(mutual_counts),
            "high_mutual_count": len([c for c in mutual_counts if c >= 10]),
            "medium_mutual_count": len([c for c in mutual_counts if 5 <= c < 10]),
            "low_mutual_count": len([c for c in mutual_counts if 1 <= c < 5])
        }
    
    def _calculate_network_strength(self, connections: List[NetworkConnection]) -> int:
        """Calculate overall network strength score (0-100)"""
        if not connections:
            return 0
        
        score = 0
        
        # Base score from connection count
        score += min(len(connections) / 10, 20)  # Max 20 points
        
        # Industry diversity bonus
        industries = set(conn.industry for conn in connections if conn.industry)
        score += min(len(industries) * 2, 20)  # Max 20 points
        
        # Seniority level bonus
        seniority = self._analyze_seniority_levels(connections)
        c_level_bonus = min(seniority.get("C-Level", 0) * 5, 20)  # Max 20 points
        vp_bonus = min(seniority.get("VP", 0) * 3, 15)  # Max 15 points
        score += c_level_bonus + vp_bonus
        
        # Mutual connections bonus
        high_mutual = len([c for c in connections if c.mutual_connections >= 10])
        score += min(high_mutual * 2, 15)  # Max 15 points
        
        return min(int(score), 100)
    
    def _identify_network_influencers(self, connections: List[NetworkConnection]) -> List[str]:
        """Identify potential influencers in the network"""
        influencers = []
        
        for conn in connections:
            # Criteria for influencers: high mutual connections, senior titles, known companies
            if (conn.mutual_connections >= 15 or 
                (conn.title and any(keyword in conn.title.upper() 
                                  for keyword in ["CEO", "FOUNDER", "PRESIDENT", "CHIEF"])) or
                (conn.company and conn.company in ["Microsoft", "Google", "Apple", "Amazon", "Meta", "Netflix"])):
                influencers.append(f"{conn.name} - {conn.title or 'Unknown Title'}")
        
        return influencers[:10]  # Top 10 influencers
    
    def _identify_decision_makers(self, connections: List[NetworkConnection]) -> List[str]:
        """Identify decision makers in the network"""
        decision_makers = []
        
        decision_keywords = ["CEO", "CTO", "CFO", "VP", "PRESIDENT", "DIRECTOR", "HEAD OF", "CHIEF"]
        
        for conn in connections:
            if conn.title and any(keyword in conn.title.upper() for keyword in decision_keywords):
                decision_makers.append(f"{conn.name} - {conn.title}")
        
        return decision_makers
    
    def _identify_industry_leaders(self, connections: List[NetworkConnection]) -> List[str]:
        """Identify industry leaders"""
        # This would ideally cross-reference with industry databases
        # For now, identify based on company and title combinations
        leader_companies = ["Microsoft", "Google", "Amazon", "Apple", "Meta", "Netflix", "Salesforce"]
        leaders = []
        
        for conn in connections:
            if (conn.company in leader_companies and 
                conn.title and any(keyword in conn.title.upper() 
                                 for keyword in ["VP", "DIRECTOR", "SENIOR", "PRINCIPAL"])):
                leaders.append(f"{conn.name} - {conn.title} at {conn.company}")
        
        return leaders
    
    def _identify_alumni_connections(self, connections: List[NetworkConnection]) -> List[str]:
        """Identify alumni connections (would need education data)"""
        # This is a placeholder - would need education information from LinkedIn
        # For THF context, look for military/academy connections
        alumni = []
        
        for conn in connections:
            if (conn.title and any(keyword in conn.title.upper() 
                                 for keyword in ["VETERAN", "MILITARY", "NAVY", "ARMY"]) or
                conn.company and "MILITARY" in conn.company.upper()):
                alumni.append(f"{conn.name} - Military/Veteran Connection")
        
        return alumni
    
    def _identify_thf_relevant_connections(self, connections: List[NetworkConnection]) -> Dict[str, List[str]]:
        """Identify connections relevant to THF mission"""
        relevant = {
            "veteran_connections": [],
            "hr_talent_professionals": [],
            "tech_industry": [],
            "nonprofit_sector": [],
            "executive_leadership": []
        }
        
        for conn in connections:
            name_title = f"{conn.name} - {conn.title or 'Unknown'}"
            
            # Veteran connections
            if (conn.title and any(keyword in conn.title.upper() 
                                 for keyword in ["VETERAN", "MILITARY", "NAVY", "ARMY", "AIR FORCE", "MARINES"]) or
                conn.industry == "Military"):
                relevant["veteran_connections"].append(name_title)
            
            # HR/Talent professionals
            if (conn.title and any(keyword in conn.title.upper() 
                                 for keyword in ["HR", "HUMAN RESOURCES", "TALENT", "RECRUITING", "RECRUITER"]) or
                conn.industry in ["Human Resources", "Staffing and Recruiting"]):
                relevant["hr_talent_professionals"].append(name_title)
            
            # Tech industry
            if (conn.industry == "Technology" or
                conn.company and any(tech in conn.company.upper() 
                                   for tech in ["TECH", "SOFTWARE", "MICROSOFT", "GOOGLE", "AMAZON", "APPLE"])):
                relevant["tech_industry"].append(name_title)
            
            # Nonprofit sector
            if conn.industry in ["Non-Profit", "Nonprofit"] or "NON-PROFIT" in (conn.company or "").upper():
                relevant["nonprofit_sector"].append(name_title)
            
            # Executive leadership
            if conn.title and any(keyword in conn.title.upper() 
                                for keyword in ["CEO", "PRESIDENT", "FOUNDER", "CHIEF"]):
                relevant["executive_leadership"].append(name_title)
        
        return relevant
    
    def _process_enhanced_apollo_results(self, results: List[Dict], person_data: Dict) -> Dict[str, Any]:
        """Process enhanced Apollo results with external data"""
        if not results:
            return {}
        
        best_match = results[0]  # Take first result for now
        
        processed = {
            # Standard contact data
            'raw_data': json.dumps(best_match),
            'email': best_match.get('email'),
            'personal_email': best_match.get('personal_email'),
            'phone': best_match.get('phone_number'),
            'mobile': best_match.get('mobile_phone_number'),
            'email_verified': best_match.get('email_verified', False),
            'phone_verified': best_match.get('phone_verified', False),
            'email_source': best_match.get('email_source', 'Unknown'),
            'phone_source': best_match.get('phone_source', 'Unknown'),
            
            # Professional data
            'title': best_match.get('title'),
            'company': best_match.get('organization_name'),
            'industry': best_match.get('industry'),
            'department': best_match.get('department'),
            'seniority': best_match.get('seniority'),
            
            # Location data
            'city': best_match.get('city'),
            'state': best_match.get('state'),
            'country': best_match.get('country'),
            'linkedin_url': best_match.get('linkedin_url'),
            
            # Enhanced external data
            'company_size': best_match.get('organization_num_employees'),
            'revenue_range': best_match.get('organization_annual_revenue'),
            'funding_stage': best_match.get('organization_funding_stage'),
            'employee_headcount_change': best_match.get('organization_headcount_change'),
            'technographics': best_match.get('organization_technologies', []),
            'intent_data': best_match.get('intent_signals'),
            'news_mentions': best_match.get('news_mentions_count', 0),
            'social_media_presence': best_match.get('social_media_profiles', []),
            'patent_count': best_match.get('patent_count', 0),
            'press_releases': best_match.get('press_releases', []),
            'event_participation': best_match.get('event_participation', []),
            'board_positions': best_match.get('board_positions', []),
            'advisory_roles': best_match.get('advisory_roles', []),
            'speaking_engagements': best_match.get('speaking_engagements', []),
            'external_certifications': best_match.get('external_certifications', []),
            'publications': best_match.get('publications', [])
        }
        
        return processed
    
    def _process_enhanced_linkedin_results(self, profile_data: Dict) -> Dict[str, Any]:
        """Process enhanced LinkedIn profile results"""
        return {
            'raw_data': json.dumps(profile_data),
            'headline': profile_data.get('headline'),
            'summary': profile_data.get('summary'),
            'location': profile_data.get('location'),
            'industry': profile_data.get('industry'),
            'connections': profile_data.get('connections'),
            'followers': profile_data.get('followers'),
            'current_position': profile_data.get('currentPosition'),
            'current_company': profile_data.get('currentCompany'),
            'experience_count': len(profile_data.get('experience', [])),
            'education_count': len(profile_data.get('education', [])),
            'top_skills': ', '.join(profile_data.get('skills', [])[:10]),
            'certifications': json.dumps(profile_data.get('certifications', [])),
            'languages': ', '.join(profile_data.get('languages', [])),
            'last_activity': profile_data.get('lastActivityDate'),
            'post_frequency': profile_data.get('postFrequency', 'Unknown'),
            'influencer_score': profile_data.get('influencerScore', 'Unknown'),
            'engagement_rate': profile_data.get('engagementRate'),
            'content_topics': ', '.join(profile_data.get('contentTopics', [])),
            'thought_leadership_score': profile_data.get('thoughtLeadershipScore'),
            'experience': json.dumps(profile_data.get('experience', [])),
            'education': json.dumps(profile_data.get('education', []))
        }
    
    def _process_connections_results(self, results: List[Dict]) -> List[NetworkConnection]:
        """Process LinkedIn connections results"""
        connections = []
        
        for result in results:
            if isinstance(result, dict) and result.get('connections'):
                for conn_data in result['connections']:
                    connection = NetworkConnection(
                        name=conn_data.get('name', 'Unknown'),
                        title=conn_data.get('title'),
                        company=conn_data.get('company'),
                        industry=conn_data.get('industry'),
                        location=conn_data.get('location'),
                        mutual_connections=conn_data.get('mutualConnections', 0),
                        profile_url=conn_data.get('profileUrl')
                    )
                    connections.append(connection)
        
        return connections
    
    def _add_error(self, record: EnhancedEnrichmentRecord, error: str):
        """Add error to enrichment record"""
        if not record.errors:
            record.errors = []
        record.errors.append(error)
        print(f"   ❌ {error}")
    
    def _calculate_enhanced_completeness(self, record: EnhancedEnrichmentRecord) -> int:
        """Calculate enhanced completeness score including network data"""
        total_fields = 0
        filled_fields = 0
        
        # Apollo fields (40% weight)
        if record.apollo_data:
            apollo_fields = ['email', 'phone', 'title', 'company', 'industry', 'intent_data', 'technographics']
            for field in apollo_fields:
                total_fields += 1
                if record.apollo_data.get(field):
                    filled_fields += 1
        
        # LinkedIn profile fields (30% weight)
        if record.linkedin_data:
            linkedin_fields = ['headline', 'summary', 'experience', 'education', 'skills']
            for field in linkedin_fields:
                total_fields += 1
                if record.linkedin_data.get(field):
                    filled_fields += 1
        
        # Network analysis (30% weight)
        if record.linkedin_connections:
            total_fields += 3  # Connections, analysis, network strength
            if len(record.linkedin_connections) > 0:
                filled_fields += 1
            if record.network_analysis:
                filled_fields += 2
        
        return int((filled_fields / total_fields) * 100) if total_fields > 0 else 0
    
    def _assess_enhanced_confidence(self, record: EnhancedEnrichmentRecord) -> str:
        """Assess enhanced confidence including external data validation"""
        confidence_score = 0
        
        # Apollo external data validation
        if record.apollo_data:
            if record.apollo_data.get('email_verified'):
                confidence_score += 25
            if record.apollo_data.get('phone_verified'):
                confidence_score += 20
            if record.apollo_data.get('intent_data'):
                confidence_score += 10
            if record.apollo_data.get('news_mentions', 0) > 0:
                confidence_score += 5
        
        # LinkedIn comprehensive profile
        if record.linkedin_data:
            if record.linkedin_data.get('experience_count', 0) >= 2:
                confidence_score += 15
            if record.linkedin_data.get('connections', 0) >= 100:
                confidence_score += 10
        
        # Network analysis depth
        if record.linkedin_connections and len(record.linkedin_connections) >= 20:
            confidence_score += 15
        
        if confidence_score >= 80:
            return "High"
        elif confidence_score >= 50:
            return "Medium"
        else:
            return "Low"
    
    def _determine_enrichment_status(self, record: EnhancedEnrichmentRecord):
        """Determine final enrichment status"""
        if record.errors:
            if record.apollo_data or record.linkedin_data:
                record.enrichment_status = "Partial"
            else:
                record.enrichment_status = "Failed"
        else:
            record.enrichment_status = "Completed"
    
    def _store_enhanced_enrichment_record(self, record: EnhancedEnrichmentRecord, person_data: Dict) -> Optional[str]:
        """Store enhanced enrichment record with all external data and network analysis"""
        
        properties = {
            "Name": {"title": [{"text": {"content": record.person_name}}]},
            "Original Record ID": {"rich_text": [{"text": {"content": record.original_record_id}}]},
            "Enrichment Date": {"date": {"start": datetime.now().isoformat()}},
            "Enrichment Status": {"select": {"name": record.enrichment_status}},
            "Data Confidence": {"select": {"name": record.data_confidence}},
            "Completeness Score": {"number": record.completeness_score}
        }
        
        # Data sources
        if record.external_data_sources:
            sources = [{"name": source} for source in record.external_data_sources]
            properties["Data Sources"] = {"multi_select": sources}
        
        # Apollo enhanced data
        if record.apollo_data:
            apollo_fields = {
                "Apollo Email": record.apollo_data.get('email'),
                "Apollo Personal Email": record.apollo_data.get('personal_email'),
                "Apollo Phone": record.apollo_data.get('phone'),
                "Apollo Mobile": record.apollo_data.get('mobile'),
                "Apollo Email Verified": record.apollo_data.get('email_verified'),
                "Apollo Phone Verified": record.apollo_data.get('phone_verified'),
                "Apollo Email Source": record.apollo_data.get('email_source'),
                "Apollo Phone Source": record.apollo_data.get('phone_source'),
                "Apollo Title": record.apollo_data.get('title'),
                "Apollo Company": record.apollo_data.get('company'),
                "Apollo Industry": record.apollo_data.get('industry'),
                "Apollo City": record.apollo_data.get('city'),
                "Apollo State": record.apollo_data.get('state'),
                "Apollo Country": record.apollo_data.get('country'),
                "Apollo Intent Data": str(record.apollo_data.get('intent_data', '')),
                "Apollo Technographics": ', '.join(record.apollo_data.get('technographics', [])),
                "Apollo Revenue Range": record.apollo_data.get('revenue_range'),
                "Apollo Funding Stage": record.apollo_data.get('funding_stage'),
                "Apollo News Mentions": record.apollo_data.get('news_mentions'),
                "Apollo Patent Count": record.apollo_data.get('patent_count'),
                "Apollo Raw Data": record.apollo_data.get('raw_data', '')
            }
            
            for field_name, value in apollo_fields.items():
                if value is not None and str(value).strip():
                    if field_name.endswith('Email'):
                        properties[field_name] = {"email": str(value)}
                    elif field_name.endswith('Phone') or field_name.endswith('Mobile'):
                        properties[field_name] = {"phone_number": str(value)}
                    elif field_name.endswith('Verified'):
                        properties[field_name] = {"checkbox": bool(value)}
                    elif field_name.endswith('Count') or field_name.endswith('Mentions'):
                        if str(value).isdigit():
                            properties[field_name] = {"number": int(value)}
                    else:
                        properties[field_name] = {"rich_text": [{"text": {"content": str(value)[:2000]}}]}
        
        # LinkedIn enhanced data and network analysis
        if record.linkedin_data:
            linkedin_fields = {
                "LinkedIn Headline": record.linkedin_data.get('headline'),
                "LinkedIn Summary": record.linkedin_data.get('summary'),
                "LinkedIn Location": record.linkedin_data.get('location'),
                "LinkedIn Industry": record.linkedin_data.get('industry'),
                "LinkedIn Connections": record.linkedin_data.get('connections'),
                "LinkedIn Followers": record.linkedin_data.get('followers'),
                "LinkedIn Current Position": record.linkedin_data.get('current_position'),
                "LinkedIn Current Company": record.linkedin_data.get('current_company'),
                "LinkedIn Experience Count": record.linkedin_data.get('experience_count'),
                "LinkedIn Education Count": record.linkedin_data.get('education_count'),
                "LinkedIn Top Skills": record.linkedin_data.get('top_skills'),
                "LinkedIn Raw Data": record.linkedin_data.get('raw_data', ''),
                "LinkedIn Experience": record.linkedin_data.get('experience', ''),
                "LinkedIn Education": record.linkedin_data.get('education', '')
            }
            
            for field_name, value in linkedin_fields.items():
                if value is not None and str(value).strip():
                    if field_name.endswith('Count') or field_name.endswith('Connections') or field_name.endswith('Followers'):
                        if str(value).isdigit():
                            properties[field_name] = {"number": int(value)}
                    else:
                        properties[field_name] = {"rich_text": [{"text": {"content": str(value)[:2000]}}]}
        
        # Network analysis data
        if record.linkedin_connections:
            connection_names = [conn.name for conn in record.linkedin_connections[:25]]  # Top 25
            connection_titles = [conn.title for conn in record.linkedin_connections[:25] if conn.title]
            connection_companies = [conn.company for conn in record.linkedin_connections[:25] if conn.company]
            
            properties["LinkedIn First Degree Connections"] = {"number": len(record.linkedin_connections)}
            properties["LinkedIn Connection Names"] = {"rich_text": [{"text": {"content": ', '.join(connection_names)[:2000]}}]}
            properties["LinkedIn Connection Titles"] = {"rich_text": [{"text": {"content": ', '.join(connection_titles)[:2000]}}]}
            properties["LinkedIn Connection Companies"] = {"rich_text": [{"text": {"content": ', '.join(connection_companies)[:2000]}}]}
        
        if record.network_analysis:
            properties["LinkedIn Network Strength Score"] = {"number": record.network_analysis.get('network_strength_score', 0)}
            
            # Store network analysis as JSON
            network_json = json.dumps(record.network_analysis)
            properties["LinkedIn Connections Raw"] = {"rich_text": [{"text": {"content": network_json[:2000]}}]}
        
        # Errors
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
            print(f"   ✅ Stored comprehensive enrichment record: {page_info['id']}")
            return page_info['id']
            
        except requests.exceptions.RequestException as e:
            print(f"   ❌ Failed to store enrichment record: {e}")
            return None
    
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
        """Wait for and retrieve actor results"""
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
                time.sleep(15)
                
            except requests.exceptions.RequestException as e:
                print(f"      ❌ Error checking run status: {e}")
                return None
        
        print(f"      ⏰ Actor run timed out after {max_wait_time} seconds")
        return None
    
    def _link_to_people_db(self, person_id: str, enrichment_page_id: str):
        """Link enrichment record back to People DB"""
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

def main():
    """Test enhanced enrichment service"""
    
    print("🎖️  THF ENHANCED ENRICHMENT SERVICE")
    print("=" * 60)
    
    # Configuration
    try:
        with open('database_config.json', 'r') as f:
            config = json.load(f)
            
        PEOPLE_DB_ID = config['people_db_id']
        ENRICHMENT_DB_ID = config['enrichment_db_id']
        NOTION_TOKEN = config['notion_token']
        
    except FileNotFoundError:
        print("❌ Database configuration not found")
        print("   Run create_enrichment_db.py first")
        return
    
    import os
    APIFY_TOKEN = os.environ.get('APIFY_TOKEN')
    
    if not APIFY_TOKEN:
        print("❌ APIFY_TOKEN not set")
        return
    
    # Initialize enhanced service
    service = EnhancedApifyEnrichmentService(APIFY_TOKEN, NOTION_TOKEN, PEOPLE_DB_ID, ENRICHMENT_DB_ID)
    
    # Test with first person
    people = service.thf_intel.get_all_people()
    if not people:
        print("❌ No people found")
        return
    
    person_data = service.thf_intel.extract_person_data(people[0])
    print(f"🧪 Testing comprehensive enrichment with: {person_data.get('name', 'Unknown')}")
    
    # Run comprehensive enrichment
    result = service.comprehensive_enrich_person(people[0]['id'])
    
    print(f"\n📋 COMPREHENSIVE ENRICHMENT RESULTS:")
    print(f"   Status: {result.enrichment_status}")
    print(f"   Confidence: {result.data_confidence}")
    print(f"   Completeness: {result.completeness_score}%")
    print(f"   External Sources: {', '.join(result.external_data_sources or [])}")
    
    if result.apollo_data:
        print(f"   Apollo Enhanced Data: ✅")
        
    if result.linkedin_data:
        print(f"   LinkedIn Profile Data: ✅")
    
    if result.linkedin_connections:
        print(f"   LinkedIn Network Analysis: ✅ ({len(result.linkedin_connections)} connections)")
    
    if result.network_analysis:
        network_score = result.network_analysis.get('network_strength_score', 0)
        print(f"   Network Strength Score: {network_score}/100")
    
    if result.errors:
        print(f"   Errors: {len(result.errors)}")
        for error in result.errors:
            print(f"     • {error}")

if __name__ == "__main__":
    main()
#!/usr/bin/env python3
"""
THF Professional Network Intelligence Tool
Advanced analytics and insights for The Honor Foundation's People DB
"""

import requests
import json
from typing import Dict, List, Any, Optional, Tuple
from collections import defaultdict, Counter
from datetime import datetime
import re

class THFIntelligence:
    def __init__(self, integration_token: str, people_db_id: str):
        self.integration_token = integration_token
        self.people_db_id = people_db_id
        self.base_url = "https://api.notion.com/v1"
        self.headers = {
            "Authorization": f"Bearer {integration_token}",
            "Notion-Version": "2022-06-28",
            "Content-Type": "application/json"
        }
        self._cache = {}
    
    def get_all_people(self) -> List[Dict[str, Any]]:
        """Retrieve all people from the database with pagination"""
        if 'all_people' in self._cache:
            return self._cache['all_people']
            
        all_people = []
        has_more = True
        start_cursor = None
        
        while has_more:
            url = f"{self.base_url}/databases/{self.people_db_id}/query"
            payload = {"page_size": 100}
            
            if start_cursor:
                payload["start_cursor"] = start_cursor
            
            try:
                response = requests.post(url, headers=self.headers, json=payload)
                response.raise_for_status()
                data = response.json()
                
                all_people.extend(data.get('results', []))
                has_more = data.get('has_more', False)
                start_cursor = data.get('next_cursor')
                
            except requests.exceptions.RequestException as e:
                print(f"Error retrieving people: {e}")
                break
        
        self._cache['all_people'] = all_people
        return all_people
    
    def extract_person_data(self, person: Dict[str, Any]) -> Dict[str, Any]:
        """Extract clean data from a person record"""
        properties = person.get('properties', {})
        
        def get_value(prop_name: str, prop_type: str = None) -> Any:
            prop = properties.get(prop_name, {})
            if not prop:
                return None
                
            prop_type = prop_type or prop.get('type')
            
            if prop_type == 'title':
                return prop.get('title', [{}])[0].get('plain_text') if prop.get('title') else None
            elif prop_type == 'rich_text':
                return prop.get('rich_text', [{}])[0].get('plain_text') if prop.get('rich_text') else None
            elif prop_type == 'select':
                return prop.get('select', {}).get('name') if prop.get('select') else None
            elif prop_type == 'multi_select':
                return [item.get('name') for item in prop.get('multi_select', [])]
            elif prop_type == 'email':
                return prop.get('email')
            elif prop_type == 'phone_number':
                return prop.get('phone_number')
            elif prop_type == 'url':
                return prop.get('url')
            elif prop_type == 'checkbox':
                return prop.get('checkbox', False)
            elif prop_type == 'date':
                date_obj = prop.get('date')
                return date_obj.get('start') if date_obj else None
            elif prop_type == 'status':
                status_obj = prop.get('status')
                return status_obj.get('name') if status_obj else None
            else:
                return str(prop) if prop else None
        
        return {
            'id': person.get('id'),
            'name': get_value('Name', 'title'),
            'position': get_value('Position'),
            'employer': get_value('Employer'),
            'industry': get_value('Industry'),
            'branch': get_value('Branch'),
            'job': get_value('Job'),
            'seniority_level': get_value('Seniority Level'),
            'country': get_value('Country'),
            'state': get_value('State'),
            'residence': get_value('Residence'),
            'military': get_value('Military', 'checkbox'),
            'undergrad_school': get_value('Undergrad School'),
            'undergrad_degree': get_value('Undergrad Degree'),
            'graduate_school': get_value('Graduate School'),
            'graduate_degree': get_value('Graduate Degree'),
            'post_grad_school': get_value('Post Grad School'),
            'post_grad_degree': get_value('Post Grad Degree'),
            'primary_email': get_value('Primary Email', 'email'),
            'personal_email': get_value('Personal Email', 'email'),
            'phone': get_value('Phone', 'phone_number'),
            'linkedin': get_value('LinkedIn Profile', 'url'),
            'personal_profile': get_value('Personal Profile', 'url'),
            'facebook': get_value('Facebook', 'url'),
            'instagram': get_value('Instagram', 'url'),
            'twitter': get_value('Twitter (X)', 'url'),
            'status': get_value('Status', 'status'),
            'organizations': get_value('Organizations'),
            'role_in_org': get_value('Role in Organization'),
            'network_first_order': get_value('Network (First Order)'),
            'date_of_birth': get_value('Date of Birth', 'date'),
            'created_time': person.get('created_time'),
            'last_edited_time': person.get('last_edited_time')
        }
    
    def generate_network_intelligence(self) -> Dict[str, Any]:
        """Generate comprehensive network intelligence report"""
        people = self.get_all_people()
        clean_data = [self.extract_person_data(person) for person in people]
        
        # Filter out records without names
        clean_data = [p for p in clean_data if p.get('name')]
        
        report = {
            'summary': {
                'total_contacts': len(clean_data),
                'last_updated': datetime.now().isoformat(),
                'data_quality_score': self._calculate_data_quality(clean_data)
            },
            'demographics': self._analyze_demographics(clean_data),
            'professional_analysis': self._analyze_professional_data(clean_data),
            'education_analysis': self._analyze_education(clean_data),
            'military_analysis': self._analyze_military_background(clean_data),
            'geographic_distribution': self._analyze_geography(clean_data),
            'network_connectivity': self._analyze_network_connectivity(clean_data),
            'contact_information': self._analyze_contact_coverage(clean_data),
            'top_insights': self._generate_key_insights(clean_data)
        }
        
        return report
    
    def _calculate_data_quality(self, data: List[Dict]) -> float:
        """Calculate data completeness score"""
        key_fields = ['name', 'primary_email', 'employer', 'position', 'linkedin']
        total_possible = len(data) * len(key_fields)
        total_filled = sum(1 for person in data for field in key_fields if person.get(field))
        return round((total_filled / total_possible) * 100, 1) if total_possible > 0 else 0
    
    def _analyze_demographics(self, data: List[Dict]) -> Dict[str, Any]:
        """Analyze demographic distribution"""
        military_count = sum(1 for p in data if p.get('military'))
        status_dist = Counter(p.get('status') for p in data if p.get('status'))
        
        return {
            'military_background': {
                'count': military_count,
                'percentage': round((military_count / len(data)) * 100, 1) if data else 0
            },
            'status_distribution': dict(status_dist.most_common()),
            'branch_distribution': dict(Counter(p.get('branch') for p in data if p.get('branch')).most_common())
        }
    
    def _analyze_professional_data(self, data: List[Dict]) -> Dict[str, Any]:
        """Analyze professional background"""
        industries = Counter(p.get('industry') for p in data if p.get('industry'))
        employers = Counter(p.get('employer') for p in data if p.get('employer'))
        seniority = Counter(p.get('seniority_level') for p in data if p.get('seniority_level'))
        
        return {
            'top_industries': dict(industries.most_common(10)),
            'top_employers': dict(employers.most_common(10)),
            'seniority_distribution': dict(seniority.most_common()),
            'industry_diversity': len(industries),
            'employer_diversity': len(employers)
        }
    
    def _analyze_education(self, data: List[Dict]) -> Dict[str, Any]:
        """Analyze educational backgrounds"""
        undergrad_schools = Counter(p.get('undergrad_school') for p in data if p.get('undergrad_school'))
        grad_schools = Counter(p.get('graduate_school') for p in data if p.get('graduate_school'))
        
        return {
            'top_undergrad_schools': dict(undergrad_schools.most_common(10)),
            'top_graduate_schools': dict(grad_schools.most_common(10)),
            'graduate_degree_holders': sum(1 for p in data if p.get('graduate_degree')),
            'education_diversity': len(undergrad_schools)
        }
    
    def _analyze_military_background(self, data: List[Dict]) -> Dict[str, Any]:
        """Analyze military service data"""
        military_people = [p for p in data if p.get('military')]
        
        if not military_people:
            return {'message': 'No military background data available'}
        
        branches = Counter(p.get('branch') for p in military_people if p.get('branch'))
        jobs = Counter(p.get('job') for p in military_people if p.get('job'))
        
        return {
            'total_veterans': len(military_people),
            'branch_breakdown': dict(branches.most_common()),
            'top_military_jobs': dict(jobs.most_common(10)),
            'veteran_percentage': round((len(military_people) / len(data)) * 100, 1)
        }
    
    def _analyze_geography(self, data: List[Dict]) -> Dict[str, Any]:
        """Analyze geographic distribution"""
        countries = Counter(p.get('country') for p in data if p.get('country'))
        states = Counter(p.get('state') for p in data if p.get('state'))
        cities = Counter(p.get('residence') for p in data if p.get('residence'))
        
        return {
            'countries': dict(countries.most_common()),
            'top_states': dict(states.most_common(10)),
            'top_cities': dict(cities.most_common(10)),
            'geographic_diversity': len(countries)
        }
    
    def _analyze_network_connectivity(self, data: List[Dict]) -> Dict[str, Any]:
        """Analyze network connectivity and relationships"""
        linkedin_count = sum(1 for p in data if p.get('linkedin'))
        social_profiles = sum(1 for p in data if any([p.get('facebook'), p.get('instagram'), p.get('twitter')]))
        
        return {
            'linkedin_coverage': {
                'count': linkedin_count,
                'percentage': round((linkedin_count / len(data)) * 100, 1) if data else 0
            },
            'social_media_presence': {
                'count': social_profiles,
                'percentage': round((social_profiles / len(data)) * 100, 1) if data else 0
            }
        }
    
    def _analyze_contact_coverage(self, data: List[Dict]) -> Dict[str, Any]:
        """Analyze contact information coverage"""
        email_count = sum(1 for p in data if p.get('primary_email') or p.get('personal_email'))
        phone_count = sum(1 for p in data if p.get('phone'))
        
        return {
            'email_coverage': {
                'count': email_count,
                'percentage': round((email_count / len(data)) * 100, 1) if data else 0
            },
            'phone_coverage': {
                'count': phone_count,
                'percentage': round((phone_count / len(data)) * 100, 1) if data else 0
            }
        }
    
    def _generate_key_insights(self, data: List[Dict]) -> List[str]:
        """Generate key insights from the data"""
        insights = []
        
        # Military analysis
        military_count = sum(1 for p in data if p.get('military'))
        if military_count > 0:
            mil_pct = round((military_count / len(data)) * 100, 1)
            insights.append(f"{mil_pct}% of contacts have military background ({military_count} veterans)")
        
        # Industry concentration
        industries = Counter(p.get('industry') for p in data if p.get('industry'))
        if industries:
            top_industry, count = industries.most_common(1)[0]
            pct = round((count / len(data)) * 100, 1)
            insights.append(f"Top industry: {top_industry} ({pct}% of contacts)")
        
        # Geographic concentration
        states = Counter(p.get('state') for p in data if p.get('state'))
        if states:
            top_state, count = states.most_common(1)[0]
            pct = round((count / len(data)) * 100, 1)
            insights.append(f"Largest geographic concentration: {top_state} ({pct}% of contacts)")
        
        # LinkedIn coverage
        linkedin_count = sum(1 for p in data if p.get('linkedin'))
        if linkedin_count > 0:
            pct = round((linkedin_count / len(data)) * 100, 1)
            insights.append(f"LinkedIn connectivity: {pct}% of contacts have LinkedIn profiles")
        
        # Data quality
        quality_score = self._calculate_data_quality(data)
        insights.append(f"Overall data completeness: {quality_score}%")
        
        return insights
    
    def search_contacts(self, query: str, field: str = None) -> List[Dict[str, Any]]:
        """Search for contacts by name, employer, industry, etc."""
        people = self.get_all_people()
        clean_data = [self.extract_person_data(person) for person in people]
        
        query_lower = query.lower()
        results = []
        
        for person in clean_data:
            if not person.get('name'):
                continue
                
            # Search in specific field if provided
            if field and field in person:
                if person.get(field) and query_lower in str(person.get(field)).lower():
                    results.append(person)
            else:
                # Search across multiple fields
                search_fields = ['name', 'employer', 'industry', 'position', 'undergrad_school', 'graduate_school']
                for search_field in search_fields:
                    if person.get(search_field) and query_lower in str(person.get(search_field)).lower():
                        results.append(person)
                        break
        
        return results

def main():
    # THF Configuration
    INTEGRATION_TOKEN = "your_notion_api_token_placeholder"
    PEOPLE_DB_ID = "258c2a32-df0d-80f3-944f-cf819718d96a"
    
    # Initialize intelligence tool
    thf = THFIntelligence(INTEGRATION_TOKEN, PEOPLE_DB_ID)
    
    print("🎖️  THF PROFESSIONAL NETWORK INTELLIGENCE REPORT")
    print("=" * 60)
    
    # Generate comprehensive report
    report = thf.generate_network_intelligence()
    
    # Display summary
    print(f"\n📊 NETWORK SUMMARY")
    print("-" * 30)
    print(f"Total Contacts: {report['summary']['total_contacts']}")
    print(f"Data Quality Score: {report['summary']['data_quality_score']}%")
    
    # Key insights
    print(f"\n🔍 KEY INSIGHTS")
    print("-" * 30)
    for insight in report['top_insights']:
        print(f"• {insight}")
    
    # Military analysis
    print(f"\n🪖 MILITARY ANALYSIS")
    print("-" * 30)
    mil_data = report['military_analysis']
    if 'total_veterans' in mil_data:
        print(f"Total Veterans: {mil_data['total_veterans']}")
        print(f"Veteran Percentage: {mil_data['veteran_percentage']}%")
        print("Branch Distribution:")
        for branch, count in mil_data['branch_breakdown'].items():
            print(f"  • {branch}: {count}")
    
    # Professional analysis
    print(f"\n💼 PROFESSIONAL ANALYSIS")
    print("-" * 30)
    prof_data = report['professional_analysis']
    print("Top Industries:")
    for industry, count in list(prof_data['top_industries'].items())[:5]:
        print(f"  • {industry}: {count}")
    
    print("\nTop Employers:")
    for employer, count in list(prof_data['top_employers'].items())[:5]:
        print(f"  • {employer}: {count}")
    
    # Geographic distribution
    print(f"\n🌎 GEOGRAPHIC DISTRIBUTION")
    print("-" * 30)
    geo_data = report['geographic_distribution']
    print("Top States:")
    for state, count in list(geo_data['top_states'].items())[:5]:
        print(f"  • {state}: {count}")
    
    # Contact coverage
    print(f"\n📞 CONTACT COVERAGE")
    print("-" * 30)
    contact_data = report['contact_information']
    print(f"Email Coverage: {contact_data['email_coverage']['percentage']}%")
    print(f"Phone Coverage: {contact_data['phone_coverage']['percentage']}%")
    linkedin_data = report['network_connectivity']
    print(f"LinkedIn Coverage: {linkedin_data['linkedin_coverage']['percentage']}%")
    
    print(f"\n📈 Full report generated at: {report['summary']['last_updated']}")

if __name__ == "__main__":
    main()
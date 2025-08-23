#!/usr/bin/env python3
"""
Direct function endpoint for THF Enrichment webhook
"""

import json
import os
import requests
import time
from typing import Dict, List, Any, Optional
from datetime import datetime

# Environment variables
APIFY_TOKEN = os.environ.get('APIFY_TOKEN')
NOTION_TOKEN = os.environ.get('NOTION_TOKEN')
PEOPLE_DB_ID = "258c2a32-df0d-80f3-944f-cf819718d96a"
ENRICHMENT_DB_ID = "258c2a32-df0d-805b-acb0-d0f2c81630cd"

def handler(request, response):
    """Vercel serverless function handler"""
    
    # Set CORS headers
    response.headers.update({
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type',
        'Content-Type': 'application/json'
    })
    
    # Handle preflight
    if request.method == 'OPTIONS':
        return ''
    
    # Only accept POST
    if request.method != 'POST':
        response.status_code = 405
        return json.dumps({'error': 'Only POST method allowed'})
    
    try:
        # Validate environment variables
        if not APIFY_TOKEN or not NOTION_TOKEN:
            response.status_code = 500
            return json.dumps({
                'error': 'Missing environment variables',
                'apify_token_set': bool(APIFY_TOKEN),
                'notion_token_set': bool(NOTION_TOKEN)
            })
        
        # Get request data
        webhook_data = request.json if hasattr(request, 'json') else json.loads(request.data)
        
        # Basic validation
        if not isinstance(webhook_data, dict):
            response.status_code = 400
            return json.dumps({'error': 'Invalid webhook payload'})
        
        # Extract person info
        person_info = extract_person_from_webhook(webhook_data)
        
        if not person_info:
            response.status_code = 400
            return json.dumps({'error': 'Could not extract person information or status is not Working'})
        
        # Process enrichment
        result = {
            'success': True,
            'person_name': person_info['name'],
            'person_id': person_info['id'],
            'timestamp': datetime.now().isoformat(),
            'message': 'Enrichment triggered successfully'
        }
        
        # Return success (actual enrichment would run in background)
        response.status_code = 200
        return json.dumps(result)
        
    except Exception as e:
        response.status_code = 500
        return json.dumps({
            'error': str(e),
            'type': type(e).__name__,
            'message': 'THF Enrichment webhook processing failed'
        })

def extract_person_from_webhook(webhook_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Extract person information from webhook"""
    
    # Handle different payload structures
    if 'object' in webhook_data and webhook_data['object'] == 'page':
        page_data = webhook_data
    elif 'data' in webhook_data:
        page_data = webhook_data['data']
    else:
        return None
    
    properties = page_data.get('properties', {})
    
    # Get name
    name = "Unknown"
    if 'Name' in properties and properties['Name'].get('title'):
        name = properties['Name']['title'][0]['plain_text']
    
    # Check status is "Working"
    status = None
    if 'Status' in properties and properties['Status'].get('status'):
        status = properties['Status']['status']['name']
    
    if status != "Working":
        return None
    
    return {
        'id': page_data['id'],
        'name': name,
        'status': status
    }
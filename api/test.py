#!/usr/bin/env python3
"""
Simple test endpoint to verify Vercel deployment
"""

import json
import os
from datetime import datetime

def handler(request, response):
    """Test handler to verify deployment"""
    
    # Set headers
    response.headers.update({
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type'
    })
    
    # Handle preflight
    if request.method == 'OPTIONS':
        return ''
    
    try:
        result = {
            'status': 'success',
            'message': 'THF Enrichment webhook is deployed and working',
            'timestamp': datetime.now().isoformat(),
            'environment': {
                'apify_token_set': bool(os.environ.get('APIFY_TOKEN')),
                'notion_token_set': bool(os.environ.get('NOTION_TOKEN')),
                'people_db_id': "258c2a32-df0d-80f3-944f-cf819718d96a",
                'enrichment_db_id': "258c2a32-df0d-805b-acb0-d0f2c81630cd"
            },
            'request_info': {
                'method': request.method,
                'has_json': hasattr(request, 'json'),
                'headers_available': bool(getattr(request, 'headers', None))
            }
        }
        
        response.status_code = 200
        return json.dumps(result, indent=2)
        
    except Exception as e:
        response.status_code = 500
        return json.dumps({
            'status': 'error',
            'message': str(e),
            'type': type(e).__name__
        })
#!/usr/bin/env python3
"""
Simple Vercel webhook endpoint for THF Enrichment
"""

import json
import os
from webhook_enrichment import WebhookEnrichmentProcessor

def handler(request, response):
    """Simple Vercel handler for enrichment webhook"""
    
    # CORS headers
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'POST, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type'
    
    # Handle preflight
    if request.method == 'OPTIONS':
        response.status_code = 200
        return ''
    
    if request.method != 'POST':
        response.status_code = 405
        return json.dumps({'error': 'Method not allowed'})
    
    try:
        # Get webhook data
        webhook_data = request.json
        
        # Process enrichment
        processor = WebhookEnrichmentProcessor()
        result = processor.process_webhook(webhook_data)
        
        # Return result
        response.status_code = 200 if result.get('success') else 500
        response.headers['Content-Type'] = 'application/json'
        
        return json.dumps(result)
        
    except Exception as e:
        response.status_code = 500
        response.headers['Content-Type'] = 'application/json'
        return json.dumps({'error': str(e), 'message': 'THF Enrichment webhook error'})
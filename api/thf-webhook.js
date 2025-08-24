// THF Enrichment Webhook - Public endpoint without authentication
export default async function handler(req, res) {
  // Set CORS headers
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

  // Handle preflight requests
  if (req.method === 'OPTIONS') {
    return res.status(200).end();
  }

  // Test endpoint - GET request
  if (req.method === 'GET') {
    return res.status(200).json({
      status: 'success',
      message: '🎖️ THF Public Webhook - Ready for Notion!',
      timestamp: new Date().toISOString(),
      environment: {
        apollo_api_key_set: !!process.env.APOLLO_API_KEY,
        notion_token_set: !!process.env.NOTION_TOKEN,
        node_version: process.version
      }
    });
  }

  // Handle webhook POST
  if (req.method === 'POST') {
    try {
      console.log('🎯 PUBLIC WEBHOOK RECEIVED!');
      console.log('Headers:', JSON.stringify(req.headers, null, 2));
      console.log('Body (FULL DEBUG):', JSON.stringify(req.body, null, 2));
      console.log('Body keys:', Object.keys(req.body || {}));
      console.log('Body type:', typeof req.body);

      return res.status(200).json({
        success: true,
        message: 'Public webhook received successfully!',
        timestamp: new Date().toISOString(),
        debug: {
          body_received: !!req.body,
          body_keys: Object.keys(req.body || {}),
          body_type: typeof req.body
        }
      });

    } catch (error) {
      console.error('❌ Public webhook processing error:', error);
      return res.status(500).json({
        error: error.message,
        type: error.name,
        message: 'Public webhook processing failed'
      });
    }
  }

  // Method not allowed
  return res.status(405).json({ 
    error: 'Method not allowed',
    allowed_methods: ['GET', 'POST', 'OPTIONS']
  });
}
// THF Enrichment Webhook - Simple Node.js version
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
      message: '🎖️ THF Enrichment webhook is working!',
      timestamp: new Date().toISOString(),
      environment: {
        apify_token_set: !!process.env.APIFY_TOKEN,
        notion_token_set: !!process.env.NOTION_TOKEN,
        node_version: process.version
      },
      endpoints: {
        webhook: 'https://thf-2025.vercel.app/api/webhook',
        test: 'Send POST with Notion page data to trigger enrichment'
      }
    });
  }

  // Handle webhook POST
  if (req.method === 'POST') {
    try {
      console.log('🎯 Webhook received!');
      console.log('Headers:', JSON.stringify(req.headers, null, 2));
      console.log('Body:', JSON.stringify(req.body, null, 2));

      const webhookData = req.body;
      
      // Extract person info
      const personInfo = extractPersonFromWebhook(webhookData);
      
      if (!personInfo) {
        console.log('❌ No person info extracted or status not Working');
        return res.status(400).json({
          error: 'Could not extract person information or status is not Working',
          received_data: webhookData,
          extraction_help: 'Make sure the page has Name and Status=Working'
        });
      }

      console.log('✅ Person extracted:', personInfo);
      
      // TODO: Here we would call the Python enrichment service
      // For now, just log that we would start enrichment
      console.log('🚀 Would start enrichment for:', personInfo.name);
      
      const result = {
        success: true,
        message: 'Webhook processed successfully',
        person_name: personInfo.name,
        person_id: personInfo.id,
        timestamp: new Date().toISOString(),
        next_steps: [
          'Apollo enrichment would run',
          'LinkedIn enrichment would run', 
          'Data would be stored in Enrichment DB',
          'People DB status would be updated'
        ],
        extracted_info: personInfo
      };

      console.log('📤 Returning success:', result);
      return res.status(200).json(result);

    } catch (error) {
      console.error('❌ Webhook processing error:', error);
      return res.status(500).json({
        error: error.message,
        type: error.name,
        message: 'THF Enrichment webhook processing failed',
        stack: error.stack
      });
    }
  }

  // Method not allowed
  return res.status(405).json({ 
    error: 'Method not allowed',
    allowed_methods: ['GET', 'POST', 'OPTIONS']
  });
}

function extractPersonFromWebhook(webhookData) {
  console.log('🔍 Extracting person from webhook...');
  
  if (!webhookData) {
    console.log('❌ No webhook data');
    return null;
  }

  // Handle different payload structures
  let pageData;
  if (webhookData.object === 'page') {
    pageData = webhookData;
  } else if (webhookData.data && webhookData.data.object === 'page') {
    pageData = webhookData.data;
  } else if (webhookData.pages && webhookData.pages.length > 0) {
    pageData = webhookData.pages[0];
  } else {
    console.log('❌ Could not find page data in:', Object.keys(webhookData));
    return null;
  }

  console.log('📄 Found page data:', pageData.id);

  const properties = pageData.properties || {};
  console.log('📋 Properties available:', Object.keys(properties));

  // Get name
  let name = "Unknown";
  if (properties.Name && properties.Name.title && properties.Name.title.length > 0) {
    name = properties.Name.title[0].plain_text;
  }
  console.log('👤 Name:', name);

  // Check status is "Working" 
  let status = null;
  if (properties.Status && properties.Status.status) {
    status = properties.Status.status.name;
  } else if (properties.Status && properties.Status.select) {
    status = properties.Status.select.name;
  }
  console.log('📊 Status:', status);

  // Only process if status is "Working"
  if (status !== "Working") {
    console.log('⏭️ Skipping - status is not Working');
    return null;
  }

  // Extract additional fields
  const email = properties['Primary Email']?.email || 
                properties['Personal Email']?.email || null;
  
  const linkedin = properties['LinkedIn Profile']?.url || 
                   properties['LinkedIn']?.url || null;
  
  const employer = properties.Employer?.rich_text?.[0]?.plain_text ||
                   properties.Company?.rich_text?.[0]?.plain_text || null;

  const position = properties.Position?.rich_text?.[0]?.plain_text ||
                   properties.Title?.rich_text?.[0]?.plain_text || null;

  const result = {
    id: pageData.id,
    name: name,
    status: status,
    email: email,
    linkedin: linkedin,
    employer: employer,
    position: position
  };

  console.log('✅ Extracted person info:', result);
  return result;
}
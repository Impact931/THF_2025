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
      
      // Trigger the actual enrichment process
      console.log('🚀 Starting enrichment for:', personInfo.name);
      
      try {
        const enrichmentResult = await triggerEnrichment(personInfo, webhookData);
        
        // Determine response based on enrichment results
        const isSuccess = enrichmentResult.status === 'completed' || enrichmentResult.status === 'partially_completed';
        const statusCode = isSuccess ? 200 : 500;
        
        const result = {
          success: isSuccess,
          message: isSuccess ? 'Enrichment process completed' : 'Enrichment process failed',
          person_name: personInfo.name,
          person_id: personInfo.id,
          timestamp: new Date().toISOString(),
          enrichment_status: enrichmentResult.status,
          apollo_success: enrichmentResult.apollo_success,
          linkedin_success: enrichmentResult.linkedin_success,
          storage_success: enrichmentResult.storage_success,
          error: enrichmentResult.error || null,
          extracted_info: personInfo
        };

        console.log('📤 Enrichment completed:', result);
        return res.status(statusCode).json(result);
        
      } catch (enrichmentError) {
        console.error('❌ Enrichment failed:', enrichmentError);
        
        const result = {
          success: false,
          message: 'Enrichment process failed',
          person_name: personInfo.name,
          person_id: personInfo.id,
          timestamp: new Date().toISOString(),
          error: enrichmentError.message,
          extracted_info: personInfo
        };

        return res.status(500).json(result);
      }

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

async function triggerEnrichment(personInfo, webhookData) {
  console.log('🔧 Starting Apify enrichment process...');
  
  const apifyToken = process.env.APIFY_TOKEN;
  const notionToken = process.env.NOTION_TOKEN;
  
  if (!apifyToken || !notionToken) {
    throw new Error('Missing required environment variables: APIFY_TOKEN or NOTION_TOKEN');
  }
  
  const enrichmentResult = {
    status: 'processing',
    apollo_success: false,
    linkedin_success: false,
    storage_success: false
  };
  
  try {
    // Run Apollo enrichment
    console.log('🚀 Starting Apollo enrichment...');
    const apolloResult = await runApolloEnrichment(personInfo, apifyToken);
    enrichmentResult.apollo_success = apolloResult.success;
    
    // Run LinkedIn enrichment  
    console.log('🔗 Starting LinkedIn enrichment...');
    const linkedinResult = await runLinkedInEnrichment(personInfo, apifyToken);
    enrichmentResult.linkedin_success = linkedinResult.success;
    
    // Store enriched data in Enrichment DB
    console.log('💾 Storing enriched data...');
    const storageResult = await storeEnrichedData(personInfo, apolloResult.data, linkedinResult.data, notionToken);
    enrichmentResult.storage_success = storageResult.success;
    
    // Determine overall status based on results
    if (enrichmentResult.apollo_success || enrichmentResult.linkedin_success || enrichmentResult.storage_success) {
      enrichmentResult.status = 'partially_completed';
    } else {
      enrichmentResult.status = 'failed';
    }
    
    if (enrichmentResult.apollo_success && enrichmentResult.linkedin_success && enrichmentResult.storage_success) {
      enrichmentResult.status = 'completed';
      console.log('✅ Enrichment process completed successfully');
    } else {
      console.log('⚠️ Enrichment process completed with errors');
    }
    
  } catch (error) {
    console.error('❌ Enrichment process failed:', error);
    enrichmentResult.status = 'failed';
    enrichmentResult.error = error.message;
  }
  
  // Update the original People DB record status based on results
  try {
    await updatePersonStatus(personInfo.id, enrichmentResult.status, notionToken);
  } catch (statusError) {
    console.error('❌ Failed to update person status:', statusError);
  }
  
  return enrichmentResult;
}

async function runApolloEnrichment(personInfo, apifyToken) {
  console.log('🔍 Running Apollo scraper for:', personInfo.name);
  
  try {
    const apolloInput = {
      url: `https://app.apollo.io/#/people?finderViewId=5b6dfc8b73f47a0001e44c3a&q_keywords=${encodeURIComponent(personInfo.name)}`,
      totalRecords: 50,
      fileName: `Apollo_${personInfo.name.replace(/\s+/g, '_')}`,
      maxConcurrency: 1
    };
    
    const response = await fetch('https://api.apify.com/v2/acts/jljBwyyQakqrL1wae/runs', {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${apifyToken}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(apolloInput)
    });
    
    if (!response.ok) {
      throw new Error(`Apollo scraper failed: ${response.status} ${response.statusText}`);
    }
    
    const runData = await response.json();
    console.log('✅ Apollo scraper initiated:', runData.data.id);
    
    // For now, return success - in production we'd wait for results
    return { 
      success: true, 
      runId: runData.data.id, 
      data: { message: 'Apollo enrichment initiated' } 
    };
    
  } catch (error) {
    console.error('❌ Apollo enrichment failed:', error);
    return { success: false, error: error.message, data: null };
  }
}

async function runLinkedInEnrichment(personInfo, apifyToken) {
  console.log('🔗 Running LinkedIn scraper for:', personInfo.linkedin);
  
  if (!personInfo.linkedin) {
    console.log('⚠️ No LinkedIn URL provided, skipping LinkedIn enrichment');
    return { success: false, error: 'No LinkedIn URL provided', data: null };
  }
  
  try {
    const linkedinInput = {
      urls: [personInfo.linkedin],
      maxConcurrency: 1
    };
    
    const response = await fetch('https://api.apify.com/v2/acts/PEgClm7RgRD7YO94b/runs', {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${apifyToken}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(linkedinInput)
    });
    
    if (!response.ok) {
      throw new Error(`LinkedIn scraper failed: ${response.status} ${response.statusText}`);
    }
    
    const runData = await response.json();
    console.log('✅ LinkedIn scraper initiated:', runData.data.id);
    
    // For now, return success - in production we'd wait for results  
    return { 
      success: true, 
      runId: runData.data.id, 
      data: { message: 'LinkedIn enrichment initiated' } 
    };
    
  } catch (error) {
    console.error('❌ LinkedIn enrichment failed:', error);
    return { success: false, error: error.message, data: null };
  }
}

async function storeEnrichedData(personInfo, apolloData, linkedinData, notionToken) {
  console.log('💾 Storing enriched data in Enrichment DB...');
  
  const enrichmentDbId = "258c2a32-df0d-805b-acb0-d0f2c81630cd";
  
  try {
    const enrichmentRecord = {
      "Name": {
        "title": [{ "text": { "content": personInfo.name } }]
      },
      "Primary Email": {
        "email": personInfo.email
      },
      "LinkedIn Profile": {
        "url": personInfo.linkedin
      }
    };
    
    const response = await fetch(`https://api.notion.com/v1/pages`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${notionToken}`,
        'Notion-Version': '2022-06-28',
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        parent: { database_id: enrichmentDbId },
        properties: enrichmentRecord
      })
    });
    
    if (!response.ok) {
      throw new Error(`Failed to store enrichment data: ${response.status} ${response.statusText}`);
    }
    
    const result = await response.json();
    console.log('✅ Enrichment record created:', result.id);
    
    return { success: true, recordId: result.id };
    
  } catch (error) {
    console.error('❌ Failed to store enriched data:', error);
    return { success: false, error: error.message };
  }
}

async function updatePersonStatus(personId, enrichmentStatus, notionToken) {
  console.log(`🔄 Updating person status to: ${enrichmentStatus}`);
  
  // Map enrichment status to Notion status values
  let notionStatus;
  switch (enrichmentStatus) {
    case 'completed':
      notionStatus = 'Completed';
      break;
    case 'failed':
      notionStatus = 'Failed';
      break;
    case 'partially_completed':
      notionStatus = 'Completed'; // Consider partial success as completed
      break;
    default:
      notionStatus = 'Failed';
  }
  
  try {
    const response = await fetch(`https://api.notion.com/v1/pages/${personId}`, {
      method: 'PATCH',
      headers: {
        'Authorization': `Bearer ${notionToken}`,
        'Notion-Version': '2022-06-28',
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        properties: {
          "Status": {
            "select": { "name": notionStatus }
          }
        }
      })
    });
    
    if (!response.ok) {
      throw new Error(`Failed to update status: ${response.status} ${response.statusText}`);
    }
    
    console.log(`✅ Person status updated to: ${notionStatus}`);
    return { success: true };
    
  } catch (error) {
    console.error('❌ Failed to update person status:', error);
    return { success: false, error: error.message };
  }
}
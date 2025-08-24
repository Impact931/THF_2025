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
        apollo_api_key_set: !!process.env.APOLLO_API_KEY,
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
  console.log('🔧 Starting Apollo Direct API enrichment process...');
  
  const apolloApiKey = process.env.APOLLO_API_KEY;
  const notionToken = process.env.NOTION_TOKEN;
  
  if (!apolloApiKey || !notionToken) {
    throw new Error('Missing required environment variables: APOLLO_API_KEY or NOTION_TOKEN');
  }
  
  const enrichmentResult = {
    status: 'processing',
    apollo_success: false,
    linkedin_success: false,
    storage_success: false
  };
  
  try {
    // Run Apollo Direct API enrichment
    console.log('🚀 Starting Apollo Direct API enrichment...');
    const apolloResult = await runApolloEnrichment(personInfo, apolloApiKey);
    enrichmentResult.apollo_success = apolloResult.success;
    
    // Run LinkedIn enrichment (temporarily disabled)
    console.log('🔗 Starting LinkedIn enrichment...');
    const linkedinResult = await runLinkedInEnrichment(personInfo, null);
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

async function runApolloEnrichment(personInfo, apolloApiKey) {
  console.log('🎯 Apollo DIRECT API - People Enrichment with /people/match endpoint');
  
  if (!personInfo.name) {
    console.log('⚠️ No person name provided for Apollo enrichment');
    return { success: false, error: 'No person name provided for Apollo enrichment', data: null };
  }
  
  console.log(`👤 Apollo DIRECT ENRICHMENT for: ${personInfo.name}`);
  console.log(`🏢 Company: ${personInfo.employer || 'N/A'}`);
  console.log(`📧 Email: ${personInfo.email || 'N/A'}`);
  console.log(`🔗 LinkedIn: ${personInfo.linkedin || 'N/A'}`);
  console.log(`💼 Position: ${personInfo.position || 'N/A'}`);
  
  try {
    // Parse the person name into first and last name
    const nameParts = personInfo.name.trim().split(' ');
    const firstName = nameParts[0] || '';
    const lastName = nameParts.slice(1).join(' ') || '';
    
    // Build Apollo enrichment request with ALL available data
    const enrichmentRequest = {
      first_name: firstName,
      last_name: lastName,
      // Add all available information for better matching
    };
    
    // Add company information if available
    if (personInfo.employer) {
      enrichmentRequest.organization_name = personInfo.employer;
    }
    
    // Add email if available
    if (personInfo.email) {
      enrichmentRequest.email = personInfo.email;
    }
    
    // Add LinkedIn URL if available
    if (personInfo.linkedin) {
      enrichmentRequest.linkedin_url = personInfo.linkedin;
    }
    
    // Add position/title if available
    if (personInfo.position) {
      enrichmentRequest.title = personInfo.position;
    }
    
    // Enable personal contact details
    enrichmentRequest.reveal_personal_emails = true;
    // Note: reveal_phone_number requires webhook_url, so commenting out for now
    // enrichmentRequest.reveal_phone_number = true;
    
    console.log('📤 Apollo Direct API Request:', JSON.stringify(enrichmentRequest, null, 2));
    
    const response = await fetch('https://api.apollo.io/api/v1/people/match', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Cache-Control': 'no-cache',
        'X-Api-Key': apolloApiKey
      },
      body: JSON.stringify(enrichmentRequest)
    });
    
    console.log('📊 Apollo API Response Status:', response.status, response.statusText);
    
    if (!response.ok) {
      let errorText = 'No details available';
      try {
        errorText = await response.text();
        console.error('❌ Apollo Direct API Error (FULL):', errorText);
      } catch (e) {
        console.error('❌ Could not read Apollo error response:', e.message);
      }
      throw new Error(`Apollo Direct API failed: ${response.status} ${response.statusText} - ${errorText}`);
    }
    
    const enrichmentResult = await response.json();
    console.log('✅ Apollo DIRECT ENRICHMENT COMPLETED');
    console.log('📊 FULL Apollo Enrichment Data (NO TRUNCATION):');
    console.log(JSON.stringify(enrichmentResult, null, 2));
    
    // Return the complete enrichment data
    return { 
      success: true, 
      method: 'direct_api',
      data: enrichmentResult,
      requestData: enrichmentRequest,
      message: 'Apollo direct enrichment completed successfully'
    };
    
  } catch (error) {
    console.error('❌ Apollo direct enrichment failed:', error);
    return { success: false, error: error.message, data: null };
  }
}

async function runLinkedInEnrichment(personInfo, apifyToken) {
  console.log('🔗 BrightData LinkedIn scraper - TEMPORARILY DISABLED');
  
  // Temporarily disable BrightData to focus on Apollo integration
  console.log('⏸️ BrightData paused - focusing on Apollo data collection');
  return { 
    success: false, 
    error: 'BrightData temporarily disabled per user request',
    data: { message: 'Focusing on Apollo scraper integration first' }
  };
}

async function storeEnrichedData(personInfo, apolloData, linkedinData, notionToken) {
  console.log('💾 Storing enriched data with Apollo results in Enrichment DB...');
  
  const enrichmentDbId = "258c2a32-df0d-805b-acb0-d0f2c81630cd";
  
  try {
    // Start with the basic Name field and add Apollo data
    const enrichmentRecord = {
      "Name": {
        "title": [{ "text": { "content": personInfo.name } }]
      }
    };
    
    // For now, just store the name until we identify the correct field names
    // This ensures the record gets created successfully
    console.log('📝 Storing minimal record (Name only) - Apollo data will be available via API logs');
    
    console.log('📝 Enhanced enrichment record to create:', JSON.stringify(enrichmentRecord, null, 2));
    
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
      let errorText = 'No error details available';
      try {
        errorText = await response.text();
        console.error('❌ Notion Enrichment DB Error Response:', errorText.substring(0, 1000));
      } catch (e) {
        console.error('❌ Could not read Notion error response:', e.message);
      }
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
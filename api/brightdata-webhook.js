// BrightData Webhook Receiver - Processes LinkedIn connection data
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
      message: '🔗 BrightData webhook receiver is working!',
      timestamp: new Date().toISOString(),
      purpose: 'Receives LinkedIn connection data from BrightData'
    });
  }

  // Handle BrightData webhook POST
  if (req.method === 'POST') {
    try {
      console.log('🔗 BrightData webhook received!');
      console.log('Headers:', JSON.stringify(req.headers, null, 2));
      console.log('Body:', JSON.stringify(req.body, null, 2));

      const brightDataResults = req.body;
      
      if (!brightDataResults) {
        console.log('❌ No BrightData results received');
        return res.status(400).json({
          error: 'No BrightData results provided',
          received_data: brightDataResults
        });
      }

      console.log('✅ BrightData results received:', brightDataResults.length || 'unknown count');
      
      // Process the LinkedIn connection data
      const processResult = await processBrightDataResults(brightDataResults);
      
      console.log('📤 BrightData processing completed:', processResult);
      
      return res.status(200).json({
        success: true,
        message: 'BrightData LinkedIn results processed successfully',
        timestamp: new Date().toISOString(),
        processed_count: processResult.processed_count,
        stored_count: processResult.stored_count,
        errors: processResult.errors || []
      });

    } catch (error) {
      console.error('❌ BrightData webhook processing error:', error);
      return res.status(500).json({
        error: error.message,
        type: error.name,
        message: 'BrightData webhook processing failed',
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

async function processBrightDataResults(brightDataResults) {
  console.log('🔄 Processing BrightData LinkedIn connection results...');
  
  const notionToken = process.env.NOTION_TOKEN;
  const enrichmentDbId = "258c2a32-df0d-805b-acb0-d0f2c81630cd";
  
  if (!notionToken) {
    throw new Error('Missing NOTION_TOKEN environment variable');
  }

  let processedCount = 0;
  let storedCount = 0;
  const errors = [];

  // Handle different possible data structures
  let connectionsData = [];
  
  if (Array.isArray(brightDataResults)) {
    connectionsData = brightDataResults;
  } else if (brightDataResults.data && Array.isArray(brightDataResults.data)) {
    connectionsData = brightDataResults.data;
  } else if (brightDataResults.results && Array.isArray(brightDataResults.results)) {
    connectionsData = brightDataResults.results;
  } else {
    console.log('⚠️ Unexpected BrightData result structure, treating as single record');
    connectionsData = [brightDataResults];
  }

  console.log(`📊 Processing ${connectionsData.length} LinkedIn connection records...`);

  for (const connection of connectionsData) {
    try {
      processedCount++;
      
      // Extract connection data
      const connectionInfo = extractConnectionData(connection);
      
      if (connectionInfo && connectionInfo.name) {
        // Store in Enrichment DB
        const storeResult = await storeConnectionInEnrichmentDB(connectionInfo, enrichmentDbId, notionToken);
        
        if (storeResult.success) {
          storedCount++;
          console.log(`✅ Stored connection: ${connectionInfo.name}`);
        } else {
          errors.push(`Failed to store ${connectionInfo.name}: ${storeResult.error}`);
        }
      } else {
        errors.push(`Could not extract valid data from connection record`);
      }
      
    } catch (error) {
      console.error('❌ Error processing connection:', error);
      errors.push(`Processing error: ${error.message}`);
    }
  }

  return {
    processed_count: processedCount,
    stored_count: storedCount,
    errors: errors.length > 0 ? errors : null
  };
}

function extractConnectionData(connection) {
  // Extract data based on the BrightData LinkedIn connection structure
  return {
    name: connection.name || `${connection.first_name || ''} ${connection.last_name || ''}`.trim(),
    linkedin_url: connection.url || connection.input_url,
    linkedin_id: connection.linkedin_id,
    position: connection.position || connection.current_position,
    company: connection.current_company_name || connection.company,
    location: connection.city || connection.location,
    country: connection.country_code || connection.country,
    followers: connection.followers,
    connections: connection.connections,
    about: connection.about,
    avatar: connection.avatar,
    experience: connection.experience,
    education: connection.education || connection.educations_details,
    skills: connection.skills,
    certifications: connection.certifications,
    languages: connection.languages,
    projects: connection.projects,
    publications: connection.publications,
    patents: connection.patents,
    volunteer_experience: connection.volunteer_experience,
    honors_and_awards: connection.honors_and_awards,
    recommendations: connection.recommendations,
    timestamp: new Date().toISOString()
  };
}

async function storeConnectionInEnrichmentDB(connectionInfo, enrichmentDbId, notionToken) {
  console.log(`💾 Storing LinkedIn connection: ${connectionInfo.name}`);
  
  try {
    // Create enrichment record with all available LinkedIn data
    const enrichmentRecord = {
      "Name": {
        "title": [{ "text": { "content": connectionInfo.name } }]
      }
    };
    
    // Add optional fields if available
    if (connectionInfo.company) {
      enrichmentRecord["Company"] = {
        "rich_text": [{ "text": { "content": connectionInfo.company } }]
      };
    }
    
    if (connectionInfo.position) {
      enrichmentRecord["Position"] = {
        "rich_text": [{ "text": { "content": connectionInfo.position } }]
      };
    }
    
    if (connectionInfo.location) {
      enrichmentRecord["Location"] = {
        "rich_text": [{ "text": { "content": connectionInfo.location } }]
      };
    }
    
    if (connectionInfo.linkedin_url) {
      enrichmentRecord["LinkedIn URL"] = {
        "url": connectionInfo.linkedin_url
      };
    }

    console.log('📝 Connection record to create:', JSON.stringify(enrichmentRecord, null, 2));
    
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
      throw new Error(`Failed to store connection data: ${response.status} ${response.statusText}`);
    }
    
    const result = await response.json();
    console.log(`✅ Connection record created: ${result.id}`);
    
    return { success: true, recordId: result.id };
    
  } catch (error) {
    console.error(`❌ Failed to store connection ${connectionInfo.name}:`, error);
    return { success: false, error: error.message };
  }
}
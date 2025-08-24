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

      const webhookData = req.body;
      
      // Extract person info from Notion webhook structure
      const personInfo = extractPersonFromNotionWebhook(webhookData);
      
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

function extractPersonFromNotionWebhook(webhookData) {
  console.log('🔍 Extracting person from Notion webhook...');
  
  if (!webhookData || !webhookData.data) {
    console.log('❌ No webhook data or data property');
    return null;
  }

  const pageData = webhookData.data;
  
  if (pageData.object !== 'page') {
    console.log('❌ Webhook data is not a page object');
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
  if (properties.Status && properties.Status.select) {
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
  
  const linkedin = properties['LinkedIn Profile']?.url || null;
  
  const employer = properties.Employer?.rich_text?.[0]?.plain_text || null;

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
    const linkedinResult = { success: false, data: null };
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
    
    if (enrichmentResult.apollo_success && enrichmentResult.storage_success) {
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

async function storeEnrichedData(personInfo, apolloData, linkedinData, notionToken) {
  console.log('💾 Storing comprehensive Apollo enrichment data in Enrichment DB...');
  
  const enrichmentDbId = "258c2a32-df0d-805b-acb0-d0f2c81630cd";
  
  try {
    // Step 1: Check for existing record with same name to prevent duplicates
    console.log('🔍 Checking for existing record with name:', personInfo.name);
    const existingRecord = await findExistingEnrichmentRecord(personInfo.name, enrichmentDbId, notionToken);
    
    if (existingRecord) {
      console.log('⚠️ Record already exists, updating instead of creating new one:', existingRecord.id);
      return await updateExistingEnrichmentRecord(existingRecord.id, personInfo, apolloData, linkedinData, notionToken);
    }
    
    // Step 2: Get Enrichment DB schema for intelligent field mapping
    console.log('📋 Getting Enrichment DB schema for field mapping...');
    const dbSchema = await getEnrichmentDBSchema(enrichmentDbId, notionToken);
    
    // Step 3: Build comprehensive enrichment record with all Apollo data
    const enrichmentRecord = {
      "Name": {
        "title": [{ "text": { "content": personInfo.name } }]
      }
    };
    
    // Step 4: Map all Apollo data to appropriate fields
    if (apolloData && apolloData.success && apolloData.data && apolloData.data.person) {
      const mappedFields = await mapApolloDataToEnrichmentFields(apolloData.data, dbSchema, notionToken);
      Object.assign(enrichmentRecord, mappedFields);
    }
    
    console.log('📝 Comprehensive enrichment record to create:', JSON.stringify(enrichmentRecord, null, 2));
    
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
    
    return { success: true, recordId: result.id, action: 'created' };
    
  } catch (error) {
    console.error('❌ Failed to store enriched data:', error);
    return { success: false, error: error.message };
  }
}

async function findExistingEnrichmentRecord(personName, enrichmentDbId, notionToken) {
  console.log('🔍 Searching for existing record with name:', personName);
  
  try {
    const response = await fetch(`https://api.notion.com/v1/databases/${enrichmentDbId}/query`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${notionToken}`,
        'Notion-Version': '2022-06-28',
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        filter: {
          property: 'Name',
          title: {
            equals: personName
          }
        }
      })
    });
    
    if (!response.ok) {
      console.error('❌ Failed to search for existing record:', response.status, response.statusText);
      return null;
    }
    
    const data = await response.json();
    
    if (data.results && data.results.length > 0) {
      console.log('✅ Found existing record:', data.results[0].id);
      return data.results[0];
    }
    
    console.log('✅ No existing record found, will create new one');
    return null;
    
  } catch (error) {
    console.error('❌ Error searching for existing record:', error);
    return null;
  }
}

async function updateExistingEnrichmentRecord(recordId, personInfo, apolloData, linkedinData, notionToken) {
  console.log('🔄 Updating existing enrichment record:', recordId);
  
  try {
    // Get current record to preserve existing data
    const currentRecord = await fetch(`https://api.notion.com/v1/pages/${recordId}`, {
      headers: {
        'Authorization': `Bearer ${notionToken}`,
        'Notion-Version': '2022-06-28'
      }
    });
    
    if (!currentRecord.ok) {
      throw new Error(`Failed to fetch existing record: ${currentRecord.status}`);
    }
    
    const current = await currentRecord.json();
    console.log('📄 Retrieved existing record for update');
    
    // Build update with new Apollo data while preserving existing fields
    const updateProperties = {};
    
    if (apolloData && apolloData.success && apolloData.data && apolloData.data.person) {
      // Get the database schema for intelligent mapping
      const dbSchema = await getEnrichmentDBSchema("258c2a32-df0d-805b-acb0-d0f2c81630cd", notionToken);
      const mappedFields = await mapApolloDataToEnrichmentFields(apolloData.data, dbSchema, notionToken);
      Object.assign(updateProperties, mappedFields);
    }
    
    console.log('📝 Updating existing record with new Apollo data:', JSON.stringify(updateProperties, null, 2));
    
    const response = await fetch(`https://api.notion.com/v1/pages/${recordId}`, {
      method: 'PATCH',
      headers: {
        'Authorization': `Bearer ${notionToken}`,
        'Notion-Version': '2022-06-28',
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        properties: updateProperties
      })
    });
    
    if (!response.ok) {
      throw new Error(`Failed to update enrichment record: ${response.status} ${response.statusText}`);
    }
    
    const result = await response.json();
    console.log('✅ Enrichment record updated:', result.id);
    
    return { success: true, recordId: result.id, action: 'updated' };
    
  } catch (error) {
    console.error('❌ Failed to update existing enrichment record:', error);
    return { success: false, error: error.message };
  }
}

async function getEnrichmentDBSchema(enrichmentDbId, notionToken) {
  console.log('📋 Fetching Enrichment DB schema...');
  
  try {
    const response = await fetch(`https://api.notion.com/v1/databases/${enrichmentDbId}`, {
      headers: {
        'Authorization': `Bearer ${notionToken}`,
        'Notion-Version': '2022-06-28'
      }
    });
    
    if (!response.ok) {
      throw new Error(`Failed to fetch database schema: ${response.status}`);
    }
    
    const database = await response.json();
    const properties = database.properties || {};
    
    console.log('✅ Retrieved database schema with', Object.keys(properties).length, 'fields');
    console.log('📋 Available fields:', Object.keys(properties).join(', '));
    
    return properties;
    
  } catch (error) {
    console.error('❌ Failed to fetch database schema:', error);
    // Return basic schema as fallback
    return {
      "Name": { "type": "title" },
      "Primary Email": { "type": "email" },
      "LinkedIn Profile": { "type": "url" },
      "Current Position": { "type": "rich_text" },
      "Current Company": { "type": "rich_text" },
      "Industry": { "type": "rich_text" },
      "Employment History": { "type": "rich_text" }
    };
  }
}

async function mapApolloDataToEnrichmentFields(apolloResponse, dbSchema, notionToken) {
  console.log('🤖 Mapping Apollo data to Enrichment DB fields...');
  
  const person = apolloResponse.person;
  const org = person.organization;
  const mappedFields = {};
  
  // First, create structured data output for logging and AI assessment
  const structuredData = {
    apollo_person_data: {
      id: person.id,
      name: person.name,
      first_name: person.first_name,
      last_name: person.last_name,
      email: person.email,
      linkedin_url: person.linkedin_url,
      title: person.title,
      headline: person.headline,
      photo_url: person.photo_url,
      city: person.city,
      state: person.state,
      country: person.country,
      time_zone: person.time_zone,
      departments: person.departments,
      seniority: person.seniority,
      employment_history: person.employment_history
    },
    apollo_organization_data: org ? {
      id: org.id,
      name: org.name,
      website_url: org.website_url,
      linkedin_url: org.linkedin_url,
      industry: org.industry,
      estimated_num_employees: org.estimated_num_employees,
      annual_revenue: org.annual_revenue,
      founded_year: org.founded_year,
      short_description: org.short_description,
      technology_names: org.technology_names,
      keywords: org.keywords,
      languages: org.languages,
      primary_phone: org.primary_phone?.number,
      city: org.city,
      state: org.state,
      country: org.country,
      address: org.raw_address
    } : null
  };
  
  console.log('📊 STRUCTURED APOLLO DATA:', JSON.stringify(structuredData, null, 2));
  
  // Direct mappings to actual database field names (from your 298-field schema)
  const apolloFieldMappings = {
    // Apollo-specific fields (exact matches from your schema)
    'Apollo Email': person.email,
    'Apollo LinkedIn URL': person.linkedin_url,
    'Apollo Title': person.title,
    'Apollo City': person.city,
    'Apollo State': person.state,
    'Apollo Country': person.country,
    'Apollo Department': person.departments ? person.departments.join(', ') : null,
    'Apollo Seniority': person.seniority,
    'Apollo Company': org?.name,
    'Apollo Industry': org?.industry,
    'Apollo Company Size': org?.estimated_num_employees,
    'Apollo Revenue Range': org?.annual_revenue,
    'Apollo Phone': org?.primary_phone?.number,
    'Apollo Raw Data': JSON.stringify(apolloResponse, null, 2),
    
    // LinkedIn-specific fields from Apollo data
    'LinkedIn Headline': person.headline,
    'LinkedIn Location': person.city && person.state ? `${person.city}, ${person.state}` : (person.city || person.state),
    'LinkedIn Current Company': org?.name,
    'LinkedIn Current Position': person.title,
    'LinkedIn Public Profile URL': person.linkedin_url,
    'LinkedIn Raw Data': JSON.stringify(person, null, 2),
    
    // Occupational fields
    'Occupational Current Job Title': person.title,
    'Occupational Current Employer': org?.name,
    'Occupational Raw Data': JSON.stringify({
      employment_history: person.employment_history,
      current_title: person.title,
      departments: person.departments,
      seniority: person.seniority
    }),
    
    // Demographic fields
    'Demographic Age': null, // Apollo doesn't provide age directly
    'Demographic Raw Data': JSON.stringify({
      location: `${person.city}, ${person.state}, ${person.country}`,
      time_zone: person.time_zone
    }),
    
    // Personal fields
    'Personal Email Addresses': person.email,
    'Personal Raw Data': JSON.stringify({
      photo_url: person.photo_url,
      personal_emails: person.personal_emails
    })
  };
  
  // Apply Apollo field mappings based on actual database schema
  for (const [fieldName, value] of Object.entries(apolloFieldMappings)) {
    if (value && dbSchema[fieldName]) {
      mappedFields[fieldName] = formatFieldValue(value, dbSchema[fieldName].type);
      console.log(`✅ Mapped ${fieldName}: ${typeof value === 'string' ? value.substring(0, 50) + '...' : value}`);
    }
  }
  
  // Handle employment history with detailed mapping
  if (person.employment_history && person.employment_history.length > 0) {
    // Current job from employment history
    const currentJob = person.employment_history.find(job => job.current === true);
    const previousJobs = person.employment_history.filter(job => job.current !== true);
    
    // LinkedIn Experience mapping
    if (dbSchema['LinkedIn Experience']) {
      const experienceData = person.employment_history.map(job => ({
        title: job.title,
        company: job.organization_name,
        start_date: job.start_date,
        end_date: job.end_date || (job.current ? 'Present' : null),
        current: job.current
      }));
      
      mappedFields['LinkedIn Experience'] = formatFieldValue(JSON.stringify(experienceData), dbSchema['LinkedIn Experience'].type);
      console.log('✅ Mapped LinkedIn Experience with employment history');
    }
    
    // LinkedIn Experience Count
    if (dbSchema['LinkedIn Experience Count']) {
      mappedFields['LinkedIn Experience Count'] = formatFieldValue(person.employment_history.length, dbSchema['LinkedIn Experience Count'].type);
      console.log('✅ Mapped LinkedIn Experience Count:', person.employment_history.length);
    }
    
    // Occupational Career Trajectory
    if (dbSchema['Occupational Career Trajectory']) {
      const trajectory = person.employment_history.map(job => 
        `${job.title} at ${job.organization_name} (${job.start_date || 'Unknown'} - ${job.end_date || 'Current'})`
      ).join(' → ');
      
      mappedFields['Occupational Career Trajectory'] = formatFieldValue(trajectory, dbSchema['Occupational Career Trajectory'].type);
      console.log('✅ Mapped Occupational Career Trajectory');
    }
  }
  
  // Technology and social media mappings
  const additionalMappings = {
    'Apollo Technographics': org?.technology_names ? org.technology_names.join(', ') : null,
    'Apollo Social Media Presence': person.linkedin_url ? 'LinkedIn' : null,
    'Apollo Twitter Handle': person.twitter_url,
    'Apollo Facebook Profile': person.facebook_url,
    'Apollo GitHub Profile': person.github_url,
    'Apollo Instagram Handle': null, // Apollo doesn't provide Instagram
    'Apollo Languages Spoken': org?.languages ? org.languages.join(', ') : null,
    'Apollo Military Service': person.employment_history?.some(job => 
      job.organization_name?.toLowerCase().includes('navy') || 
      job.organization_name?.toLowerCase().includes('army') ||
      job.organization_name?.toLowerCase().includes('military') ||
      job.title?.toLowerCase().includes('naval')
    ) ? 'Yes' : 'No',
    'Apollo Intent Data': person.intent_strength || null,
    'Apollo Phone Verified': person.phone ? 'Yes' : 'No',
    'Apollo Email Verified': person.email ? 'Yes' : 'No',
    'Apollo Phone Source': person.phone ? 'Apollo API' : null,
    'Apollo Email Source': person.email ? 'Apollo API' : null,
    'Last Apollo Sync': new Date().toISOString(),
    
    // Set sync statuses
    'Sync Status': 'Active',
    'Enrichment Status': 'Completed',
    'Data Sources': 'Apollo API',
    'Enrichment Date': new Date().toISOString(),
    'Last Updated': new Date().toISOString(),
    'Completeness Score': Object.keys(mappedFields).length, // Will update after all mapping
    'Data Confidence': 'High - Apollo Verified',
    'Next Review Date': new Date(Date.now() + 90 * 24 * 60 * 60 * 1000).toISOString() // 90 days from now
  };
  
  // Apply additional mappings
  for (const [fieldName, value] of Object.entries(additionalMappings)) {
    if (value && dbSchema[fieldName]) {
      mappedFields[fieldName] = formatFieldValue(value, dbSchema[fieldName].type);
      console.log(`✅ Mapped ${fieldName}: ${typeof value === 'string' ? value.substring(0, 50) + '...' : value}`);
    }
  }
  
  // Use OpenAI for complex data like keywords and remaining unmapped fields
  const unmappedData = [];
  
  // Organization keywords for AI mapping
  if (org?.keywords && org.keywords.length > 0) {
    unmappedData.push({
      data: org.keywords.join(', '),
      context: 'Company business keywords and focus areas - map to psychographic or relational fields'
    });
  }
  
  // Technology stack for AI mapping
  if (org?.technology_names && org.technology_names.length > 0) {
    unmappedData.push({
      data: org.technology_names.join(', '),
      context: 'Company technology stack and tools - map to occupational or psychographic fields'
    });
  }
  
  // Company description for AI mapping
  if (org?.short_description) {
    unmappedData.push({
      data: org.short_description,
      context: 'Detailed company description - extract insights for demographic, psychographic, or relational fields'
    });
  }
  
  if (unmappedData.length > 0) {
    console.log('🤖 Using OpenAI to intelligently map remaining complex data...');
    const aiMappedFields = await mapDataWithOpenAI(unmappedData, dbSchema, notionToken);
    Object.assign(mappedFields, aiMappedFields);
  }
  
  // Update completeness score
  if (dbSchema['Completeness Score']) {
    mappedFields['Completeness Score'] = formatFieldValue(Object.keys(mappedFields).length, dbSchema['Completeness Score'].type);
  }
  
  console.log('✅ Mapped', Object.keys(mappedFields).length, 'total fields from Apollo data');
  console.log('📋 Fields mapped:', Object.keys(mappedFields).join(', '));
  
  return mappedFields;
}

function formatFieldValue(value, fieldType) {
  if (!value) return null;
  
  switch (fieldType) {
    case 'title':
      return { "title": [{ "text": { "content": String(value) } }] };
    case 'rich_text':
      return { "rich_text": [{ "text": { "content": String(value) } }] };
    case 'email':
      return { "email": String(value) };
    case 'url':
      return { "url": String(value) };
    case 'phone_number':
      return { "phone_number": String(value) };
    case 'number':
      return { "number": typeof value === 'number' ? value : parseInt(value) || 0 };
    case 'select':
      return { "select": { "name": String(value) } };
    case 'multi_select':
      const options = String(value).split(',').map(opt => ({ "name": opt.trim() }));
      return { "multi_select": options };
    case 'date':
      return { "date": { "start": String(value) } };
    case 'checkbox':
      return { "checkbox": Boolean(value) };
    default:
      return { "rich_text": [{ "text": { "content": String(value) } }] };
  }
}

function extractUnmappedData(apolloResponse, alreadyMapped) {
  // Extract significant unmapped data for OpenAI processing
  const unmappedData = [];
  const person = apolloResponse.person;
  const org = person.organization;
  
  // Check if certain complex data wasn't mapped
  if (org?.keywords && org.keywords.length > 0 && !Object.keys(alreadyMapped).some(key => key.includes('keyword') || key.includes('tag'))) {
    unmappedData.push({
      data: org.keywords.join(', '),
      context: 'Company keywords and business focus areas'
    });
  }
  
  if (org?.languages && org.languages.length > 0 && !Object.keys(alreadyMapped).some(key => key.includes('language'))) {
    unmappedData.push({
      data: org.languages.join(', '),
      context: 'Languages used by the organization'
    });
  }
  
  if (person.intent_strength && !Object.keys(alreadyMapped).some(key => key.includes('intent'))) {
    unmappedData.push({
      data: `Intent strength: ${person.intent_strength}`,
      context: 'Person\'s intent or buying signal strength'
    });
  }
  
  return unmappedData;
}

async function mapDataWithOpenAI(unmappedData, dbSchema, notionToken) {
  console.log('🤖 Using OpenAI to intelligently map unmapped data...');
  
  const openaiApiKey = process.env.OPENAI_API_KEY;
  if (!openaiApiKey) {
    console.log('⚠️ OpenAI API key not available, skipping intelligent mapping');
    return {};
  }
  
  try {
    const availableFields = Object.keys(dbSchema).filter(field => field !== 'Name');
    const unmappedDataString = unmappedData.map(item => `${item.context}: ${item.data}`).join('\n');
    
    const prompt = `You are a data mapping expert. I have professional enrichment data that needs to be mapped to database fields.

UNMAPPED DATA:
${unmappedDataString}

AVAILABLE DATABASE FIELDS:
${availableFields.join(', ')}

Please map the unmapped data to the most appropriate database fields. Consider:
- Semantic meaning and context
- Field names that suggest similar content
- Professional/business relevance
- Data types and format compatibility

Respond with ONLY a JSON object mapping field names to values. No explanations.
Example: {"Keywords": "value1, value2", "Languages": "English, Spanish"}`;

    const response = await fetch('https://api.openai.com/v1/chat/completions', {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${openaiApiKey}`,
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({
        model: 'gpt-4o-mini',
        messages: [{ role: 'user', content: prompt }],
        temperature: 0.1,
        max_tokens: 500
      })
    });
    
    if (!response.ok) {
      throw new Error(`OpenAI API failed: ${response.status}`);
    }
    
    const aiResponse = await response.json();
    const mapping = JSON.parse(aiResponse.choices[0].message.content);
    
    // Format the AI-mapped fields according to database schema
    const formattedMapping = {};
    for (const [fieldName, value] of Object.entries(mapping)) {
      if (dbSchema[fieldName] && value) {
        formattedMapping[fieldName] = formatFieldValue(value, dbSchema[fieldName].type);
      }
    }
    
    console.log('🤖 OpenAI mapped', Object.keys(formattedMapping).length, 'additional fields');
    return formattedMapping;
    
  } catch (error) {
    console.error('❌ OpenAI mapping failed:', error);
    return {};
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
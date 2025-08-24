#!/usr/bin/env node
/**
 * Direct test of Apollo People Enrichment API
 */

async function testApolloDirectAPI() {
  console.log('🎯 Testing Apollo Direct API /people/match...');
  
  const apolloApiKey = 'cV2MsNiGeeVjATS-_gXvqg';
  
  // Test request matching the person data
  const enrichmentRequest = {
    first_name: 'Matt',
    last_name: 'Stevens',
    organization_name: 'The Honor Foundation',
    email: 'matt@honor.org',
    linkedin_url: 'https://www.linkedin.com/in/matthewpstevens/',
    title: 'CEO',
    reveal_personal_emails: true
    // reveal_phone_number: true  // Requires webhook_url
  };
  
  console.log('📤 Apollo API Request:', JSON.stringify(enrichmentRequest, null, 2));
  
  try {
    const response = await fetch('https://api.apollo.io/api/v1/people/match', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Cache-Control': 'no-cache',
        'X-Api-Key': apolloApiKey
      },
      body: JSON.stringify(enrichmentRequest)
    });
    
    console.log('📊 Apollo Response Status:', response.status, response.statusText);
    console.log('📊 Apollo Response Headers:', Object.fromEntries(response.headers));
    
    const responseText = await response.text();
    console.log('📊 Apollo Raw Response:', responseText);
    
    if (response.ok) {
      try {
        const jsonResult = JSON.parse(responseText);
        console.log('✅ Apollo SUCCESS! JSON Result:');
        console.log(JSON.stringify(jsonResult, null, 2));
      } catch (e) {
        console.log('⚠️ Response is not JSON format');
      }
    } else {
      console.error('❌ Apollo API Error');
    }
    
  } catch (error) {
    console.error('💥 Network Error:', error.message);
  }
}

// Run the test
testApolloDirectAPI().then(() => {
  console.log('🏁 Apollo Direct API test complete');
}).catch(error => {
  console.error('💥 Test failed:', error);
});
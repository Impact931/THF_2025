#!/usr/bin/env node
/**
 * Test corrected BrightData LinkedIn scraper with proper format
 */

async function testCorrectedBrightData() {
  console.log('🔗 Testing corrected BrightData LinkedIn scraper...');
  
  // Extract first and last name 
  const fullName = 'Matt Stevens';
  const nameParts = fullName.trim().split(' ');
  const firstName = nameParts[0] || '';
  const lastName = nameParts.slice(1).join(' ') || '';
  
  console.log(`👤 Searching for: ${firstName} ${lastName}`);
  
  // BrightData API configuration from screenshot
  const apiKey = 'f709421f8b3de28198171232a3795a144a2d21b3d77a4a898cc012';
  
  // Correct input format
  const linkedinInput = {
    first_name: firstName,
    last_name: lastName
  };
  
  console.log('📤 Input:', JSON.stringify(linkedinInput, null, 2));
  
  // Try the data collection trigger API
  const apiUrl = `https://api.brightdata.com/datacollector/trigger?collector_name=gd_l1viktl72bvl7bjuj0&format=json`;
  
  try {
    console.log('🌐 API URL:', apiUrl);
    
    const response = await fetch(apiUrl, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${apiKey}`,
        'Content-Type': 'application/json',
        'User-Agent': 'THF-Enrichment-Test/1.0'
      },
      body: JSON.stringify([linkedinInput]) // Array format
    });
    
    console.log('📊 Response Status:', response.status, response.statusText);
    console.log('📊 Response Headers:', Object.fromEntries(response.headers));
    
    const responseText = await response.text();
    console.log('📊 Response Body (first 1000 chars):', responseText.substring(0, 1000));
    
    if (response.ok) {
      try {
        const jsonResult = JSON.parse(responseText);
        console.log('✅ JSON Result:', JSON.stringify(jsonResult, null, 2));
      } catch (e) {
        console.log('⚠️ Response is not JSON');
      }
    } else {
      console.error('❌ Request failed');
    }
    
  } catch (error) {
    console.error('💥 Error:', error.message);
    console.error('📍 Stack:', error.stack);
  }
}

// Run the test
testCorrectedBrightData().then(() => {
  console.log('🏁 Test complete');
}).catch(error => {
  console.error('💥 Test failed:', error);
});
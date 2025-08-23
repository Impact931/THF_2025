#!/usr/bin/env node
/**
 * Direct test of BrightData LinkedIn scraper
 */

const https = require('https');
const http = require('http');

async function testBrightDataDirect() {
  console.log('🔗 Testing BrightData LinkedIn scraper directly...');
  
  const brightDataToken = 'b20a2b3f-af9b-4d32-8bfb-aaac9cb701b1';
  const linkedinInput = {
    url: 'https://www.linkedin.com/in/matthewpstevens/',
    country: "US"
  };
  
  console.log('📤 Input:', JSON.stringify(linkedinInput, null, 2));
  
  const apiUrl = `https://brightdata.com/cp/scrapers/api/gd_l1viktl72bvl7bjuj0/name/management_api?id=hl_272ba236&token=${brightDataToken}`;
  
  try {
    const response = await fetch(apiUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'User-Agent': 'THF-Enrichment-Webhook/1.0'
      },
      body: JSON.stringify(linkedinInput)
    });
    
    console.log('📊 Response Status:', response.status, response.statusText);
    console.log('📊 Response Headers:', Object.fromEntries(response.headers));
    
    const responseText = await response.text();
    console.log('📊 Response Body:', responseText);
    
    if (response.ok) {
      try {
        const jsonResult = JSON.parse(responseText);
        console.log('✅ JSON Result:', JSON.stringify(jsonResult, null, 2));
      } catch (e) {
        console.log('⚠️ Response is not JSON, raw text:', responseText.substring(0, 500));
      }
    }
    
  } catch (error) {
    console.error('❌ Error:', error.message);
    console.error('❌ Stack:', error.stack);
  }
}

// Run the test
testBrightDataDirect().then(() => {
  console.log('🏁 Test complete');
}).catch(error => {
  console.error('💥 Test failed:', error);
});
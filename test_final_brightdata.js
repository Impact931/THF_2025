#!/usr/bin/env node
/**
 * Test final BrightData implementation with correct datasets v3 API
 */

async function testFinalBrightData() {
  console.log('🔗 Testing final BrightData LinkedIn datasets v3 API...');
  
  // Test with Matt Stevens
  const firstName = 'Matt';
  const lastName = 'Stevens';
  
  console.log(`👤 Searching LinkedIn for: ${firstName} ${lastName}`);
  
  // Complete API configuration from user
  const apiKey = 'f709421f8b3de28198171232a3795a144a2d21b3d77a4a898cc012bf7267e5f6';
  const datasetId = 'gd_l1viktl72bvl7bjuj0';
  
  // BrightData datasets v3 API endpoint
  const apiUrl = 'https://api.brightdata.com/datasets/v3/trigger';
  const params = new URLSearchParams({
    dataset_id: datasetId,
    include_errors: 'true',
    type: 'discover_new',
    discover_by: 'name'
  });
  
  // Input data - first_name and last_name as separate fields
  const linkedinInput = [{
    first_name: firstName,
    last_name: lastName
  }];
  
  console.log('📤 Input Data:', JSON.stringify(linkedinInput, null, 2));
  console.log('🌐 Full URL:', `${apiUrl}?${params.toString()}`);
  
  try {
    const response = await fetch(`${apiUrl}?${params.toString()}`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${apiKey}`,
        'Content-Type': 'application/json',
        'User-Agent': 'THF-Enrichment-Test/1.0'
      },
      body: JSON.stringify(linkedinInput)
    });
    
    console.log('📊 Response Status:', response.status, response.statusText);
    console.log('📊 Response Headers:', Object.fromEntries(response.headers));
    
    const responseText = await response.text();
    console.log('📊 Response Body:', responseText.substring(0, 1000));
    
    if (response.ok) {
      try {
        const jsonResult = JSON.parse(responseText);
        console.log('✅ SUCCESS! JSON Result:', JSON.stringify(jsonResult, null, 2));
      } catch (e) {
        console.log('✅ Response received but not JSON format');
      }
    } else {
      console.error('❌ Request failed');
    }
    
  } catch (error) {
    console.error('💥 Error:', error.message);
  }
}

// Run the test
testFinalBrightData().then(() => {
  console.log('🏁 Test complete');
}).catch(error => {
  console.error('💥 Test failed:', error);
});
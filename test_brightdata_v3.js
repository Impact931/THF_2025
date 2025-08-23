#!/usr/bin/env node
/**
 * Test BrightData API using different endpoint approaches
 */

async function testBrightDataEndpoints() {
  console.log('🔗 Testing various BrightData API endpoints...');
  
  const firstName = 'Matt';
  const lastName = 'Stevens';
  const apiKey = 'f709421f8b3de28198171232a3795a144a2d21b3d77a4a898cc012';
  const collectorId = 'gd_l1viktl72bvl7bjuj0';
  const snapshotId = 'Jynh132v19n82v81kx';
  
  const inputData = {
    first_name: firstName,
    last_name: lastName
  };
  
  console.log('📤 Input Data:', JSON.stringify(inputData, null, 2));
  
  // Different endpoint variations to try
  const endpoints = [
    `https://api.brightdata.com/dca/trigger?collector=${collectorId}&format=json`,
    `https://api.brightdata.com/dca/dataset_create?id=${collectorId}`,
    `https://api.brightdata.com/dca/${collectorId}/create_snapshot`,
    `https://api.brightdata.com/datacollector/${collectorId}/run`,
    `https://api.brightdata.com/dca/trigger/${collectorId}?format=json`
  ];
  
  for (let i = 0; i < endpoints.length; i++) {
    const apiUrl = endpoints[i];
    console.log(`\\n🌐 Test ${i + 1}: ${apiUrl}`);
    
    try {
      const response = await fetch(apiUrl, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${apiKey}`,
          'Content-Type': 'application/json',
          'User-Agent': 'THF-Enrichment-Test/1.0'
        },
        body: JSON.stringify([inputData])
      });
      
      console.log(`📊 Status: ${response.status} ${response.statusText}`);
      
      const responseText = await response.text();
      console.log(`📊 Response: ${responseText.substring(0, 200)}${responseText.length > 200 ? '...' : ''}`);
      
      if (response.ok) {
        console.log('✅ This endpoint worked!');
        break;
      }
      
    } catch (error) {
      console.log(`❌ Error: ${error.message}`);
    }
    
    // Small delay between requests
    await new Promise(resolve => setTimeout(resolve, 1000));
  }
  
  // Also try the original management API approach with proper auth
  console.log('\\n🔧 Testing original management API with Bearer auth...');
  const mgmtUrl = 'https://brightdata.com/cp/scrapers/api/gd_l1viktl72bvl7bjuj0/name/management_api?id=hl_272ba236';
  
  try {
    const response = await fetch(mgmtUrl, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${apiKey}`,
        'Content-Type': 'application/json',
        'User-Agent': 'THF-Enrichment-Test/1.0'
      },
      body: JSON.stringify(inputData)
    });
    
    console.log(`📊 Mgmt API Status: ${response.status} ${response.statusText}`);
    const responseText = await response.text();
    console.log(`📊 Mgmt API Response: ${responseText.substring(0, 300)}`);
    
  } catch (error) {
    console.log(`❌ Mgmt API Error: ${error.message}`);
  }
}

// Run the test
testBrightDataEndpoints().then(() => {
  console.log('\\n🏁 All tests complete');
}).catch(error => {
  console.error('💥 Tests failed:', error);
});
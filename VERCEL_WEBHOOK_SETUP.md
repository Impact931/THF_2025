# Vercel Webhook Setup Guide for THF Enrichment

## 🎯 Overview

This webhook system triggers comprehensive data enrichment when a person's status in your People DB changes to "Working". The enrichment runs Apollo and LinkedIn scrapers, then stores results in your Enrichment DB.

---

## 🗄️ **Database Configuration Updated**

✅ **People DB ID**: `258c2a32-df0d-80f3-944f-cf819718d96a`
✅ **Enrichment DB ID**: `258c2a32-df0d-805b-acb0-d0f2c81630cd` 
✅ **Notion Token**: Configured
✅ **Database Config**: Saved to `database_config.json`

---

## 🚀 **Vercel Deployment Steps**

### 1. **Test Locally First**
```bash
python3 test_webhook_local.py
```
This validates the webhook processor with Matt Stevens' data.

### 2. **Deploy to Vercel**

**A. Install Vercel CLI:**
```bash
npm install -g vercel
```

**B. Login to Vercel:**
```bash
vercel login
```

**C. Deploy from project directory:**
```bash
cd /Users/iso_mac2/Documents/GitHub/THF_2025
vercel --prod
```

### 3. **Set Environment Variables in Vercel**

In your Vercel dashboard → Project Settings → Environment Variables:

```
APIFY_TOKEN = your_apify_api_token_placeholder
NOTION_TOKEN = your_notion_api_token_placeholder
```

### 4. **Get Your Webhook URL**

After deployment, Vercel will provide a URL like:
```
https://your-project.vercel.app/api/webhook-enrichment
```

---

## 📡 **Notion Webhook Setup**

### 1. **Create Notion Integration Webhook**

Go to: https://www.notion.so/my-integrations

1. Select your THF integration
2. Go to "Webhook endpoints" 
3. Click "Add webhook endpoint"
4. **URL**: `https://your-project.vercel.app/api/webhook-enrichment`
5. **Events**: Select "Page property edited"
6. **Database**: Select your People DB
7. **Property filter**: Status field only

### 2. **Alternative: Notion Automation**

If direct webhooks aren't available:

1. **In your People DB**, create a Notion automation:
2. **Trigger**: "When Status changes to Working"
3. **Action**: "Send HTTP request" 
4. **URL**: Your Vercel webhook URL
5. **Method**: POST
6. **Body**: Include page data

---

## 🔄 **How It Works**

### **Trigger Flow:**
```
People DB Status → "Working" 
       ↓
Notion Webhook Fires
       ↓
Vercel Processes Request
       ↓
Apollo + LinkedIn Scrapers Run
       ↓
Data Stored in Enrichment DB
       ↓
People DB Status Updated to "Completed"/"Failed"
```

### **Webhook Processing:**
1. **Validates** incoming webhook payload
2. **Extracts** person information (name, email, LinkedIn, etc.)
3. **Runs Apollo** enrichment (contact data, company intel)
4. **Runs LinkedIn** enrichment (profile, connections, experience)
5. **Stores** comprehensive data in Enrichment DB
6. **Updates** People DB status based on results

---

## 🧪 **Testing the Complete Flow**

### 1. **Test with Matt Stevens:**

1. Go to your People DB in Notion
2. Find Matt Stevens record
3. Change his **Status** to "Working"
4. Watch the webhook trigger enrichment
5. Check Enrichment DB for new comprehensive record
6. Verify People DB status updates to "Completed"

### 2. **Expected Results:**

**Apollo Data Retrieved:**
- Verified email and phone
- Company intelligence 
- Intent data and technographics
- External mentions and social presence

**LinkedIn Data Retrieved:**
- Complete profile information
- 1st-degree connections data
- Experience and education history
- Skills and activity metrics

**Storage:**
- New record in Enrichment DB with 200+ fields populated
- People DB status automatically updated
- Error logging if issues occur

---

## 🔧 **Configuration Files Created**

- `vercel.json` - Vercel deployment configuration
- `api/webhook-enrichment.py` - Main webhook processor
- `requirements.txt` - Python dependencies
- `database_config.json` - Database IDs and tokens
- `test_webhook_local.py` - Local testing script

---

## 📊 **Monitoring & Troubleshooting**

### **Check Webhook Logs:**
```bash
vercel logs --follow
```

### **Test Webhook Manually:**
```bash
curl -X POST https://your-project.vercel.app/api/webhook-enrichment \
  -H "Content-Type: application/json" \
  -d @test_payload.json
```

### **Common Issues:**
- **401 Unauthorized**: Check environment variables in Vercel
- **404 Not Found**: Verify webhook URL is correct
- **Timeout**: Apollo/LinkedIn scrapers may take 2-3 minutes
- **Partial Results**: One scraper worked, other failed (still saves data)

---

## 🎯 **Next Steps**

1. **Test locally**: `python3 test_webhook_local.py`
2. **Deploy to Vercel**: `vercel --prod`
3. **Set environment variables** in Vercel dashboard
4. **Configure Notion webhook** with your Vercel URL
5. **Test with Matt Stevens** by changing his status to "Working"
6. **Verify comprehensive enrichment** appears in Enrichment DB

**Once working, you'll have automatic enrichment triggered by simple status changes in Notion!**

---

*The webhook processes comprehensive data enrichment including Apollo external intelligence and LinkedIn network analysis, storing 200+ data points per person for future AI analytics.*
# THF_2025 - Professional Network Intelligence Tool

A comprehensive data enrichment system for The Honor Foundation's professional network database, integrating Notion with Apollo and LinkedIn scrapers via Apify.

## 🎖️ Overview

This tool enhances your THF People Database by automatically enriching contact records with verified professional information from Apollo and LinkedIn, providing deeper insights into your professional network.

## 📋 Features

- **Notion Integration**: Direct API access to your THF People Database
- **Apollo Enrichment**: Professional contact information, verified emails, phone numbers
- **LinkedIn Enrichment**: Profile details, experience, education, skills, connections
- **Intelligent Analytics**: Network analysis, industry insights, geographic distribution
- **Data Quality Scoring**: Automated assessment of contact data completeness

## 🚀 Quick Start

### Prerequisites

1. **Apify Account & API Token**
   - Sign up at [Apify Console](https://console.apify.com)
   - Generate API token from Account → Integrations
   - Ensure access to actors: `jljBwyyQakqrL1wae` (Apollo) and `PEgClm7RgRD7YO94b` (LinkedIn)

2. **Notion Database Setup**
   - Add new fields to your People DB (see `NOTION_DATABASE_FIELDS.md`)
   - Ensure integration has read/write access to the database

### Installation

1. **Test Current Setup**
   ```bash
   python3 test_enrichment.py
   ```

2. **Set Apify Token**
   ```bash
   export APIFY_TOKEN="your_token_here"
   ```

3. **Run Full Enrichment**
   ```bash
   python3 apify_enrichment.py
   ```

## 📊 Current Network Overview

Based on your People DB analysis:

- **Total Contacts**: 1 (Matt Stevens)
- **Data Quality**: 100% completeness
- **Military Background**: 100% (Navy veterans)
- **LinkedIn Coverage**: 100%
- **Geographic Focus**: California
- **Industry Focus**: Non-Profit/Veteran Services

## 🔧 System Components

### Core Files

- `thf_intelligence.py` - Network analysis and Notion API client
- `apify_enrichment.py` - Main enrichment service with Apollo/LinkedIn integration
- `test_enrichment.py` - Connection testing and validation
- `notion_client.py` - Basic Notion database operations
- `find_databases.py` - Database discovery utility

### Configuration Files

- `NOTION_DATABASE_FIELDS.md` - Required database fields for enrichment
- `.gitignore` - Git ignore patterns for sensitive data

## 🔍 Enrichment Process

### 1. Data Collection
- Extracts existing contact information from Notion
- Identifies enrichable records (has email, LinkedIn, or company data)

### 2. Apollo Enrichment
- Searches Apollo.io for verified contact information
- Retrieves professional emails, phone numbers, job details
- Maps results to `apollo_*` fields in Notion

### 3. LinkedIn Enrichment
- Scrapes LinkedIn profiles for detailed professional information
- Extracts experience, education, skills, connections
- Maps results to `linkedin_*` fields in Notion

### 4. Data Validation & Storage
- Validates enriched data quality
- Updates Notion records with new information
- Tracks enrichment status and confidence levels

## 📈 Analytics Features

### Network Intelligence
- Professional distribution analysis
- Industry and employer insights
- Geographic concentration mapping
- Military service analysis

### Contact Coverage
- Email/phone availability tracking
- LinkedIn connectivity assessment
- Social media presence analysis

### Data Quality Metrics
- Completeness scoring
- Verification status tracking
- Confidence level assessment

## 🎯 Next Steps

### Phase 1: Database Setup ✅
- [x] Notion API integration
- [x] Basic analytics framework
- [x] Apollo/LinkedIn scraper setup
- [x] Field mapping documentation

### Phase 2: Enrichment Testing
- [ ] Add required fields to Notion database
- [ ] Test Apollo scraper with sample data
- [ ] Test LinkedIn scraper with sample data
- [ ] Validate data quality and accuracy

### Phase 3: Automation
- [ ] Set up Notion automation triggers
- [ ] Create batch processing for multiple records
- [ ] Implement error handling and retry logic
- [ ] Add email notifications for completion

### Phase 4: Advanced Features
- [ ] Networking recommendation engine
- [ ] Industry trend analysis
- [ ] Contact prioritization scoring
- [ ] Integration with CRM systems

## ⚙️ Configuration

### Environment Variables
```bash
APIFY_TOKEN="your_apify_token"
NOTION_TOKEN="your_notion_token_here"
PEOPLE_DB_ID="258c2a32-df0d-80f3-944f-cf819718d96a"
```

### Required Notion Fields

See `NOTION_DATABASE_FIELDS.md` for complete list of fields to add:

**Apollo Fields**: `apollo_email`, `apollo_phone`, `apollo_company`, etc.
**LinkedIn Fields**: `linkedin_headline`, `linkedin_experience`, `linkedin_skills`, etc.
**Metadata Fields**: `enrichment_status`, `last_enriched`, `data_confidence`, etc.

## 🔒 Security & Privacy

- Store API tokens as environment variables
- Respect LinkedIn Terms of Service
- Follow data privacy regulations (GDPR compliance)
- Implement rate limiting to avoid API blocks
- Verify data accuracy before use

## 📞 Support

For issues or questions:
1. Check test results: `python3 test_enrichment.py`
2. Review logs for error messages
3. Verify API token permissions
4. Confirm Notion database field setup

## 🏗️ Architecture

```
THF People DB (Notion)
     ↓
THF Intelligence Tool
     ↓
Apify Integration Layer
     ↓
Apollo Scraper ←→ LinkedIn Scraper
     ↓
Enriched Data → Back to Notion
```

---

*Built for The Honor Foundation - Supporting veteran professional networks through intelligent data enrichment.*
# THF People DB - Required New Fields for Data Enrichment

## Overview
To support data enrichment from Apollo and LinkedIn scrapers, the following new fields need to be added to your Notion People DB. These fields will capture enriched data that supplements your existing contact information.

## Apollo Scraper Fields

### Contact Information (Apollo)
| Field Name | Type | Description |
|------------|------|-------------|
| `apollo_email` | Email | Work email found by Apollo |
| `apollo_personal_email` | Email | Personal email found by Apollo |
| `apollo_phone` | Phone Number | Primary phone number from Apollo |
| `apollo_mobile` | Phone Number | Mobile phone number from Apollo |

### Professional Information (Apollo)
| Field Name | Type | Description |
|------------|------|-------------|
| `apollo_title` | Rich Text | Job title from Apollo |
| `apollo_company` | Rich Text | Company name from Apollo |
| `apollo_linkedin` | URL | LinkedIn profile URL from Apollo |

### Location Information (Apollo)
| Field Name | Type | Description |
|------------|------|-------------|
| `apollo_city` | Rich Text | City from Apollo |
| `apollo_state` | Rich Text | State from Apollo |
| `apollo_country` | Rich Text | Country from Apollo |

## LinkedIn Scraper Fields

### Profile Information (LinkedIn)
| Field Name | Type | Description |
|------------|------|-------------|
| `linkedin_headline` | Rich Text | LinkedIn professional headline |
| `linkedin_summary` | Rich Text | LinkedIn profile summary/about section |
| `linkedin_location` | Rich Text | Location from LinkedIn profile |

### Professional Experience (LinkedIn)
| Field Name | Type | Description |
|------------|------|-------------|
| `linkedin_experience` | Rich Text | JSON string of work experience (top 3) |
| `linkedin_education` | Rich Text | JSON string of education history |
| `linkedin_skills` | Rich Text | Comma-separated list of top skills |

### Contact & Metrics (LinkedIn)
| Field Name | Type | Description |
|------------|------|-------------|
| `linkedin_email` | Email | Email found on LinkedIn profile |
| `linkedin_connections` | Number | Number of LinkedIn connections |
| `linkedin_followers` | Number | Number of LinkedIn followers |

## Enrichment Metadata Fields

### Process Tracking
| Field Name | Type | Description |
|------------|------|-------------|
| `enrichment_status` | Select | Options: "Not Started", "In Progress", "Completed", "Failed" |
| `last_enriched` | Date & Time | When enrichment was last performed |
| `enrichment_score` | Number | Data completeness score (0-100) |
| `enrichment_notes` | Rich Text | Notes about enrichment process or issues |

### Data Quality
| Field Name | Type | Description |
|------------|------|-------------|
| `data_confidence` | Select | Options: "High", "Medium", "Low" - confidence in enriched data |
| `verified_email` | Checkbox | Whether email has been verified |
| `verified_phone` | Checkbox | Whether phone has been verified |

## Implementation Steps

### Step 1: Add Fields to Notion Database
1. Open your THF_2025 Notion workspace
2. Navigate to the "People DB" database
3. Add each field listed above with the specified type
4. Set up the Select field options as indicated

### Step 2: Configure Field Properties

#### For Select Fields:
**enrichment_status** options:
- Not Started (Gray)
- In Progress (Yellow)
- Completed (Green)  
- Failed (Red)

**data_confidence** options:
- High (Green)
- Medium (Yellow)
- Low (Red)

#### For Text Fields:
- Set character limits as needed
- Enable rich text formatting where specified

### Step 3: Set Default Values
- `enrichment_status` → "Not Started"
- `last_enriched` → Leave empty
- `enrichment_score` → 0
- All enriched data fields → Leave empty initially

## Field Usage Guidelines

### Apollo Fields
- Use Apollo fields for verified contact information
- Apollo data typically has high accuracy for business contacts
- Prefer Apollo emails for professional outreach

### LinkedIn Fields
- Use LinkedIn fields for professional context and networking
- LinkedIn skills help identify expertise areas
- Connection/follower counts indicate influence level

### Quality Assurance
- Always check `data_confidence` before using enriched data
- Use `enrichment_notes` to document any data quality issues
- Verify important contact information before outreach

## API Integration Notes

### Field Mapping in Code
The enrichment service will map API responses to these fields:
- Apollo API responses → `apollo_*` fields
- LinkedIn API responses → `linkedin_*` fields
- Metadata updates → enrichment tracking fields

### Error Handling
- Failed enrichments will be logged in `enrichment_notes`
- `enrichment_status` will be set to "Failed" with error details
- Partial enrichments are allowed (some fields may be empty)

### Rate Limiting
- Apollo: Limited by credits (1 credit per contact enriched)
- LinkedIn: Limited by API rate limits
- Batch processing recommended for large datasets

## Next Steps

1. **Add Fields**: Manually add all fields listed above to your Notion People DB
2. **Test Connection**: Run the enrichment service with your Apify token
3. **Verify Data**: Check that enriched data appears correctly in Notion
4. **Set up Triggers**: Configure Notion automation to trigger enrichment
5. **Monitor Quality**: Regular review of enriched data quality

## Security Considerations

- Store Apify API tokens securely (use environment variables)
- Be mindful of LinkedIn's terms of service
- Respect rate limits to avoid API blocks
- Consider data privacy regulations (GDPR, etc.)

---

*This document should be updated as new enrichment sources are added or field requirements change.*
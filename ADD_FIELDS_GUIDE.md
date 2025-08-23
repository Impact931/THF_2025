# Step-by-Step Guide: Adding Enrichment Fields to Notion

## 🎯 Overview
You need to add 26 new fields to your THF People DB before running the enrichment. This guide provides the exact steps and field configurations.

## 📋 Quick Setup Instructions

### 1. Open Your Notion Database
1. Go to your THF_2025 Notion workspace
2. Open the "People DB" database
3. Click the "Add a property" button (+ icon in the header row)

### 2. Add Apollo Fields (10 fields)

#### Contact Fields
| Field Name | Type | Notes |
|------------|------|-------|
| `apollo_email` | Email | Work email from Apollo |
| `apollo_personal_email` | Email | Personal email from Apollo |
| `apollo_phone` | Phone | Primary phone from Apollo |
| `apollo_mobile` | Phone | Mobile phone from Apollo |

#### Professional Fields  
| Field Name | Type | Notes |
|------------|------|-------|
| `apollo_title` | Text | Job title from Apollo |
| `apollo_company` | Text | Company name from Apollo |
| `apollo_linkedin` | URL | LinkedIn URL from Apollo |

#### Location Fields
| Field Name | Type | Notes |
|------------|------|-------|
| `apollo_city` | Text | City from Apollo |
| `apollo_state` | Text | State from Apollo |
| `apollo_country` | Text | Country from Apollo |

### 3. Add LinkedIn Fields (9 fields)

#### Profile Fields
| Field Name | Type | Notes |
|------------|------|-------|
| `linkedin_headline` | Text | Professional headline |
| `linkedin_summary` | Text | Profile summary/about |
| `linkedin_location` | Text | Location from profile |
| `linkedin_email` | Email | Email from LinkedIn |

#### Experience & Education
| Field Name | Type | Notes |
|------------|------|-------|
| `linkedin_experience` | Text | Work history (JSON format) |
| `linkedin_education` | Text | Education history (JSON format) |
| `linkedin_skills` | Text | Skills list (comma-separated) |

#### Metrics
| Field Name | Type | Notes |
|------------|------|-------|
| `linkedin_connections` | Number | Connection count |
| `linkedin_followers` | Number | Follower count |

### 4. Add Metadata Fields (7 fields)

#### Process Tracking
| Field Name | Type | Configuration |
|------------|------|--------------|
| `enrichment_status` | Select | Options: "Not Started", "In Progress", "Completed", "Failed" |
| `last_enriched` | Date & Time | When enrichment was last run |
| `enrichment_score` | Number | Data completeness (0-100) |
| `enrichment_notes` | Text | Notes about enrichment issues |

#### Quality Control
| Field Name | Type | Configuration |
|------------|------|--------------|
| `data_confidence` | Select | Options: "High", "Medium", "Low" |
| `verified_email` | Checkbox | Email verification status |
| `verified_phone` | Checkbox | Phone verification status |

## 🔧 Detailed Setup for Select Fields

### enrichment_status
1. Choose "Select" as field type
2. Add these options with colors:
   - **Not Started** (Gray)
   - **In Progress** (Yellow)  
   - **Completed** (Green)
   - **Failed** (Red)

### data_confidence  
1. Choose "Select" as field type
2. Add these options with colors:
   - **High** (Green)
   - **Medium** (Yellow)
   - **Low** (Red)

## ⚡ Quick Add Method

Instead of adding fields one by one, you can:

1. **Copy existing similar fields** - Duplicate existing email/text/url fields and rename them
2. **Use templates** - If you have field templates, apply them
3. **Bulk import** - Add sample data with the new field names to auto-create them

## 🧪 Verification Steps

After adding all fields, run this command to verify:

```bash
python3 check_notion_fields.py
```

You should see: ✅ All required fields are present!

## 🚀 Ready to Test Enrichment

Once all fields are added:

1. **Verify setup**: `python3 check_notion_fields.py`
2. **Test enrichment**: `APIFY_TOKEN='your_token' python3 apify_enrichment.py`
3. **Check results** in your Notion database

## 💡 Pro Tips

- **Set default values**: Set `enrichment_status` to "Not Started" by default
- **Hide unused fields**: You can hide Apollo/LinkedIn fields until they contain data
- **Group fields**: Create field groups for "Original Data", "Apollo Data", "LinkedIn Data", "Metadata"
- **Field descriptions**: Add descriptions to remember what each field contains

## 🔍 Expected Timeline

- **Adding fields**: 10-15 minutes
- **Testing enrichment**: 2-3 minutes per contact
- **Full setup verification**: 1-2 minutes

Once you've added these fields, we can run the enrichment on Matt Stevens' record and see the system in action!

---

*Need help? Run `python3 check_notion_fields.py` to see which fields are still missing.*
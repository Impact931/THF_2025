# Notion CSV Import Guide - THF Enrichment Database

## 🎯 Quick Setup Instructions

### Step 1: Import the CSV
1. In your THF_2025 Notion workspace, create a new database
2. Name it: **"THF Enrichment Data"**
3. Click "Import" → "CSV" 
4. Upload `THF_Enrichment_Database_Fields.csv`
5. Check "Use first row as header"
6. Import the file

### Step 2: Configure Field Types
After import, you'll need to set the correct field types for each column. Here's the complete mapping:

## 📋 Field Type Configuration

### Core Identity Fields
| Field Name | Set Type To | Configuration |
|------------|-------------|---------------|
| **Name** | Title | ✅ This will be your primary key field |
| Original Record ID | Text | For linking back to People DB |
| Enrichment Date | Date | When enrichment was performed |

### Data Sources Field
| Field Name | Set Type To | Options to Add |
|------------|-------------|----------------|
| Data Sources | Multi-select | Apollo (blue), LinkedIn (green), Manual Research (yellow), Web Scraping (orange) |

### Apollo Contact Fields
| Field Name | Set Type To | Notes |
|------------|-------------|-------|
| Apollo Email | Email | Verified work email |
| Apollo Personal Email | Email | Personal email address |
| Apollo Phone | Phone | Primary phone number |
| Apollo Mobile | Phone | Mobile phone number |
| Apollo Email Verified | Checkbox | Email verification status |
| Apollo Phone Verified | Checkbox | Phone verification status |

### Apollo Professional Fields
| Field Name | Set Type To | Configuration |
|------------|-------------|---------------|
| Apollo Title | Text | Job title |
| Apollo Company | Text | Company name |
| Apollo Company Size | Select | 1-10 (gray), 11-50 (brown), 51-200 (orange), 201-500 (yellow), 501-1000 (green), 1000+ (blue) |
| Apollo Industry | Text | Industry category |
| Apollo Department | Text | Department/division |
| Apollo Seniority | Select | Entry (gray), Junior (brown), Senior (orange), Manager (yellow), Director (green), VP (blue), C-Level (purple) |

### Apollo Location Fields
| Field Name | Set Type To | Notes |
|------------|-------------|-------|
| Apollo City | Text | City location |
| Apollo State | Text | State/province |
| Apollo Country | Text | Country |
| Apollo LinkedIn URL | URL | LinkedIn profile from Apollo |

### LinkedIn Profile Fields
| Field Name | Set Type To | Notes |
|------------|-------------|-------|
| LinkedIn Headline | Text | Professional headline |
| LinkedIn Summary | Text | Profile summary |
| LinkedIn Location | Text | Location from profile |
| LinkedIn Industry | Text | Industry from LinkedIn |
| LinkedIn Connections | Number | Connection count |
| LinkedIn Followers | Number | Follower count |
| LinkedIn Public Profile URL | URL | Direct LinkedIn URL |

### LinkedIn Professional Fields
| Field Name | Set Type To | Notes |
|------------|-------------|-------|
| LinkedIn Current Position | Text | Current job title |
| LinkedIn Current Company | Text | Current employer |
| LinkedIn Experience Count | Number | Number of jobs listed |
| LinkedIn Education Count | Number | Number of schools |
| LinkedIn Top Skills | Text | Comma-separated skills |
| LinkedIn Certifications | Text | Professional certifications |
| LinkedIn Languages | Text | Languages spoken |

### LinkedIn Activity Fields
| Field Name | Set Type To | Options |
|------------|-------------|---------|
| LinkedIn Last Activity | Date | Last post/activity date |
| LinkedIn Post Frequency | Select | Daily (green), Weekly (yellow), Monthly (orange), Rarely (red), Unknown (gray) |
| LinkedIn Influencer Score | Select | High (green), Medium (yellow), Low (red), Unknown (gray) |

### Enrichment Metadata Fields
| Field Name | Set Type To | Options |
|------------|-------------|---------|
| Enrichment Status | Select | Not Started (gray), In Progress (yellow), Completed (green), Failed (red), Partial (orange) |
| Data Confidence | Select | High (green), Medium (yellow), Low (red) |
| Completeness Score | Number | 0-100 percentage |
| Apollo Credits Used | Number | API credits consumed |
| LinkedIn API Calls | Number | API calls made |

### Notes & Documentation Fields
| Field Name | Set Type To | Purpose |
|------------|-------------|---------|
| Enrichment Notes | Text | Process notes and issues |
| Data Conflicts | Text | Where Apollo/LinkedIn disagree |
| Manual Overrides | Text | Manual corrections made |
| Next Review Date | Date | When to re-enrich |

### Raw Data Storage Fields
| Field Name | Set Type To | Purpose |
|------------|-------------|---------|
| Apollo Raw Data | Text | JSON data from Apollo |
| LinkedIn Raw Data | Text | JSON data from LinkedIn |
| LinkedIn Experience | Text | Formatted work history |
| LinkedIn Education | Text | Formatted education history |

### Relationship & Sync Fields
| Field Name | Set Type To | Purpose |
|------------|-------------|---------|
| Main Record Link | URL | Link back to People DB |
| Sync Status | Select | Synced (green), Pending (yellow), Conflict (red), Manual Review (orange) |

### Timestamp Fields
| Field Name | Set Type To | Auto-populated |
|------------|-------------|---------------|
| Created Time | Created time | ✅ Automatic |
| Last Updated | Last edited time | ✅ Automatic |
| Last Apollo Sync | Date | Manual entry |
| Last LinkedIn Sync | Date | Manual entry |

## 🚀 Post-Import Cleanup

### Step 3: Set Default Values
1. **Enrichment Status** → Set default to "Not Started"
2. **Data Confidence** → Set default to "Medium"
3. **Completeness Score** → Set default to 0

### Step 4: Configure Views
Create these filtered views:
- **Pending Enrichment** → Status = "Not Started"
- **High Quality Data** → Confidence = "High"
- **Needs Review** → Status = "Failed" or Conflicts exist
- **Apollo Data Only** → Has Apollo data, no LinkedIn
- **LinkedIn Data Only** → Has LinkedIn data, no Apollo

### Step 5: Test the Setup
1. Delete the sample row (Matt Stevens)
2. The field types should now be properly configured
3. Ready for enrichment data import!

## ✅ Verification Checklist

After setup, verify these key configurations:
- [ ] **Name** field is set as Title (primary key)
- [ ] Email fields are set to Email type
- [ ] Phone fields are set to Phone type  
- [ ] URL fields are set to URL type
- [ ] Select fields have all options configured with colors
- [ ] Number fields accept numeric values
- [ ] Date fields are properly formatted
- [ ] Multi-select field (Data Sources) has all options

## 🔧 Pro Tips

1. **Hide unused fields** until they contain data
2. **Group fields** by source (Apollo, LinkedIn, Metadata)
3. **Set field descriptions** for team clarity
4. **Create templates** for common enrichment scenarios
5. **Use formulas** to calculate derived metrics

Once configured, your enrichment database will be ready to receive data from the Apollo and LinkedIn scrapers!

---
*This database structure prevents data duplication by using Name as the primary key and provides comprehensive tracking of enrichment quality and sources.*
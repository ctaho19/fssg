# Query Output and Limitations

## What This Query Does
This Snowflake query validates system account certifications by comparing what **should** happen (expected results) versus what **actually** happened (actual results) during quarterly certification campaigns. It identifies discrepancies where the exclusion rules may not be working correctly.

## Current Limitations
**The `iiqcap1.certification_exclusion` Oracle table is not accessible in Snowflake**, which means 4 types of configurable exclusion rules are currently disabled:

1. **Applications to be excluded** - Custom applications that should never appear in certification
2. **Disabled account applications** - Applications where disabled accounts should be excluded
3. **Roles to be excluded** - Specific roles/bundles that should not be certified
4. **Entitlements to be excluded** - Individual permissions that should be excluded

## What Still Works
The query still validates these hardcoded exclusion rules:
- ✅ **AD empty accounts** - Active Directory accounts with no entitlements
- ✅ **Non-quarterly applications** - Apps not marked for quarterly certification  
- ✅ **Base type bundles** - System-level roles automatically excluded
- ✅ **Team site/role exclusions** - Roles containing "_TEAM_SITE" or "_TEAM_ROLE"
- ✅ **Duplicate role detection** - DetectedRoles that also exist as assignedRoles
- ✅ **AD non-requestable entitlements** - Entitlements without related applications
- ✅ **AWS Connect security profiles** - Automatically excluded
- ✅ **AWS IAM without native identity** - Accounts missing awsAccountList

## Expected Output
The query returns only **discrepancies** where expected ≠ actual results. Empty results = good (no issues found).

Each row shows:
- **What type of access** (Account/Bundle/Exception)
- **Which system account** (EID)
- **What application/permission**
- **Why it should be included/excluded** (Reason)
- **What was expected vs actual**

## Impact of Missing Rules
Without the Oracle exclusion table, you may see **false positives** where items appear as discrepancies but should actually be excluded by the missing configurable rules. The core validation logic remains intact and valuable for identifying most certification issues.

## Resolution
Once the `certification_exclusion` table is available in Snowflake, replace the disabled `exclusion_rules` CTE with the proper table reference and remove the `WHEN FALSE` conditions to restore full functionality.
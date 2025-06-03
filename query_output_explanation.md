# Identity Certification Validation Query - Output Explanation

## Purpose
This query validates whether the COF System ID Exclusion Rule is working correctly by comparing what should happen (expected) vs. what actually happened (actual) during quarterly certification campaigns.

## Query Output Columns

### 1. **ITEM_TYPE**
The type of access being validated:
- `Account` - Full account access to a system
- `Bundle` - Role collections (assigned or detected roles)
- `Exception` - Individual entitlements/permissions

### 2. **EID**
Employee/Entity ID of the system account (e.g., `GNB912_SRVC_08`, `SVC_APP01`)

### 3. **APPLICATION**
The system/application name:
- `Active Directory`
- `AWS IAM User/Role Request`
- `AWS Connect - Card_CDE-East`
- `CyberArk Vault`
- Other enterprise applications

### 4. **NATIVE_IDENTITY**
The actual username in the target system (may differ from EID)

### 5. **ATTRIBUTE**
The type of permission:
- `assignedRoles` - Manually assigned roles
- `detectedRoles` - Automatically detected roles
- `groups` - AD groups
- `SecurityProfileNames` - AWS profiles
- Other application-specific attributes

### 6. **VALUE**
The specific permission/role/group name

### 7. **REASON**
Explanation of why this item should be included or excluded:
- "AD Account not requestable and no related app"
- "Application not marked for quarterly certification"
- "Disabled account in excluded application"
- "Team site/role exclusion"
- "Role in exclusion list"
- "No exclusion found"

### 8. **EXPECTED_RESULT**
What the COF System ID Exclusion Rule dictates:
- `Inclusion` - Should appear in certification for review
- `Exclusion` - Should NOT appear in certification

### 9. **ACTUAL_RESULT**
What actually happened in the certification:
- `Inclusion` - Did appear in certification
- `Exclusion` - Did NOT appear in certification
- `No Data in Cert` - Not found in certification data

### 10. **STATUS**
Always shows `DISCREPANCY` (query only returns mismatches)

## What You'll See

The query **only returns rows where there's a mismatch** between expected and actual results.

### Types of Discrepancies

#### 1. **False Inclusions** (Should be excluded but wasn't)
```
Expected: Exclusion
Actual: Inclusion
Example: A disabled AD account that still appeared in certification
```

#### 2. **False Exclusions** (Should be included but wasn't)
```
Expected: Inclusion  
Actual: Exclusion
Example: A quarterly-certified application that was missed
```

## Example Output

```
ITEM_TYPE | EID        | APPLICATION      | NATIVE_IDENTITY | ATTRIBUTE      | VALUE              | REASON                                           | EXPECTED_RESULT | ACTUAL_RESULT | STATUS
----------|------------|------------------|-----------------|----------------|--------------------|--------------------------------------------------|-----------------|---------------|------------
Account   | SVC_APP01  | Active Directory | svc_app01       | NULL           | NULL               | AD Account with no entitlements                 | Exclusion       | Inclusion     | DISCREPANCY
Bundle    | SVC_DB02   | NULL             | NULL            | detectedRoles  | IT_TEAM_SITE_01    | Team site/role exclusion                         | Exclusion       | Inclusion     | DISCREPANCY
Exception | SVC_AWS03  | AWS IAM          | NULL            | awsAccountList | prod-account       | AWS IAM User/Role Request without awsAccountList| Exclusion       | Inclusion     | DISCREPANCY
Exception | SVC_CYB04  | CyberArk Vault   | svc_cyb04       | groups         | SPHINX-PROD-ADMIN  | Entitlement in COF Certification Exclusion      | Exclusion       | Inclusion     | DISCREPANCY
```

## Interpreting Results

### No Results = Good!
If the query returns **no rows**, the exclusion rule is working perfectly - all items were correctly included or excluded according to business rules.

### Many Results = Investigation Needed
Large numbers of discrepancies indicate:
1. Exclusion rules may need updating
2. Custom object configurations might be incorrect
3. Data synchronization issues between systems
4. Changes in business requirements not reflected in rules

## Common Discrepancy Patterns

1. **Disabled Accounts Still Certified**
   - Check if application is in `disabledAccountApplicationsToBeExcluded`
   - Verify the `IIQ_DISABLED` flag is set correctly

2. **Team Sites/Roles Appearing**
   - Ensure role names contain `_TEAM_SITE` or `_TEAM_ROLE`
   - Verify bundle type is 'it' or 'business'

3. **AD Non-Requestable Entitlements**
   - Check `requestable` flag in managed attributes
   - Verify no related application is set

4. **Missing Quarterly Applications**
   - Ensure `cof_app_certification_freq` = 'Quarterly'
   - Check application isn't in exclusion list

## Use Cases

1. **Audit Compliance**: Prove certification process follows defined rules
2. **Rule Debugging**: Identify which exclusion rules aren't working
3. **Process Improvement**: Find patterns to refine exclusion logic
4. **Exception Reporting**: Generate reports for manual review

## Next Steps

1. Run query after each certification campaign
2. Investigate each discrepancy to determine root cause
3. Update exclusion rules or fix data issues as needed
4. Re-run to verify fixes are working
5. Document any business rule changes
# Certificate Discovery and Analysis Guide

This guide explains how to use the Certificate Discovery Analyzer to identify and analyze ALL ACM certificates across different issuers (Amazon, DigiCert, AWS Private CA, and others).

## Overview

The Certificate Discovery Analyzer is built on the CTRL-1077188 pipeline patterns and provides comprehensive certificate portfolio analysis by:

1. **Discovering ALL ACM certificates** via CloudRadar API (no issuer restrictions)
2. **Categorizing certificates** by issuer type (Amazon, DigiCert, AWS Private CA, etc.)
3. **Analyzing usage patterns** (in-use vs. unused certificates)
4. **Evaluating expiration timelines** (expired, expiring in 30/90 days)
5. **Comparing with catalog data** (when CSV provided)
6. **Exporting detailed results** for further analysis

## Prerequisites

- Bearer token for CloudRadar API authentication 
- Python environment with required dependencies (requests, pandas, etc.)
- (Optional) CSV file containing certificate ARNs from Snowflake catalog

## Authentication Setup

The certificate analysis scripts use bearer token authentication. Set your authentication token as an environment variable:

```bash
export AUTH_TOKEN="your-bearer-token-here"
```

Or provide it inline when running the script:

```bash
AUTH_TOKEN="your-bearer-token-here" python certificate_discovery_analyzer.py
```

## Quick Start Examples

### 1. Basic Certificate Discovery

Discover all certificates without any additional analysis:

```bash
AUTH_TOKEN="your-bearer-token" python certificate_discovery_analyzer.py
```

This will:
- Fetch ALL ACM certificates from CloudRadar API
- Categorize by issuer type
- Display summary report in console
- Show distribution by Amazon, DigiCert, AWS Private CA, etc.

### 2. Discovery with Catalog Comparison

Compare API discoveries with your Snowflake certificate catalog:

```bash
AUTH_TOKEN="your-bearer-token" python certificate_discovery_analyzer.py \\
    --catalog-csv snowflake_certificates.csv \\
    --compare-catalog
```

This adds:
- Gap analysis between API and catalog data
- Coverage percentages 
- Lists of certificates unique to each source

### 3. Full Analysis with Exports

Comprehensive analysis with detailed exports:

```bash
AUTH_TOKEN="your-bearer-token" python certificate_discovery_analyzer.py \\
    --catalog-csv snowflake_certificates.csv \\
    --compare-catalog \\
    --output-json detailed_analysis.json \\
    --output-csv certificate_summary.csv \\
    --verbose
```

This provides:
- Detailed JSON export with all certificate metadata
- CSV summary for spreadsheet analysis
- Verbose logging for troubleshooting
- Complete catalog comparison

### 4. Testing Mode

Test the script without making API calls:

```bash
AUTH_TOKEN="your-bearer-token" python certificate_discovery_analyzer.py \\
    --dry-run \\
    --output-json mock_analysis.json
```

## Input: Snowflake Certificate CSV Format

The `--catalog-csv` option accepts CSV files with certificate ARNs from your Snowflake catalog. The script automatically detects ARN columns by looking for common column names:

### Supported Column Names
- `certificate_arn`
- `cert_arn` 
- `arn`
- Any column containing "arn" (case-insensitive)

### Example CSV Formats

**Simple format (single column):**
```csv
certificate_arn
arn:aws:acm:us-east-1:123456789012:certificate/12345678-1234-1234-1234-123456789012
arn:aws:acm:us-west-2:123456789012:certificate/87654321-4321-4321-4321-210987654321
```

**Multi-column format:**
```csv
certificate_arn,nominal_issuer,not_valid_after
arn:aws:acm:us-east-1:123456789012:certificate/12345678-1234-1234-1234-123456789012,Amazon,2024-12-31
arn:aws:acm:us-west-2:123456789012:certificate/87654321-4321-4321-4321-210987654321,DigiCert,2024-06-15
```

**Note**: If no ARN column is detected, the script uses the first column.

## Output Formats

### Console Summary Report

The console output provides an executive summary:

```
============================================================
CERTIFICATE DISCOVERY ANALYSIS REPORT
============================================================

📊 SUMMARY
Total Certificates: 1,247
In Use: 892 (71.5%)
Unused: 355
Expired: 23
Expiring (30 days): 45
Expiring (90 days): 127

🏢 ISSUER ANALYSIS
Amazon              : 1,098 certs (810 in use, 73.8%, 15 expired)
DigiCert            : 89 certs (52 in use, 58.4%, 5 expired)
AWS_Private_CA      : 34 certs (18 in use, 52.9%, 2 expired)
LetsEncrypt         : 15 certs (8 in use, 53.3%, 1 expired)
Other               : 11 certs (4 in use, 36.4%, 0 expired)

📋 STATUS DISTRIBUTION
ISSUED              : 1,224
EXPIRED             : 23

🌍 TOP REGIONS
us-east-1           : 567
us-west-2           : 234
eu-west-1           : 156
ap-southeast-1      : 98

🔍 CATALOG COMPARISON
API Certificates: 1,247
Catalog Certificates: 1,203
Common Certificates: 1,156
API Coverage of Catalog: 96.1%
Catalog Coverage of API: 92.7%
API-Only Certificates: 91
Catalog-Only Certificates: 47
```

### JSON Export Format

The `--output-json` option creates a comprehensive JSON file with:

```json
{
  "analysis_timestamp": "2024-11-05T12:09:00",
  "summary": {
    "total_certificates": 1247,
    "in_use_certificates": 892,
    "unused_certificates": 355,
    "expired_certificates": 23,
    "expiring_30_days": 45,
    "expiring_90_days": 127,
    "usage_percentage": 71.53
  },
  "issuer_analysis": {
    "Amazon": {
      "count": 1098,
      "in_use": 810,
      "expired": 15,
      "expiring_30_days": 32,
      "expiring_90_days": 89
    }
  },
  "status_distribution": {
    "ISSUED": 1224,
    "EXPIRED": 23
  },
  "region_distribution": {
    "us-east-1": 567,
    "us-west-2": 234
  },
  "catalog_comparison": {
    "api_certificates": 1247,
    "catalog_certificates": 1203,
    "common_certificates": 1156,
    "api_coverage_of_catalog_percent": 96.09,
    "catalog_coverage_of_api_percent": 92.70,
    "api_only_arns": ["arn:aws:acm:..."],
    "catalog_only_arns": ["arn:aws:acm:..."]
  },
  "certificates": [
    {
      "arn": "arn:aws:acm:us-east-1:123456789012:certificate/12345678-1234-1234-1234-123456789012",
      "certificate_id": "12345678-1234-1234-1234-123456789012",
      "issuer": "Amazon",
      "issuer_category": "Amazon",
      "status": "ISSUED",
      "domain_name": "example.com",
      "subject_alternative_names": ["www.example.com", "api.example.com"],
      "not_before": "2024-01-01T00:00:00",
      "not_after": "2024-12-31T23:59:59",
      "in_use_by": ["arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/test/abcd1234"],
      "is_in_use": true,
      "source": "ConfigurationSnapshot",
      "region": "us-east-1",
      "account_id": "123456789012",
      "days_until_expiry": 56,
      "is_expired": false,
      "expiry_category": "Expiring_90_days"
    }
  ]
}
```

### CSV Export Format

The `--output-csv` option creates a spreadsheet-friendly summary:

| certificate_arn | certificate_id | issuer | issuer_category | status | domain_name | is_in_use | in_use_by_count | region | account_id | not_before | not_after | days_until_expiry | is_expired | expiry_category |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| arn:aws:acm:us-east-1:123... | 12345678-1234... | Amazon | Amazon | ISSUED | example.com | TRUE | 1 | us-east-1 | 123456789012 | 2024-01-01T00:00:00 | 2024-12-31T23:59:59 | 56 | FALSE | Expiring_90_days |

## Certificate Issuer Categories

The script automatically categorizes certificates by issuer:

### Amazon
- **Criteria**: Issuer contains "Amazon"
- **Description**: AWS-managed ACM certificates
- **Example**: `CN=Amazon, OU=Server CA 1B, O=Amazon, C=US`

### DigiCert
- **Criteria**: Issuer contains "DigiCert"
- **Description**: Commercial DigiCert certificates
- **Example**: `CN=DigiCert SHA2 Secure Server CA, O=DigiCert Inc, C=US`

### AWS_Private_CA
- **Criteria**: Issuer contains "Private CA", "PrivateCA", or "AWS Certificate Authority"
- **Description**: AWS Private Certificate Authority issued certificates
- **Example**: `CN=My Company CA, OU=Engineering, O=My Company Inc`

### LetsEncrypt
- **Criteria**: Issuer contains "Let's Encrypt" or "LetsEncrypt"
- **Description**: Free Let's Encrypt certificates
- **Example**: `CN=R3, O=Let's Encrypt, C=US`

### Sectigo
- **Criteria**: Issuer contains "Sectigo" or "Comodo"
- **Description**: Sectigo (formerly Comodo) certificates
- **Example**: `CN=Sectigo RSA Domain Validation Secure Server CA, O=Sectigo Limited, L=Salford, ST=Greater Manchester, C=GB`

### Other Categories
- **GoDaddy**: Issuer contains "GoDaddy"
- **GlobalSign**: Issuer contains "GlobalSign"
- **Entrust**: Issuer contains "Entrust"
- **Symantec_VeriSign**: Issuer contains "Symantec" or "VeriSign"
- **Other**: Any other certificate authority

## Expiration Categories

Certificates are automatically categorized by expiration timeline:

- **Expired**: Certificate has already expired
- **Expiring_30_days**: Certificate expires within 30 days
- **Expiring_90_days**: Certificate expires within 31-90 days
- **Expiring_1_year**: Certificate expires within 91-365 days
- **Long_term_valid**: Certificate expires in more than 1 year

## Integration with Existing Pipelines

### Expanding CTRL-1077188 Pipeline

To expand the existing certificate monitoring pipeline based on discoveries:

1. **Update API Search Parameters**:
   ```python
   # Current (Amazon only)
   "configurationItems": [
       {
           "configurationName": "issuer",
           "configurationValue": "Amazon"
       }
   ]
   
   # Expanded (all certificates)
   # Remove configurationItems filter entirely
   ```

2. **Add Issuer-Specific Logic**:
   ```python
   def _categorize_certificate(self, resource):
       issuer = self._get_config_value(resource, "issuer")
       
       if "Amazon" in issuer:
           return "amazon"
       elif "DigiCert" in issuer:
           return "digicert"
       elif "Private CA" in issuer:
           return "aws_private_ca"
       # ... other categories
   ```

3. **Update SQL Queries**:
   ```sql
   -- Current (Amazon only)
   WHERE nominal_issuer = 'Amazon'
   
   -- Expanded (all issuers)
   WHERE nominal_issuer IN ('Amazon', 'DigiCert', 'AWS Private CA', ...)
   -- OR remove filter entirely for comprehensive coverage
   ```

## Performance and Rate Limiting

The script includes built-in optimizations:

- **Rate Limiting**: 150ms delay between API calls
- **Pagination**: Automatic handling of large result sets
- **Error Handling**: Retry logic with exponential backoff
- **Memory Efficiency**: Streaming processing of large datasets

### Typical Performance

- **Small environment** (< 100 certs): 1-2 minutes
- **Medium environment** (100-1000 certs): 3-10 minutes
- **Large environment** (1000+ certs): 10-30 minutes

## Troubleshooting

### Common Issues

**1. Authentication Errors**
```
ERROR: AUTH_TOKEN environment variable not set
```
- Ensure AUTH_TOKEN environment variable is set with a valid bearer token
- Verify your bearer token has the necessary permissions for CloudRadar API access

**2. CSV Loading Errors**
```
ERROR: Failed to load catalog CSV
```
- Ensure CSV file exists and is readable
- Check that file contains valid certificate ARNs
- Try removing any non-ARN rows from CSV

**3. API Rate Limiting**
```
WARNING: API request failed: 429 - Too Many Requests
```
- Script includes automatic retry logic
- Consider running during off-peak hours for large environments

**4. Memory Issues with Large Datasets**
```
ERROR: Out of memory
```
- Use `--dry-run` to test without API calls
- Consider filtering results by region or account if needed

### Debug Mode

Use `--verbose` flag for detailed logging:

```bash
AUTH_TOKEN="your-bearer-token" python certificate_discovery_analyzer.py --verbose
```

This provides:
- Detailed API request/response logging
- Step-by-step processing information
- Performance metrics and timing
- Error details and stack traces

## Use Cases

### Security Audit
- Identify all certificate authorities in use
- Find unused certificates that can be decommissioned
- Locate expired certificates still present in infrastructure

### Certificate Lifecycle Management
- Track certificates approaching expiration
- Monitor certificate usage patterns
- Plan certificate renewals and migrations

### Compliance Reporting
- Generate comprehensive certificate inventory
- Validate certificate coverage in monitoring systems
- Document certificate authority diversity

### Cost Optimization
- Identify unused certificates for cleanup
- Optimize certificate requests and renewals
- Consolidate certificate authorities where possible

## Integration Examples

### Pipeline Integration
```python
# Use in existing ETIP pipeline
from certificate_discovery_analyzer import CertificateDiscoveryAnalyzer

analyzer = CertificateDiscoveryAnalyzer(env)
certificates = analyzer.discover_all_certificates()

# Filter for specific issuer types
digicert_certs = [cert for cert in certificates if cert.issuer_category == "DigiCert"]
```

### Automation Scripts
```python
# Automated monitoring script
import schedule
import time

def daily_certificate_check():
    analyzer = CertificateDiscoveryAnalyzer(env)
    certificates = analyzer.discover_all_certificates()
    
    # Check for certificates expiring soon
    expiring_soon = [cert for cert in certificates 
                    if cert.days_until_expiry and cert.days_until_expiry <= 30]
    
    if expiring_soon:
        send_alert(f"Found {len(expiring_soon)} certificates expiring within 30 days")

schedule.every().day.at("09:00").do(daily_certificate_check)
```

## Next Steps

After running the analysis:

1. **Review issuer distribution** - Understand your certificate authority portfolio
2. **Identify gaps** - Check certificates missing from catalog vs. API
3. **Plan expansions** - Update monitoring pipelines to include all certificate types
4. **Establish baselines** - Use current state as baseline for ongoing monitoring
5. **Automate monitoring** - Integrate findings into automated certificate lifecycle management

This comprehensive analysis provides the foundation for expanding certificate monitoring beyond just Amazon-issued certificates to include your entire certificate portfolio.
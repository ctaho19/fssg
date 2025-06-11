#!/usr/bin/env python3
"""
Analyze KMS keys with rotation status false to find patterns for CTRL-1077224 exclusions.
This script paginates through CloudRadar API to analyze KMS key configurations.
"""

import json
import os
import sys
from typing import Dict, List, Any, Optional
from datetime import datetime
from collections import defaultdict, Counter
import pandas as pd
import requests
import time
import logging


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class KMSRotationAnalyzer:
    """Analyze KMS keys to find patterns in rotation status false cases."""
    
    def __init__(self, auth_token: str):
        """
        Initialize the analyzer with a bearer token.
        
        Args:
            auth_token: Bearer token for API authentication
        """
        self.base_url = "https://api.cloud.capitalone.com/internal-operations/cloud-service/aws-tooling"
        self.config_url = f"{self.base_url}/search-resource-configurations"
        self.headers = {
            'Accept': 'application/json;v=1',
            'Authorization': f'Bearer {auth_token}',
            'Content-Type': 'application/json'
        }
    
    def fetch_kms_keys(self, resource_ids: Optional[List[str]] = None, timeout: int = 120, max_retries: int = 3) -> List[Dict[str, Any]]:
        """
        Fetch KMS keys from CloudRadar API with pagination.
        
        Args:
            resource_ids: Optional list of specific resource IDs to analyze
            timeout: Request timeout in seconds
            max_retries: Maximum number of retry attempts
            
        Returns:
            List of KMS key resource configurations
        """
        all_resources = []
        next_record_key = None
        page_count = 0
        
        while True:
            page_count += 1
            
            # Build search parameters
            search_params = [{"resourceType": "AWS::KMS::Key"}]
            if resource_ids:
                search_params[0]["resourceId"] = resource_ids
            
            payload = {
                "searchParameters": search_params
                # No responseFields specified - get everything
            }
            
            params = {"limit": 10000}
            if next_record_key:
                params["nextRecordKey"] = next_record_key
            
            # Retry logic
            for retry in range(max_retries + 1):
                try:
                    logger.info(f"Fetching page {page_count}" + (f" (retry {retry})" if retry > 0 else ""))
                    
                    response = requests.post(
                        self.config_url,
                        headers=self.headers,
                        json=payload,
                        params=params,
                        verify=False,  # Set to True in production with proper cert management
                        timeout=timeout
                    )
                    
                    if response.status_code == 200:
                        data = response.json()
                        resources = data.get("resourceConfigurations", [])
                        all_resources.extend(resources)
                        
                        logger.info(f"Page {page_count}: Fetched {len(resources)} resources (Total: {len(all_resources)})")
                        
                        next_record_key = data.get("nextRecordKey")
                        break  # Success, exit retry loop
                        
                    elif response.status_code == 429:
                        wait_time = min(2 ** retry, 60)
                        logger.warning(f"Rate limited (429). Waiting {wait_time}s before retry {retry+1}/{max_retries}")
                        time.sleep(wait_time)
                        if retry == max_retries:
                            raise RuntimeError(f"Max retries reached for rate limiting")
                    else:
                        logger.error(f"API request failed: {response.status_code} - {response.text}")
                        if retry < max_retries:
                            wait_time = min(2 ** retry, 30)
                            time.sleep(wait_time)
                        else:
                            raise RuntimeError(f"API request failed after {max_retries + 1} attempts: {response.status_code}")
                            
                except requests.exceptions.Timeout:
                    logger.warning(f"Request timeout after {timeout}s")
                    if retry < max_retries:
                        wait_time = min(2 ** retry, 30)
                        time.sleep(wait_time)
                    else:
                        raise RuntimeError(f"Request timeout after {max_retries + 1} attempts")
                        
                except Exception as e:
                    logger.error(f"Exception during API request: {str(e)}")
                    if retry < max_retries:
                        wait_time = min(2 ** retry, 30)
                        time.sleep(wait_time)
                    else:
                        raise
            
            # Check if we're done paginating
            if not next_record_key:
                logger.info(f"No more pages. Total resources fetched: {len(all_resources)}")
                break
        
        return all_resources
    
    def extract_key_attributes(self, resource: Dict[str, Any]) -> Dict[str, Any]:
        """Extract all attributes from a KMS key resource."""
        # Start with all top-level fields
        attributes = {k: v for k, v in resource.items() if k not in ["configurationList", "supplementaryConfiguration"]}
        
        # Extract all configuration values dynamically
        config_list = resource.get("configurationList", [])
        for config in config_list:
            config_name = config.get("configurationName", "")
            config_value = config.get("configurationValue")
            
            # Create a simplified key name (remove 'configuration.' prefix)
            key_name = config_name.replace("configuration.", "") if config_name.startswith("configuration.") else config_name
            attributes[key_name] = config_value
        
        # Extract all supplementary configuration dynamically
        supp_config = resource.get("supplementaryConfiguration", [])
        for config in supp_config:
            config_name = config.get("supplementaryConfigurationName", "")
            config_value = config.get("supplementaryConfigurationValue")
            
            # Create a simplified key name (remove 'supplementaryConfiguration.' prefix)
            key_name = config_name.replace("supplementaryConfiguration.", "") if config_name.startswith("supplementaryConfiguration.") else config_name
            
            # Try to parse JSON values
            if key_name in ["aliases", "keyPolicy", "grants"]:
                try:
                    attributes[key_name] = json.loads(config_value) if config_value else []
                except:
                    attributes[key_name] = config_value
            else:
                attributes[key_name] = config_value
                
            # Also store with original name for compatibility
            if config_name == "supplementaryConfiguration.KeyRotationStatus":
                attributes["rotationStatus"] = config_value
        
        # Store raw configurations as well for debugging
        attributes["_raw_configurationList"] = config_list
        attributes["_raw_supplementaryConfiguration"] = supp_config
        
        return attributes
    
    def analyze_rotation_false_patterns(self, keys: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Analyze patterns in KMS keys with rotation status false.
        
        Args:
            keys: List of KMS key resources
            
        Returns:
            Dictionary containing pattern analysis results
        """
        rotation_false_keys = []
        rotation_true_keys = []
        no_rotation_keys = []
        
        # Categorize keys by rotation status
        for key in keys:
            attrs = self.extract_key_attributes(key)
            
            # Skip AWS managed keys and keys pending deletion
            if attrs.get("keyManager") == "AWS":
                continue
            if attrs.get("keyState") in ["PendingDeletion", "PendingReplicaDeletion"]:
                continue
            
            rotation_status = attrs.get("KeyRotationStatus") or attrs.get("rotationStatus")
            if rotation_status == "FALSE":
                rotation_false_keys.append(attrs)
            elif rotation_status == "TRUE":
                rotation_true_keys.append(attrs)
            else:
                no_rotation_keys.append(attrs)
        
        # Analyze patterns in rotation false keys
        patterns = {
            "total_analyzed": len(keys),
            "rotation_false_count": len(rotation_false_keys),
            "rotation_true_count": len(rotation_true_keys),
            "no_rotation_status_count": len(no_rotation_keys),
            "patterns": {}
        }
        
        if rotation_false_keys:
            # Pattern 1: Key origins
            origins = Counter(key.get("origin", "Unknown") for key in rotation_false_keys)
            patterns["patterns"]["origins"] = dict(origins)
            
            # Pattern 2: Multi-region keys
            multi_region = Counter(key.get("multiRegion", "False") for key in rotation_false_keys)
            patterns["patterns"]["multi_region"] = dict(multi_region)
            
            # Pattern 3: Account distribution
            accounts = Counter(key.get("accountName", "Unknown") for key in rotation_false_keys)
            patterns["patterns"]["top_accounts"] = dict(accounts.most_common(10))
            
            # Pattern 4: Key age analysis
            creation_dates = []
            for key in rotation_false_keys:
                if key.get("creationDate"):
                    try:
                        creation_dates.append(datetime.fromisoformat(key["creationDate"].replace("Z", "+00:00")))
                    except:
                        pass
            
            if creation_dates:
                oldest_date = min(creation_dates)
                newest_date = max(creation_dates)
                patterns["patterns"]["age_range"] = {
                    "oldest": oldest_date.isoformat(),
                    "newest": newest_date.isoformat(),
                    "days_range": (newest_date - oldest_date).days
                }
            
            # Pattern 5: Description patterns
            descriptions = [key.get("description", "") for key in rotation_false_keys if key.get("description")]
            common_keywords = defaultdict(int)
            for desc in descriptions:
                desc_lower = desc.lower()
                for keyword in ["backup", "test", "temp", "legacy", "migration", "archive", "import", "external", "replica"]:
                    if keyword in desc_lower:
                        common_keywords[keyword] += 1
            patterns["patterns"]["description_keywords"] = dict(common_keywords)
            
            # Pattern 6: Alias patterns
            alias_patterns = defaultdict(int)
            for key in rotation_false_keys:
                aliases = key.get("aliases", [])
                for alias in aliases:
                    if "alias/" in alias:
                        alias_name = alias.split("alias/")[1]
                        for keyword in ["backup", "test", "temp", "legacy", "migration", "archive", "import", "external", "replica"]:
                            if keyword in alias_name.lower():
                                alias_patterns[keyword] += 1
            patterns["patterns"]["alias_keywords"] = dict(alias_patterns)
            
            # Pattern 7: Tag patterns
            tag_patterns = defaultdict(int)
            tag_values = defaultdict(list)
            for key in rotation_false_keys:
                tags = key.get("tags", {})
                for tag_key, tag_value in tags.items():
                    tag_patterns[tag_key] += 1
                    if tag_value and len(tag_values[tag_key]) < 10:  # Limit examples
                        tag_values[tag_key].append(tag_value)
            
            patterns["patterns"]["common_tags"] = dict(tag_patterns.most_common(20))
            patterns["patterns"]["tag_value_examples"] = {k: list(set(v)) for k, v in tag_values.items() if len(v) > 1}
            
            # Pattern 8: Source patterns (for access denied)
            sources = Counter(key.get("source", "Unknown") for key in rotation_false_keys)
            patterns["patterns"]["sources"] = dict(sources)
            
            # Pattern 9: Analyze all unique fields present
            all_fields = set()
            for key in rotation_false_keys:
                all_fields.update(key.keys())
            patterns["patterns"]["available_fields"] = sorted(list(all_fields))
            
            # Pattern 10: Key usage and grants
            has_grants = sum(1 for key in rotation_false_keys if key.get("grants"))
            patterns["patterns"]["keys_with_grants"] = has_grants
            
            # Pattern 11: Key policies patterns
            policy_patterns = defaultdict(int)
            for key in rotation_false_keys:
                policy = key.get("keyPolicy", {})
                if isinstance(policy, dict) and "Statement" in policy:
                    for statement in policy.get("Statement", []):
                        if isinstance(statement, dict):
                            effect = statement.get("Effect", "")
                            principal = statement.get("Principal", {})
                            if isinstance(principal, dict) and "Service" in principal:
                                services = principal["Service"] if isinstance(principal["Service"], list) else [principal["Service"]]
                                for service in services:
                                    policy_patterns[f"Service:{service}"] += 1
            patterns["patterns"]["policy_service_principals"] = dict(policy_patterns)
            
            # Pattern 12: Additional configuration analysis
            config_fields = defaultdict(list)
            for key in rotation_false_keys[:20]:  # Sample first 20 keys
                for field_name, field_value in key.items():
                    if not field_name.startswith("_") and field_name not in ["keyPolicy", "tags", "aliases"]:
                        if field_value and str(field_value).strip():
                            config_fields[field_name].append(str(field_value)[:100])  # Truncate long values
            
            # Find fields with diverse values that might indicate patterns
            patterns["patterns"]["field_value_samples"] = {}
            for field, values in config_fields.items():
                unique_values = list(set(values))
                if len(unique_values) > 1 and len(unique_values) < 10:
                    patterns["patterns"]["field_value_samples"][field] = unique_values[:5]
        
        return patterns
    
    def generate_report(self, patterns: Dict[str, Any], specific_keys: Optional[List[Dict[str, Any]]] = None):
        """Generate a detailed report of the analysis."""
        print("\n" + "="*80)
        print("KMS KEY ROTATION ANALYSIS REPORT FOR CTRL-1077224")
        print("="*80)
        print(f"Generated: {datetime.now().isoformat()}")
        print(f"\nTotal Keys Analyzed: {patterns['total_analyzed']}")
        print(f"Keys with Rotation FALSE: {patterns['rotation_false_count']}")
        print(f"Keys with Rotation TRUE: {patterns['rotation_true_count']}")
        print(f"Keys without Rotation Status: {patterns['no_rotation_status_count']}")
        
        if patterns["patterns"]:
            print("\n" + "-"*40)
            print("IDENTIFIED PATTERNS IN ROTATION=FALSE KEYS:")
            print("-"*40)
            
            # Origins
            if patterns["patterns"].get("origins"):
                print("\n1. Key Origins:")
                for origin, count in patterns["patterns"]["origins"].items():
                    print(f"   - {origin}: {count} keys")
            
            # Multi-region
            if patterns["patterns"].get("multi_region"):
                print("\n2. Multi-Region Status:")
                for status, count in patterns["patterns"]["multi_region"].items():
                    print(f"   - {status}: {count} keys")
            
            # Accounts
            if patterns["patterns"].get("top_accounts"):
                print("\n3. Top Accounts (by count):")
                for account, count in patterns["patterns"]["top_accounts"].items():
                    print(f"   - {account}: {count} keys")
            
            # Age
            if patterns["patterns"].get("age_range"):
                age = patterns["patterns"]["age_range"]
                print(f"\n4. Key Age Range:")
                print(f"   - Oldest: {age['oldest']}")
                print(f"   - Newest: {age['newest']}")
                print(f"   - Range: {age['days_range']} days")
            
            # Description keywords
            if patterns["patterns"].get("description_keywords"):
                print("\n5. Common Keywords in Descriptions:")
                for keyword, count in sorted(patterns["patterns"]["description_keywords"].items(), key=lambda x: x[1], reverse=True):
                    print(f"   - '{keyword}': {count} keys")
            
            # Alias keywords
            if patterns["patterns"].get("alias_keywords"):
                print("\n6. Common Keywords in Aliases:")
                for keyword, count in sorted(patterns["patterns"]["alias_keywords"].items(), key=lambda x: x[1], reverse=True):
                    print(f"   - '{keyword}': {count} keys")
            
            # Tags
            if patterns["patterns"].get("common_tags"):
                print("\n7. Most Common Tags:")
                for tag, count in patterns["patterns"]["common_tags"].items():
                    print(f"   - {tag}: {count} keys")
            
            # Sources
            if patterns["patterns"].get("sources"):
                print("\n8. Data Sources:")
                for source, count in patterns["patterns"]["sources"].items():
                    print(f"   - {source}: {count} keys")
            
            # Available fields
            if patterns["patterns"].get("available_fields"):
                print("\n9. Available Fields in Response:")
                print(f"   Total unique fields: {len(patterns['patterns']['available_fields'])}")
                print(f"   Fields: {', '.join(patterns['patterns']['available_fields'][:20])}...")
            
            # Grants
            if "keys_with_grants" in patterns["patterns"]:
                print(f"\n10. Keys with Grants: {patterns['patterns']['keys_with_grants']}")
            
            # Policy patterns
            if patterns["patterns"].get("policy_service_principals"):
                print("\n11. Service Principals in Key Policies:")
                for service, count in sorted(patterns["patterns"]["policy_service_principals"].items(), key=lambda x: x[1], reverse=True)[:10]:
                    print(f"   - {service}: {count} keys")
            
            # Field value samples
            if patterns["patterns"].get("field_value_samples"):
                print("\n12. Interesting Field Value Patterns:")
                for field, values in patterns["patterns"]["field_value_samples"].items():
                    print(f"   - {field}: {values}")
        
        # Specific key details if requested
        if specific_keys:
            print("\n" + "-"*40)
            print("SPECIFIC KEY DETAILS:")
            print("-"*40)
            for i, key in enumerate(specific_keys[:10], 1):  # Limit to first 10
                print(f"\nKey {i}:")
                print(f"  Resource ID: {key.get('resourceId')}")
                print(f"  Account: {key.get('accountName')}")
                print(f"  Origin: {key.get('origin')}")
                print(f"  Multi-Region: {key.get('multiRegion')}")
                print(f"  Description: {key.get('description', 'N/A')}")
                print(f"  Aliases: {', '.join(key.get('aliases', [])) if key.get('aliases') else 'None'}")
                if key.get('tags'):
                    print(f"  Tags: {', '.join(f'{k}={v}' for k, v in key.get('tags', {}).items())}")
        
        print("\n" + "="*80)
        print("RECOMMENDATIONS FOR EXCLUSION CRITERIA:")
        print("="*80)
        print("\nBased on the patterns identified, consider excluding keys with:")
        print("1. Origin = 'EXTERNAL' (imported keys cannot have automatic rotation)")
        print("2. Multi-Region = 'True' (replica keys inherit rotation from primary)")
        print("3. Specific tag patterns indicating test/backup/temporary keys")
        print("4. Description/alias keywords: backup, test, temp, legacy, migration")
        print("5. Keys in specific accounts used for testing or non-production")
        print("\n")


def main():
    """Main function to run the analysis."""
    # Check for required bearer token
    auth_token = os.environ.get("AUTH_TOKEN")
    if not auth_token:
        print("ERROR: AUTH_TOKEN environment variable not set.")
        print("Usage: AUTH_TOKEN='your-bearer-token' python analyze_kms_rotation_patterns.py [resource-id-1] [resource-id-2] ...")
        sys.exit(1)
    
    # Initialize analyzer
    analyzer = KMSRotationAnalyzer(auth_token)
    
    print("Fetching KMS keys from CloudRadar API...")
    
    # Check if specific resource IDs were provided as command line arguments
    resource_ids = None
    if len(sys.argv) > 1:
        resource_ids = sys.argv[1:]
        print(f"Analyzing specific resource IDs: {resource_ids}")
    
    try:
        # Fetch KMS keys
        keys = analyzer.fetch_kms_keys(resource_ids)
        print(f"Fetched {len(keys)} KMS keys")
        
        # Analyze patterns
        patterns = analyzer.analyze_rotation_false_patterns(keys)
        
        # Get specific examples of rotation=false keys
        rotation_false_examples = []
        for key in keys:
            attrs = analyzer.extract_key_attributes(key)
            rotation_status = attrs.get("KeyRotationStatus") or attrs.get("rotationStatus")
            if rotation_status == "FALSE" and attrs.get("keyManager") != "AWS":
                rotation_false_examples.append(attrs)
        
        # Generate report
        analyzer.generate_report(patterns, rotation_false_examples)
        
        # Optional: Export to CSV for further analysis
        if rotation_false_examples:
            df = pd.DataFrame(rotation_false_examples)
            csv_filename = f"kms_rotation_false_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            df.to_csv(csv_filename, index=False)
            print(f"\nDetailed data exported to: {csv_filename}")
            
            # Also export full JSON for deep analysis
            json_filename = f"kms_rotation_false_full_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            with open(json_filename, 'w') as f:
                json.dump(rotation_false_examples[:100], f, indent=2, default=str)  # Limit to 100 for file size
            print(f"Full JSON data exported to: {json_filename}")
        
    except Exception as e:
        print(f"\nError during analysis: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
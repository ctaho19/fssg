#!/usr/bin/env python3
"""
Certificate Discovery Analyzer

This script analyzes ALL ACM certificates using the CloudRadar API to identify
Amazon, DigiCert, AWS Private CA, and other certificate types. Based on the
CTRL-1077188 pipeline patterns.

Usage:
    python certificate_discovery_analyzer.py [options]

Options:
    --output-json FILE    Export detailed results to JSON file
    --output-csv FILE     Export summary results to CSV file
    --catalog-csv FILE    Path to CSV file containing certificate ARNs from Snowflake
    --compare-catalog     Compare API results with catalog data (requires --catalog-csv)
    --verbose             Enable verbose logging
    --dry-run             Analyze without making API calls (for testing)

Examples:
    # Basic discovery
    python certificate_discovery_analyzer.py

    # Discovery with catalog comparison
    python certificate_discovery_analyzer.py --catalog-csv snowflake_certificates.csv --compare-catalog

    # Full analysis with exports
    python certificate_discovery_analyzer.py \\
        --catalog-csv snowflake_certificates.csv \\
        --compare-catalog \\
        --output-json cert_analysis.json \\
        --output-csv cert_summary.csv \\
        --verbose
"""

import json
import csv
import time
import argparse
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Set, Optional, Tuple
import pandas as pd
import logging
import requests
from dataclasses import dataclass, asdict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class CertificateInfo:
    """Data structure for certificate information"""
    arn: str
    certificate_id: str
    issuer: str
    issuer_category: str
    status: str
    domain_name: str
    subject_alternative_names: List[str]
    not_before: Optional[datetime]
    not_after: Optional[datetime]
    in_use_by: List[str]
    is_in_use: bool
    source: str
    region: str
    account_id: str
    
    # Analysis fields
    days_until_expiry: Optional[int] = None
    is_expired: bool = False
    expiry_category: str = "Unknown"
    
    def __post_init__(self):
        """Calculate derived fields after initialization"""
        if self.not_after:
            now = datetime.now()
            self.days_until_expiry = (self.not_after - now).days
            self.is_expired = self.not_after < now
            
            if self.is_expired:
                self.expiry_category = "Expired"
            elif self.days_until_expiry <= 30:
                self.expiry_category = "Expiring_30_days"
            elif self.days_until_expiry <= 90:
                self.expiry_category = "Expiring_90_days"
            elif self.days_until_expiry <= 365:
                self.expiry_category = "Expiring_1_year"
            else:
                self.expiry_category = "Long_term_valid"

class CertificateDiscoveryAnalyzer:
    """Main analyzer class following CTRL-1077188 patterns"""
    
    def __init__(self, auth_token: str):
        self.auth_token = auth_token
        self.api_url = "https://api.cloud.capitalone.com/internal-operations/cloud-service/aws-tooling/search-resource-configurations"
        self.certificates: List[CertificateInfo] = []
        self.catalog_arns: Set[str] = set()
        self.headers = {
            'Accept': 'application/json;v=1',
            'Authorization': f'Bearer {auth_token}',
            'Content-Type': 'application/json'
        }
        
    def _make_api_request(self, payload: Dict, timeout: int = 120, max_retries: int = 3) -> requests.Response:
        """Make API request with bearer token authentication"""
        for retry in range(max_retries + 1):
            try:
                response = requests.post(
                    self.api_url,
                    headers=self.headers,
                    json=payload,
                    verify=False,  # Set to True in production with proper cert management
                    timeout=timeout
                )
                
                if response.status_code == 200:
                    return response
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
        
        raise RuntimeError("API request failed after all retry attempts")
    
    def _categorize_issuer(self, issuer: str) -> str:
        """Categorize certificate issuer into standard types"""
        if not issuer:
            return "Unknown"
        
        issuer_lower = issuer.lower()
        
        # Amazon certificates
        if "amazon" in issuer_lower:
            return "Amazon"
        
        # DigiCert certificates
        if "digicert" in issuer_lower:
            return "DigiCert"
        
        # AWS Private CA
        if any(term in issuer_lower for term in ["private ca", "privateca", "aws certificate authority"]):
            return "AWS_Private_CA"
        
        # Let's Encrypt
        if "let's encrypt" in issuer_lower or "letsencrypt" in issuer_lower:
            return "LetsEncrypt"
        
        # Sectigo (formerly Comodo)
        if any(term in issuer_lower for term in ["sectigo", "comodo"]):
            return "Sectigo"
        
        # GoDaddy
        if "godaddy" in issuer_lower:
            return "GoDaddy"
        
        # GlobalSign
        if "globalsign" in issuer_lower:
            return "GlobalSign"
        
        # Entrust
        if "entrust" in issuer_lower:
            return "Entrust"
        
        # Symantec/VeriSign (legacy)
        if any(term in issuer_lower for term in ["symantec", "verisign"]):
            return "Symantec_VeriSign"
        
        return "Other"
    
    def _parse_certificate_arn(self, arn: str) -> Tuple[str, str]:
        """Extract region and account ID from certificate ARN"""
        try:
            # Format: arn:aws:acm:region:account-id:certificate/certificate-id
            parts = arn.split(":")
            if len(parts) >= 5:
                region = parts[3]
                account_id = parts[4]
                return region, account_id
        except Exception:
            pass
        return "unknown", "unknown"
    
    def _parse_datetime(self, date_string: str) -> Optional[datetime]:
        """Parse datetime string from API response"""
        if not date_string:
            return None
        
        try:
            # Try common datetime formats
            for fmt in [
                "%Y-%m-%dT%H:%M:%S.%fZ",
                "%Y-%m-%dT%H:%M:%SZ", 
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d"
            ]:
                try:
                    return datetime.strptime(date_string, fmt)
                except ValueError:
                    continue
        except Exception as e:
            logger.warning(f"Failed to parse datetime '{date_string}': {e}")
        
        return None
    
    def _extract_certificate_info(self, resource: Dict) -> CertificateInfo:
        """Extract certificate information from API resource"""
        arn = resource.get("amazonResourceName", "")
        certificate_id = arn.split("/")[-1] if "/" in arn else ""
        region, account_id = self._parse_certificate_arn(arn)
        
        # Extract configuration values
        config_map = {}
        for config in resource.get("configurationList", []):
            config_name = config.get("configurationName")
            config_value = config.get("configurationValue")
            if config_name:
                config_map[config_name] = config_value
        
        # Parse issuer and categorize
        issuer = config_map.get("issuer", "")
        issuer_category = self._categorize_issuer(issuer)
        
        # Parse in-use information
        in_use_by_raw = config_map.get("inUseBy", "[]")
        try:
            in_use_by = json.loads(in_use_by_raw) if in_use_by_raw != "[]" else []
        except json.JSONDecodeError:
            in_use_by = []
        
        # Parse subject alternative names
        sans_raw = config_map.get("subjectAlternativeNames", "[]")
        try:
            sans = json.loads(sans_raw) if sans_raw != "[]" else []
        except json.JSONDecodeError:
            sans = []
        
        # Parse dates
        not_before = self._parse_datetime(config_map.get("notBefore"))
        not_after = self._parse_datetime(config_map.get("notAfter"))
        
        return CertificateInfo(
            arn=arn,
            certificate_id=certificate_id,
            issuer=issuer,
            issuer_category=issuer_category,
            status=config_map.get("status", ""),
            domain_name=config_map.get("domainName", ""),
            subject_alternative_names=sans,
            not_before=not_before,
            not_after=not_after,
            in_use_by=in_use_by,
            is_in_use=len(in_use_by) > 0,
            source=resource.get("source", ""),
            region=region,
            account_id=account_id
        )
    
    def discover_all_certificates(self, dry_run: bool = False) -> List[CertificateInfo]:
        """
        Discover ALL ACM certificates using CloudRadar API
        
        Args:
            dry_run: If True, return mock data instead of making API calls
            
        Returns:
            List of CertificateInfo objects
        """
        if dry_run:
            logger.info("DRY RUN: Returning mock certificate data")
            return self._generate_mock_data()
        
        logger.info("Starting certificate discovery via CloudRadar API...")
        
        all_resources = []
        next_record_key = None
        page_count = 0
        
        # API payload for ALL ACM certificates (no issuer filter)
        api_payload = {
            "searchParameters": [
                {
                    "resourceType": "AWS::ACM::Certificate"
                    # NOTE: No configurationItems filter - get ALL certificates
                }
            ]
        }
        
        try:
            while True:
                page_count += 1
                logger.info(f"Fetching page {page_count}...")
                
                payload = api_payload.copy()
                if next_record_key:
                    payload["nextRecordKey"] = next_record_key
                
                response = self._make_api_request(payload)
                
                data = response.json()
                resources = data.get("resourceConfigurations", [])
                all_resources.extend(resources)
                
                logger.info(f"Page {page_count}: Found {len(resources)} certificates")
                
                # Handle pagination
                next_record_key = data.get("nextRecordKey")
                if not next_record_key:
                    break
                
                # Rate limiting
                time.sleep(0.15)
                
        except Exception as e:
            logger.error(f"Failed to fetch certificates from API: {str(e)}")
            raise
        
        logger.info(f"Discovery complete: Found {len(all_resources)} total certificates across {page_count} pages")
        
        # Process and filter certificates
        processed_certificates = []
        for resource in all_resources:
            try:
                # Skip orphaned certificates
                if resource.get('source') == 'CT-AccessDenied':
                    continue
                
                cert_info = self._extract_certificate_info(resource)
                
                # Skip certificates in pending states
                if cert_info.status in ['PENDING_DELETION', 'PENDING_IMPORT']:
                    continue
                
                processed_certificates.append(cert_info)
                
            except Exception as e:
                logger.warning(f"Failed to process certificate: {e}")
                continue
        
        logger.info(f"Processed {len(processed_certificates)} valid certificates")
        self.certificates = processed_certificates
        return processed_certificates
    
    def load_catalog_arns(self, csv_file_path: str) -> Set[str]:
        """
        Load certificate ARNs from Snowflake catalog CSV file
        
        Args:
            csv_file_path: Path to CSV file containing certificate ARNs
            
        Returns:
            Set of certificate ARNs from catalog
        """
        logger.info(f"Loading certificate ARNs from catalog CSV: {csv_file_path}")
        
        catalog_arns = set()
        try:
            # Try reading with pandas first (handles various CSV formats)
            df = pd.read_csv(csv_file_path)
            
            # Look for common column names that might contain ARNs
            arn_columns = [col for col in df.columns.str.lower() 
                          if any(term in col for term in ['arn', 'certificate_arn', 'cert_arn'])]
            
            if not arn_columns:
                # If no ARN columns found, assume first column contains ARNs
                arn_column = df.columns[0]
                logger.warning(f"No ARN column detected, using first column: {arn_column}")
            else:
                arn_column = arn_columns[0]
                logger.info(f"Using ARN column: {arn_column}")
            
            # Extract ARNs and normalize
            for arn in df[arn_column].dropna():
                arn_str = str(arn).strip()
                if arn_str and "arn:aws:acm" in arn_str:
                    catalog_arns.add(arn_str.lower())
            
            logger.info(f"Loaded {len(catalog_arns)} ARNs from catalog CSV")
            
        except Exception as e:
            logger.error(f"Failed to load catalog CSV: {e}")
            raise
        
        self.catalog_arns = catalog_arns
        return catalog_arns
    
    def compare_with_catalog(self) -> Dict:
        """
        Compare discovered certificates with catalog data
        
        Returns:
            Dictionary containing comparison statistics
        """
        if not self.catalog_arns:
            logger.warning("No catalog ARNs loaded for comparison")
            return {}
        
        logger.info("Comparing API discoveries with catalog data...")
        
        # Get API ARNs
        api_arns = {cert.arn.lower() for cert in self.certificates}
        
        # Calculate overlaps and differences
        both_sources = api_arns.intersection(self.catalog_arns)
        api_only = api_arns - self.catalog_arns
        catalog_only = self.catalog_arns - api_arns
        
        # Calculate coverage percentages
        api_coverage = (len(both_sources) / len(self.catalog_arns) * 100) if self.catalog_arns else 0
        catalog_coverage = (len(both_sources) / len(api_arns) * 100) if api_arns else 0
        
        comparison_stats = {
            "api_certificates": len(api_arns),
            "catalog_certificates": len(self.catalog_arns),
            "common_certificates": len(both_sources),
            "api_only_certificates": len(api_only),
            "catalog_only_certificates": len(catalog_only),
            "api_coverage_of_catalog_percent": round(api_coverage, 2),
            "catalog_coverage_of_api_percent": round(catalog_coverage, 2),
            "api_only_arns": sorted(list(api_only)),
            "catalog_only_arns": sorted(list(catalog_only))
        }
        
        logger.info(f"Comparison complete:")
        logger.info(f"  API certificates: {len(api_arns)}")
        logger.info(f"  Catalog certificates: {len(self.catalog_arns)}")
        logger.info(f"  Common certificates: {len(both_sources)}")
        logger.info(f"  API coverage of catalog: {api_coverage:.1f}%")
        logger.info(f"  Catalog coverage of API: {catalog_coverage:.1f}%")
        
        return comparison_stats
    
    def generate_analysis_report(self, include_catalog_comparison: bool = False) -> Dict:
        """
        Generate comprehensive analysis report
        
        Args:
            include_catalog_comparison: Whether to include catalog comparison
            
        Returns:
            Dictionary containing analysis results
        """
        logger.info("Generating analysis report...")
        
        if not self.certificates:
            logger.warning("No certificates to analyze")
            return {}
        
        # Basic statistics
        total_certs = len(self.certificates)
        
        # Issuer category analysis
        issuer_stats = {}
        for cert in self.certificates:
            category = cert.issuer_category
            if category not in issuer_stats:
                issuer_stats[category] = {
                    "count": 0,
                    "in_use": 0,
                    "expired": 0,
                    "expiring_30_days": 0,
                    "expiring_90_days": 0
                }
            
            stats = issuer_stats[category]
            stats["count"] += 1
            if cert.is_in_use:
                stats["in_use"] += 1
            if cert.is_expired:
                stats["expired"] += 1
            elif cert.days_until_expiry and cert.days_until_expiry <= 30:
                stats["expiring_30_days"] += 1
            elif cert.days_until_expiry and cert.days_until_expiry <= 90:
                stats["expiring_90_days"] += 1
        
        # Usage analysis
        in_use_count = sum(1 for cert in self.certificates if cert.is_in_use)
        unused_count = total_certs - in_use_count
        
        # Expiration analysis
        expired_count = sum(1 for cert in self.certificates if cert.is_expired)
        expiring_30_count = sum(1 for cert in self.certificates 
                               if cert.days_until_expiry and 0 <= cert.days_until_expiry <= 30)
        expiring_90_count = sum(1 for cert in self.certificates 
                               if cert.days_until_expiry and 0 <= cert.days_until_expiry <= 90)
        
        # Status analysis
        status_stats = {}
        for cert in self.certificates:
            status = cert.status or "Unknown"
            status_stats[status] = status_stats.get(status, 0) + 1
        
        # Region analysis
        region_stats = {}
        for cert in self.certificates:
            region = cert.region
            region_stats[region] = region_stats.get(region, 0) + 1
        
        report = {
            "analysis_timestamp": datetime.now().isoformat(),
            "summary": {
                "total_certificates": total_certs,
                "in_use_certificates": in_use_count,
                "unused_certificates": unused_count,
                "expired_certificates": expired_count,
                "expiring_30_days": expiring_30_count,
                "expiring_90_days": expiring_90_count,
                "usage_percentage": round((in_use_count / total_certs * 100), 2) if total_certs > 0 else 0
            },
            "issuer_analysis": issuer_stats,
            "status_distribution": status_stats,
            "region_distribution": region_stats,
            "certificates": [asdict(cert) for cert in self.certificates]
        }
        
        # Add catalog comparison if requested
        if include_catalog_comparison:
            comparison_stats = self.compare_with_catalog()
            report["catalog_comparison"] = comparison_stats
        
        logger.info("Analysis report generated successfully")
        return report
    
    def export_to_json(self, file_path: str, report: Dict) -> None:
        """Export analysis report to JSON file"""
        logger.info(f"Exporting analysis to JSON: {file_path}")
        
        with open(file_path, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        logger.info(f"JSON export complete: {file_path}")
    
    def export_to_csv(self, file_path: str) -> None:
        """Export certificate summary to CSV file"""
        logger.info(f"Exporting certificate summary to CSV: {file_path}")
        
        if not self.certificates:
            logger.warning("No certificates to export")
            return
        
        # Prepare CSV data
        csv_data = []
        for cert in self.certificates:
            csv_data.append({
                "certificate_arn": cert.arn,
                "certificate_id": cert.certificate_id,
                "issuer": cert.issuer,
                "issuer_category": cert.issuer_category,
                "status": cert.status,
                "domain_name": cert.domain_name,
                "is_in_use": cert.is_in_use,
                "in_use_by_count": len(cert.in_use_by),
                "region": cert.region,
                "account_id": cert.account_id,
                "not_before": cert.not_before.isoformat() if cert.not_before else "",
                "not_after": cert.not_after.isoformat() if cert.not_after else "",
                "days_until_expiry": cert.days_until_expiry,
                "is_expired": cert.is_expired,
                "expiry_category": cert.expiry_category,
                "source": cert.source,
                "subject_alternative_names": "; ".join(cert.subject_alternative_names),
                "in_use_by": "; ".join(cert.in_use_by)
            })
        
        # Write CSV
        df = pd.DataFrame(csv_data)
        df.to_csv(file_path, index=False)
        
        logger.info(f"CSV export complete: {file_path} ({len(csv_data)} certificates)")
    
    def print_summary_report(self, report: Dict) -> None:
        """Print a summary report to console"""
        if not report:
            print("No analysis data available")
            return
        
        summary = report.get("summary", {})
        issuer_analysis = report.get("issuer_analysis", {})
        
        print("\n" + "="*60)
        print("CERTIFICATE DISCOVERY ANALYSIS REPORT")
        print("="*60)
        
        # Summary section
        print(f"\n📊 SUMMARY")
        print(f"Total Certificates: {summary.get('total_certificates', 0):,}")
        print(f"In Use: {summary.get('in_use_certificates', 0):,} ({summary.get('usage_percentage', 0):.1f}%)")
        print(f"Unused: {summary.get('unused_certificates', 0):,}")
        print(f"Expired: {summary.get('expired_certificates', 0):,}")
        print(f"Expiring (30 days): {summary.get('expiring_30_days', 0):,}")
        print(f"Expiring (90 days): {summary.get('expiring_90_days', 0):,}")
        
        # Issuer analysis
        print(f"\n🏢 ISSUER ANALYSIS")
        for issuer_category, stats in sorted(issuer_analysis.items()):
            count = stats.get('count', 0)
            in_use = stats.get('in_use', 0)
            expired = stats.get('expired', 0)
            usage_pct = (in_use / count * 100) if count > 0 else 0
            
            print(f"{issuer_category:20s}: {count:,} certs ({in_use:,} in use, {usage_pct:.1f}%, {expired:,} expired)")
        
        # Status distribution
        status_dist = report.get("status_distribution", {})
        if status_dist:
            print(f"\n📋 STATUS DISTRIBUTION")
            for status, count in sorted(status_dist.items()):
                print(f"{status:20s}: {count:,}")
        
        # Region distribution (top 10)
        region_dist = report.get("region_distribution", {})
        if region_dist:
            print(f"\n🌍 TOP REGIONS")
            sorted_regions = sorted(region_dist.items(), key=lambda x: x[1], reverse=True)
            for region, count in sorted_regions[:10]:
                print(f"{region:20s}: {count:,}")
        
        # Catalog comparison
        catalog_comparison = report.get("catalog_comparison")
        if catalog_comparison:
            print(f"\n🔍 CATALOG COMPARISON")
            print(f"API Certificates: {catalog_comparison.get('api_certificates', 0):,}")
            print(f"Catalog Certificates: {catalog_comparison.get('catalog_certificates', 0):,}")
            print(f"Common Certificates: {catalog_comparison.get('common_certificates', 0):,}")
            print(f"API Coverage of Catalog: {catalog_comparison.get('api_coverage_of_catalog_percent', 0):.1f}%")
            print(f"Catalog Coverage of API: {catalog_comparison.get('catalog_coverage_of_api_percent', 0):.1f}%")
            
            api_only = catalog_comparison.get('api_only_certificates', 0)
            catalog_only = catalog_comparison.get('catalog_only_certificates', 0)
            if api_only > 0:
                print(f"API-Only Certificates: {api_only:,}")
            if catalog_only > 0:
                print(f"Catalog-Only Certificates: {catalog_only:,}")
        
        print("\n" + "="*60)
    
    def _generate_mock_data(self) -> List[CertificateInfo]:
        """Generate mock certificate data for testing"""
        mock_certs = [
            CertificateInfo(
                arn="arn:aws:acm:us-east-1:123456789012:certificate/12345678-1234-1234-1234-123456789012",
                certificate_id="12345678-1234-1234-1234-123456789012",
                issuer="Amazon",
                issuer_category="Amazon",
                status="ISSUED",
                domain_name="example.com",
                subject_alternative_names=["www.example.com", "api.example.com"],
                not_before=datetime.now() - timedelta(days=30),
                not_after=datetime.now() + timedelta(days=335),
                in_use_by=["arn:aws:elasticloadbalancing:us-east-1:123456789012:loadbalancer/app/test/abcd1234"],
                is_in_use=True,
                source="ConfigurationSnapshot",
                region="us-east-1",
                account_id="123456789012"
            ),
            CertificateInfo(
                arn="arn:aws:acm:us-west-2:123456789012:certificate/87654321-4321-4321-4321-210987654321",
                certificate_id="87654321-4321-4321-4321-210987654321",
                issuer="DigiCert Inc",
                issuer_category="DigiCert",
                status="ISSUED",
                domain_name="secure.example.com",
                subject_alternative_names=["secure.example.com"],
                not_before=datetime.now() - timedelta(days=60),
                not_after=datetime.now() + timedelta(days=15),
                in_use_by=[],
                is_in_use=False,
                source="ConfigurationSnapshot",
                region="us-west-2",
                account_id="123456789012"
            )
        ]
        
        logger.info(f"Generated {len(mock_certs)} mock certificates")
        return mock_certs

    def analyze_specific_certificates(self, test_arns: List[str]) -> Dict:
        """
        Analyze specific certificate ARNs for detailed inspection
        
        PASTE YOUR EXAMPLE ARNs HERE FOR TESTING:
        
        # DigiCert example ARN - replace with your real ARN:
        # arn:aws:acm:region:account:certificate/PASTE_DIGICERT_ARN_HERE
        
        # AWS Private CA example ARN - replace with your real ARN:
        # arn:aws:acm:region:account:certificate/PASTE_PRIVATE_CA_ARN_HERE
        
        Args:
            test_arns: List of specific certificate ARNs to analyze
            
        Returns:
            Dictionary with detailed analysis of these specific certificates
        """
        if not test_arns:
            logger.info("No specific ARNs provided for analysis")
            return {}
        
        logger.info(f"Analyzing {len(test_arns)} specific certificate ARNs...")
        
        # Find these specific certificates in our discovered set
        target_certificates = []
        for cert in self.certificates:
            if cert.arn.lower() in [arn.lower() for arn in test_arns]:
                target_certificates.append(cert)
        
        if not target_certificates:
            logger.warning("None of the specified ARNs were found in the discovered certificates")
            return {"found_certificates": [], "analysis": "No matching certificates found"}
        
        logger.info(f"Found {len(target_certificates)} matching certificates")
        
        # Detailed analysis of these specific certificates
        analysis = {
            "found_certificates": len(target_certificates),
            "requested_arns": test_arns,
            "detailed_analysis": []
        }
        
        for cert in target_certificates:
            cert_analysis = {
                "arn": cert.arn,
                "certificate_id": cert.certificate_id,
                "issuer": cert.issuer,
                "issuer_category": cert.issuer_category,
                "status": cert.status,
                "domain_name": cert.domain_name,
                "subject_alternative_names": cert.subject_alternative_names,
                "region": cert.region,
                "account_id": cert.account_id,
                "is_in_use": cert.is_in_use,
                "in_use_by": cert.in_use_by,
                "in_use_by_count": len(cert.in_use_by),
                "source": cert.source,
                "not_before": cert.not_before.isoformat() if cert.not_before else None,
                "not_after": cert.not_after.isoformat() if cert.not_after else None,
                "days_until_expiry": cert.days_until_expiry,
                "is_expired": cert.is_expired,
                "expiry_category": cert.expiry_category,
                
                # Analysis insights
                "scope_recommendation": self._get_scope_recommendation(cert),
                "monitoring_priority": self._get_monitoring_priority(cert),
                "risk_assessment": self._get_risk_assessment(cert)
            }
            
            analysis["detailed_analysis"].append(cert_analysis)
        
        return analysis
    
    def _get_scope_recommendation(self, cert: CertificateInfo) -> str:
        """Provide scope recommendation for a certificate"""
        if cert.issuer_category == "Amazon":
            return "ALREADY_IN_SCOPE - Amazon certificates currently monitored"
        elif cert.issuer_category == "DigiCert":
            if cert.is_in_use:
                return "RECOMMENDED_FOR_SCOPE - DigiCert cert in active use"
            else:
                return "CONSIDER_FOR_SCOPE - DigiCert cert not currently in use"
        elif cert.issuer_category == "AWS_Private_CA":
            if cert.is_in_use:
                return "RECOMMENDED_FOR_SCOPE - Private CA cert in active use"
            else:
                return "CONSIDER_FOR_SCOPE - Private CA cert not currently in use"
        elif cert.issuer_category == "LetsEncrypt":
            return "EVALUATE_NEEDED - Let's Encrypt certificates may be short-lived"
        else:
            return f"EVALUATE_NEEDED - {cert.issuer_category} certificates need assessment"
    
    def _get_monitoring_priority(self, cert: CertificateInfo) -> str:
        """Determine monitoring priority"""
        if cert.is_expired:
            return "CRITICAL - Certificate has expired"
        elif cert.days_until_expiry and cert.days_until_expiry <= 30:
            return "HIGH - Expires within 30 days"
        elif cert.days_until_expiry and cert.days_until_expiry <= 90:
            return "MEDIUM - Expires within 90 days"
        elif cert.is_in_use:
            return "MEDIUM - Certificate is in active use"
        else:
            return "LOW - Certificate not in use"
    
    def _get_risk_assessment(self, cert: CertificateInfo) -> str:
        """Assess certificate risk"""
        risks = []
        
        if cert.is_expired and cert.is_in_use:
            risks.append("EXPIRED_IN_USE")
        elif cert.is_expired:
            risks.append("EXPIRED_UNUSED")
            
        if cert.days_until_expiry and cert.days_until_expiry <= 30 and cert.is_in_use:
            risks.append("EXPIRING_SOON_IN_USE")
            
        if not cert.is_in_use and cert.days_until_expiry and cert.days_until_expiry > 30:
            risks.append("UNUSED_VALID")
            
        if cert.issuer_category == "Other":
            risks.append("UNKNOWN_ISSUER")
            
        return "; ".join(risks) if risks else "LOW_RISK"

    def analyze_certificate_metadata_by_type(self) -> Dict:
        """
        Analyze certificate metadata structure by issuer type for pipeline development
        
        REFERENCE EXAMPLE ARNs FOR DETAILED ANALYSIS:
        
        # DigiCert example ARN - PASTE YOUR REAL ARN HERE:
        # digicert_example_arn = "arn:aws:acm:region:account:certificate/PASTE_DIGICERT_ARN_HERE"
        
        # AWS Private CA example ARN - PASTE YOUR REAL ARN HERE:
        # pca_example_arn = "arn:aws:acm:region:account:certificate/PASTE_PCA_ARN_HERE"
        
        Returns:
            Dictionary containing detailed metadata analysis by certificate type
        """
        if not self.certificates:
            logger.warning("No certificates available for metadata analysis")
            return {}
        
        logger.info("Analyzing certificate metadata structure by issuer type...")
        
        # Group certificates by issuer category
        certificates_by_type = {}
        for cert in self.certificates:
            cert_type = cert.issuer_category
            if cert_type not in certificates_by_type:
                certificates_by_type[cert_type] = []
            certificates_by_type[cert_type].append(cert)
        
        metadata_analysis = {
            "analysis_timestamp": datetime.now().isoformat(),
            "total_certificates_analyzed": len(self.certificates),
            "certificate_type_summary": {},
            "detailed_metadata_analysis": {},
            "pipeline_integration_recommendations": {},
            "reference_examples": {}
        }
        
        # Analyze each certificate type
        for cert_type, certs in certificates_by_type.items():
            logger.info(f"Analyzing {len(certs)} {cert_type} certificates...")
            
            # Basic statistics
            metadata_analysis["certificate_type_summary"][cert_type] = {
                "count": len(certs),
                "percentage": round(len(certs) / len(self.certificates) * 100, 1),
                "in_use_count": sum(1 for cert in certs if cert.is_in_use),
                "usage_percentage": round(sum(1 for cert in certs if cert.is_in_use) / len(certs) * 100, 1)
            }
            
            # Detailed metadata structure analysis
            metadata_analysis["detailed_metadata_analysis"][cert_type] = self._analyze_certificate_type_metadata(certs)
            
            # Pipeline integration recommendations
            metadata_analysis["pipeline_integration_recommendations"][cert_type] = self._generate_pipeline_recommendations(cert_type, certs)
            
            # Find reference examples (first few certificates of this type)
            examples = []
            for cert in certs[:3]:  # Get up to 3 examples
                examples.append({
                    "arn": cert.arn,
                    "issuer": cert.issuer,
                    "domain_name": cert.domain_name,
                    "is_in_use": cert.is_in_use,
                    "days_until_expiry": cert.days_until_expiry
                })
            metadata_analysis["reference_examples"][cert_type] = examples
        
        return metadata_analysis
    
    def _analyze_certificate_type_metadata(self, certs: List[CertificateInfo]) -> Dict:
        """Analyze metadata structure for a specific certificate type"""
        # This would need access to the raw API response data
        # For now, we'll analyze what we can from the CertificateInfo objects
        
        metadata_fields = {
            "issuer_patterns": {},
            "domain_patterns": {},
            "usage_patterns": {},
            "expiration_patterns": {},
            "availability_analysis": {}
        }
        
        # Analyze issuer patterns
        issuers = [cert.issuer for cert in certs if cert.issuer]
        issuer_count = {}
        for issuer in issuers:
            issuer_count[issuer] = issuer_count.get(issuer, 0) + 1
        metadata_fields["issuer_patterns"] = dict(sorted(issuer_count.items(), key=lambda x: x[1], reverse=True)[:10])
        
        # Analyze domain patterns
        domains = [cert.domain_name for cert in certs if cert.domain_name]
        domain_suffixes = {}
        for domain in domains:
            if '.' in domain:
                suffix = '.' + domain.split('.')[-1]
                domain_suffixes[suffix] = domain_suffixes.get(suffix, 0) + 1
        metadata_fields["domain_patterns"] = dict(sorted(domain_suffixes.items(), key=lambda x: x[1], reverse=True)[:10])
        
        # Analyze usage patterns
        in_use_count = sum(1 for cert in certs if cert.is_in_use)
        metadata_fields["usage_patterns"] = {
            "total_certificates": len(certs),
            "in_use": in_use_count,
            "unused": len(certs) - in_use_count,
            "usage_rate": round(in_use_count / len(certs) * 100, 1) if certs else 0
        }
        
        # Analyze expiration patterns
        expiry_categories = {}
        for cert in certs:
            category = cert.expiry_category or "Unknown"
            expiry_categories[category] = expiry_categories.get(category, 0) + 1
        metadata_fields["expiration_patterns"] = expiry_categories
        
        # Field availability analysis
        metadata_fields["availability_analysis"] = {
            "issuer_present": sum(1 for cert in certs if cert.issuer) / len(certs) * 100,
            "domain_present": sum(1 for cert in certs if cert.domain_name) / len(certs) * 100,
            "expiry_date_present": sum(1 for cert in certs if cert.not_after) / len(certs) * 100,
            "sans_present": sum(1 for cert in certs if cert.subject_alternative_names) / len(certs) * 100,
            "usage_info_present": sum(1 for cert in certs if cert.in_use_by) / len(certs) * 100
        }
        
        return metadata_fields
    
    def _generate_pipeline_recommendations(self, cert_type: str, certs: List[CertificateInfo]) -> Dict:
        """Generate pipeline integration recommendations for a certificate type"""
        recommendations = {
            "api_filter_criteria": {},
            "identification_logic": {},
            "monitoring_considerations": {},
            "comparison_fields": {}
        }
        
        if cert_type == "Amazon":
            recommendations["api_filter_criteria"] = {
                "current_filter": "configurationItems: [{configurationName: 'issuer', configurationValue: 'Amazon'}]",
                "reliability": "High - Amazon certificates consistently have 'Amazon' in issuer field"
            }
            recommendations["identification_logic"] = {
                "primary": "issuer contains 'Amazon'",
                "backup": "Check for Amazon-specific certificate characteristics"
            }
            recommendations["monitoring_considerations"] = {
                "strengths": ["Consistent metadata structure", "Good tag usage for ASV/BA identification"],
                "limitations": ["May miss certificates issued by other Amazon CAs"]
            }
            
        elif cert_type == "DigiCert":
            recommendations["api_filter_criteria"] = {
                "suggested_filter": "configurationItems: [{configurationName: 'issuer', configurationValue: 'DigiCert'}]",
                "reliability": "High - DigiCert certificates have distinctive issuer patterns"
            }
            recommendations["identification_logic"] = {
                "primary": "issuer contains 'DigiCert'",
                "patterns": [issuer for issuer in set(cert.issuer for cert in certs[:5] if cert.issuer)]
            }
            recommendations["monitoring_considerations"] = {
                "strengths": ["Clear issuer identification", "Commercial certificate standards"],
                "limitations": ["May have different metadata structure than Amazon certificates"]
            }
            
        elif cert_type == "AWS_Private_CA":
            recommendations["api_filter_criteria"] = {
                "suggested_approach": "Use negative filtering (exclude Amazon and DigiCert) or search for Private CA patterns",
                "reliability": "Medium - Requires custom identification logic"
            }
            recommendations["identification_logic"] = {
                "primary": "Check for Private CA patterns in issuer",
                "secondary": "Look for certificateAuthorityArn in supplementaryConfiguration"
            }
            recommendations["monitoring_considerations"] = {
                "strengths": ["Internal certificate control", "Custom CA policies"],
                "limitations": ["Variable issuer names", "May require custom parsing logic"]
            }
            
        else:
            recommendations["api_filter_criteria"] = {
                "approach": "Requires investigation of specific issuer patterns"
            }
            recommendations["identification_logic"] = {
                "primary": f"issuer contains specific {cert_type} patterns"
            }
        
        # Common comparison fields analysis
        sample_certs = certs[:10]  # Analyze first 10 certificates
        field_availability = {
            "issuer": sum(1 for cert in sample_certs if cert.issuer) / len(sample_certs) * 100,
            "status": sum(1 for cert in sample_certs if cert.status) / len(sample_certs) * 100,
            "domain_name": sum(1 for cert in sample_certs if cert.domain_name) / len(sample_certs) * 100,
            "in_use_by": sum(1 for cert in sample_certs if cert.in_use_by) / len(sample_certs) * 100,
            "expiry_dates": sum(1 for cert in sample_certs if cert.not_after) / len(sample_certs) * 100
        }
        
        recommendations["comparison_fields"] = {
            "reliable_fields": [field for field, availability in field_availability.items() if availability >= 90],
            "field_availability": field_availability,
            "recommended_for_pipeline": ["issuer", "status", "domain_name", "in_use_by", "not_after"]
        }
        
        return recommendations
    
    def print_metadata_analysis_report(self, analysis: Dict) -> None:
        """Print detailed metadata analysis report for pipeline development"""
        if not analysis:
            print("No metadata analysis data available")
            return
        
        print("\n" + "="*100)
        print("CERTIFICATE METADATA STRUCTURE ANALYSIS FOR PIPELINE DEVELOPMENT")
        print("="*100)
        print(f"Analysis Timestamp: {analysis.get('analysis_timestamp')}")
        print(f"Total Certificates Analyzed: {analysis.get('total_certificates_analyzed', 0):,}")
        
        # Certificate type summary
        print(f"\n📊 CERTIFICATE TYPE DISTRIBUTION")
        print("-" * 50)
        type_summary = analysis.get("certificate_type_summary", {})
        for cert_type, stats in sorted(type_summary.items(), key=lambda x: x[1]['count'], reverse=True):
            print(f"{cert_type:20s}: {stats['count']:,} certs ({stats['percentage']:.1f}%) - "
                  f"{stats['in_use_count']:,} in use ({stats['usage_percentage']:.1f}%)")
        
        # Detailed analysis for each type
        detailed_analysis = analysis.get("detailed_metadata_analysis", {})
        pipeline_recs = analysis.get("pipeline_integration_recommendations", {})
        
        for cert_type in ["Amazon", "DigiCert", "AWS_Private_CA", "LetsEncrypt", "Other"]:
            if cert_type not in detailed_analysis:
                continue
                
            if cert_type == "Amazon":
                icon = "🟢"
            elif cert_type == "DigiCert":
                icon = "🔒"
            elif cert_type == "AWS_Private_CA":
                icon = "🏢"
            elif cert_type == "LetsEncrypt":
                icon = "🆓"
            else:
                icon = "❓"
                
            print(f"\n{icon} {cert_type.upper()} CERTIFICATES")
            print("-" * 70)
            
            # Metadata structure
            metadata = detailed_analysis[cert_type]
            
            print("📋 Issuer Patterns:")
            for issuer, count in list(metadata.get("issuer_patterns", {}).items())[:3]:
                print(f"  • {issuer[:60]}... ({count} certificates)")
            
            print("🌐 Domain Patterns:")
            for domain, count in list(metadata.get("domain_patterns", {}).items())[:5]:
                print(f"  • {domain}: {count} certificates")
            
            print("📈 Usage Analysis:")
            usage = metadata.get("usage_patterns", {})
            print(f"  • Total: {usage.get('total_certificates', 0)} certificates")
            print(f"  • In Use: {usage.get('in_use', 0)} ({usage.get('usage_rate', 0):.1f}%)")
            print(f"  • Unused: {usage.get('unused', 0)}")
            
            print("⏰ Expiration Analysis:")
            for category, count in metadata.get("expiration_patterns", {}).items():
                print(f"  • {category}: {count} certificates")
            
            print("🔍 Field Availability:")
            availability = metadata.get("availability_analysis", {})
            for field, percentage in availability.items():
                status = "✓" if percentage >= 90 else "⚠" if percentage >= 50 else "✗"
                print(f"  {status} {field.replace('_', ' ').title()}: {percentage:.1f}%")
            
            # Pipeline recommendations
            if cert_type in pipeline_recs:
                recs = pipeline_recs[cert_type]
                print("🔧 Pipeline Integration:")
                
                if "api_filter_criteria" in recs:
                    filter_info = recs["api_filter_criteria"]
                    if "current_filter" in filter_info:
                        print(f"  • Current Filter: {filter_info['current_filter']}")
                    if "suggested_filter" in filter_info:
                        print(f"  • Suggested Filter: {filter_info['suggested_filter']}")
                    if "reliability" in filter_info:
                        print(f"  • Reliability: {filter_info['reliability']}")
                
                if "identification_logic" in recs:
                    id_logic = recs["identification_logic"]
                    if "primary" in id_logic:
                        print(f"  • Primary ID: {id_logic['primary']}")
                    if "patterns" in id_logic:
                        print(f"  • Issuer Patterns: {len(id_logic['patterns'])} unique patterns found")
                
                if "comparison_fields" in recs:
                    fields = recs["comparison_fields"]
                    reliable_fields = fields.get("reliable_fields", [])
                    print(f"  • Reliable Fields: {', '.join(reliable_fields)}")
        
        # Pipeline integration summary
        print(f"\n🔧 PIPELINE INTEGRATION RECOMMENDATIONS")
        print("="*70)
        
        print("1. API PAYLOAD MODIFICATIONS:")
        print("   • Remove current Amazon-only filter:")
        print("     OLD: \"configurationValue\": \"Amazon\"")
        print("   • Use broader search to capture all certificate types:")
        print("     NEW: Remove configurationItems filter entirely")
        
        print("\n2. CERTIFICATE TYPE IDENTIFICATION:")
        amazon_count = type_summary.get("Amazon", {}).get("count", 0)
        digicert_count = type_summary.get("DigiCert", {}).get("count", 0)
        pca_count = type_summary.get("AWS_Private_CA", {}).get("count", 0)
        
        print(f"   • Amazon ({amazon_count:,} certs): issuer contains 'Amazon'")
        print(f"   • DigiCert ({digicert_count:,} certs): issuer contains 'DigiCert'")
        print(f"   • AWS Private CA ({pca_count:,} certs): issuer contains 'Private CA' or custom patterns")
        
        print("\n3. SCOPE EXPANSION RECOMMENDATIONS:")
        total_non_amazon = sum(stats['count'] for cert_type, stats in type_summary.items() if cert_type != "Amazon")
        total_certs = analysis.get('total_certificates_analyzed', 0)
        expansion_percentage = (total_non_amazon / total_certs * 100) if total_certs > 0 else 0
        
        print(f"   • Current scope: {amazon_count:,} Amazon certificates")
        print(f"   • Potential expansion: {total_non_amazon:,} additional certificates ({expansion_percentage:.1f}% increase)")
        print(f"   • High priority: DigiCert certificates (commercial SSL)")
        print(f"   • Medium priority: AWS Private CA certificates (internal use)")
        
        print("\n4. IMPLEMENTATION PRIORITY:")
        if digicert_count > 0:
            digicert_usage = type_summary.get("DigiCert", {}).get("usage_percentage", 0)
            print(f"   🔒 DigiCert: {digicert_count:,} certificates ({digicert_usage:.1f}% in use) - RECOMMENDED")
        if pca_count > 0:
            pca_usage = type_summary.get("AWS_Private_CA", {}).get("usage_percentage", 0)
            print(f"   🏢 AWS Private CA: {pca_count:,} certificates ({pca_usage:.1f}% in use) - CONSIDER")
        
        other_types = [t for t in type_summary.keys() if t not in ["Amazon", "DigiCert", "AWS_Private_CA"]]
        if other_types:
            print(f"   ❓ Other types: {', '.join(other_types)} - EVALUATE INDIVIDUALLY")
        
        # Reference examples
        print(f"\n📋 REFERENCE EXAMPLES FOR TESTING")
        print("-" * 50)
        examples = analysis.get("reference_examples", {})
        for cert_type, type_examples in examples.items():
            if cert_type in ["DigiCert", "AWS_Private_CA"]:  # Focus on expansion targets
                print(f"\n{cert_type} Examples:")
                for i, example in enumerate(type_examples[:2], 1):  # Show 2 examples
                    print(f"  {i}. ARN: {example['arn']}")
                    print(f"     Issuer: {example['issuer']}")
                    print(f"     Domain: {example['domain_name']}")
                    print(f"     In Use: {example['is_in_use']}")
        
        print("\n" + "="*100)

def main():
    """Main function with command-line interface"""
    parser = argparse.ArgumentParser(
        description="Analyze all ACM certificates using CloudRadar API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument("--output-json", 
                       help="Export detailed results to JSON file")
    parser.add_argument("--output-csv", 
                       help="Export summary results to CSV file")
    parser.add_argument("--catalog-csv", 
                       help="Path to CSV file containing certificate ARNs from Snowflake")
    parser.add_argument("--compare-catalog", action="store_true",
                       help="Compare API results with catalog data (requires --catalog-csv)")
    parser.add_argument("--verbose", action="store_true",
                       help="Enable verbose logging")
    parser.add_argument("--dry-run", action="store_true",
                       help="Analyze without making API calls (for testing)")
    parser.add_argument("--analyze-specific", nargs="+",
                       help="Analyze specific certificate ARNs (paste your DigiCert/PCA examples here)")
    parser.add_argument("--analyze-metadata", action="store_true",
                       help="Analyze certificate metadata structure by type for pipeline development")
    
    args = parser.parse_args()
    
    # Configure logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        # Get authentication token from environment
        auth_token = os.environ.get("AUTH_TOKEN")
        if not auth_token:
            logger.error("AUTH_TOKEN environment variable not set.")
            logger.error("Usage: AUTH_TOKEN='your-bearer-token' python certificate_discovery_analyzer.py [options]")
            return 1
        
        # Create analyzer
        analyzer = CertificateDiscoveryAnalyzer(auth_token)
        
        # Load catalog data if provided
        if args.catalog_csv:
            if not Path(args.catalog_csv).exists():
                logger.error(f"Catalog CSV file not found: {args.catalog_csv}")
                return 1
            analyzer.load_catalog_arns(args.catalog_csv)
        
        # Discover certificates
        certificates = analyzer.discover_all_certificates(dry_run=args.dry_run)
        
        if not certificates:
            logger.warning("No certificates discovered")
            return 0
        
        # Perform metadata analysis if requested
        if args.analyze_metadata:
            logger.info("Performing certificate metadata analysis...")
            metadata_analysis = analyzer.analyze_certificate_metadata_by_type()
            
            # Print detailed metadata analysis report
            analyzer.print_metadata_analysis_report(metadata_analysis)
            
            # Add metadata analysis to the main report
            report = analyzer.generate_analysis_report(include_catalog_comparison=False)
            report["metadata_analysis"] = metadata_analysis
            
            # Export metadata analysis if JSON output requested
            if args.output_json:
                analyzer.export_to_json(args.output_json, report)
                logger.info(f"Metadata analysis exported to {args.output_json}")
            
            return 0
        
        # Generate standard analysis report
        include_comparison = args.compare_catalog and args.catalog_csv
        report = analyzer.generate_analysis_report(include_catalog_comparison=include_comparison)
        
        # Analyze specific certificates if requested
        if args.analyze_specific:
            logger.info(f"Analyzing {len(args.analyze_specific)} specific certificate ARNs...")
            specific_analysis = analyzer.analyze_specific_certificates(args.analyze_specific)
            
            if specific_analysis and specific_analysis.get("detailed_analysis"):
                print("\n" + "="*80)
                print("SPECIFIC CERTIFICATE ANALYSIS")
                print("="*80)
                
                for i, cert_analysis in enumerate(specific_analysis["detailed_analysis"], 1):
                    print(f"\n🔍 CERTIFICATE {i}:")
                    print(f"ARN: {cert_analysis['arn']}")
                    print(f"Issuer: {cert_analysis['issuer']}")
                    print(f"Category: {cert_analysis['issuer_category']}")
                    print(f"Status: {cert_analysis['status']}")
                    print(f"Domain: {cert_analysis['domain_name']}")
                    print(f"In Use: {cert_analysis['is_in_use']} ({cert_analysis['in_use_by_count']} resources)")
                    print(f"Expires: {cert_analysis['not_after']} ({cert_analysis['days_until_expiry']} days)")
                    print(f"Scope Recommendation: {cert_analysis['scope_recommendation']}")
                    print(f"Monitoring Priority: {cert_analysis['monitoring_priority']}")
                    print(f"Risk Assessment: {cert_analysis['risk_assessment']}")
                    
                    if cert_analysis['subject_alternative_names']:
                        print(f"SANs: {', '.join(cert_analysis['subject_alternative_names'])}")
                    
                    if cert_analysis['in_use_by']:
                        print(f"Used by: {cert_analysis['in_use_by'][:2]}...")  # Show first 2 resources
                
                # Add specific analysis to report
                report["specific_certificate_analysis"] = specific_analysis
            else:
                logger.warning("No matching certificates found for the specified ARNs")
        
        # Print summary
        analyzer.print_summary_report(report)
        
        # Export results
        if args.output_json:
            analyzer.export_to_json(args.output_json, report)
        
        if args.output_csv:
            analyzer.export_to_csv(args.output_csv)
        
        logger.info("Certificate discovery analysis complete!")
        return 0
        
    except KeyboardInterrupt:
        logger.info("Analysis interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"Analysis failed: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit(main())
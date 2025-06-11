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
        
        # Generate analysis report
        include_comparison = args.compare_catalog and args.catalog_csv
        report = analyzer.generate_analysis_report(include_catalog_comparison=include_comparison)
        
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
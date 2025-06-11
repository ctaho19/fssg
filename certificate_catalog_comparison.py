#!/usr/bin/env python3
"""
Certificate Catalog Comparison Script

This script compares certificate data between:
1. CloudRadar API (live certificate configurations)
2. Certificate Catalog Dataset (usage and compliance data)

Provides gap analysis and data quality insights for certificate monitoring.

Usage:
    python certificate_catalog_comparison.py [--output-file comparison.json] [--format {json,csv}]
"""

import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
import argparse
import sys

import pandas as pd

from config_pipeline import ConfigPipeline
from connectors.api import OauthApi
from connectors.ca_certs import C1_CERT_FILE
from connectors.exchange.oauth_token import refresh
from connectors.snowflake import SnowflakeConnector
from etip_env import Env, set_env_vars

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CertificateCatalogComparison:
    """
    Compare certificate data between CloudRadar API and Certificate Catalog Dataset
    
    This tool identifies gaps, inconsistencies, and provides data quality insights
    for certificate monitoring and compliance purposes.
    """
    
    def __init__(self, env: Env):
        self.env = env
        self.api_url = f"https://{self.env.exchange.exchange_url}/internal-operations/cloud-service/aws-tooling/search-resource-configurations"
        self.api_certificates = {}
        self.catalog_certificates = {}
        self.comparison_results = {}
        
    def _get_api_connector(self) -> OauthApi:
        """Get authenticated API connector"""
        api_token = refresh(
            client_id=self.env.exchange.client_id,
            client_secret=self.env.exchange.client_secret,
            exchange_url=self.env.exchange.exchange_url,
        )
        
        return OauthApi(
            url=self.api_url,
            api_token=f"Bearer {api_token}"
        )
    
    def _get_snowflake_connector(self) -> SnowflakeConnector:
        """Get Snowflake connector for catalog data"""
        return SnowflakeConnector(env=self.env)
    
    def fetch_api_certificates(self, issuer_filter: Optional[str] = None) -> Dict[str, Dict]:
        """
        Fetch certificates from CloudRadar API
        
        Args:
            issuer_filter: Optional issuer filter (e.g., 'Amazon', 'DigiCert')
            
        Returns:
            Dictionary mapping ARNs to certificate data
        """
        logger.info(f"Fetching certificates from CloudRadar API (issuer filter: {issuer_filter or 'None'})...")
        
        api_connector = self._get_api_connector()
        all_certificates = {}
        next_record_key = None
        
        # Build search parameters
        search_parameters = [{"resourceType": "AWS::ACM::Certificate"}]
        
        if issuer_filter:
            search_parameters[0]["configurationItems"] = [
                {
                    "configurationName": "issuer",
                    "configurationValue": issuer_filter
                }
            ]
        
        search_payload = {
            "searchParameters": search_parameters,
            "responseFields": [
                "accountName",
                "accountResourceId", 
                "amazonResourceName",
                "resourceId",
                "resourceType",
                "configurationList",
                "supplementaryConfiguration",
                "source",
                "tags"
            ]
        }
        
        headers = {
            "Accept": "application/json;v=1",
            "Authorization": api_connector.api_token,
            "Content-Type": "application/json"
        }
        
        page_count = 0
        while True:
            try:
                page_count += 1
                logger.info(f"Fetching API page {page_count}...")
                
                payload = search_payload.copy()
                if next_record_key:
                    payload["nextRecordKey"] = next_record_key
                
                response = api_connector.send_request(
                    url="",
                    request_type="post",
                    request_kwargs={
                        "headers": headers,
                        "json": payload,
                        "verify": C1_CERT_FILE,
                        "timeout": 120,
                    },
                    retry_delay=5,
                    retry_count=3
                )
                
                if response.status_code != 200:
                    raise RuntimeError(f"API request failed: {response.status_code} - {response.text}")
                
                data = response.json()
                certificates = data.get("resourceConfigurations", [])
                
                for cert in certificates:
                    # Skip orphaned certificates
                    if cert.get("source") == "CT-AccessDenied":
                        continue
                    
                    arn = cert.get("amazonResourceName", "")
                    if arn:
                        all_certificates[arn.lower()] = self._parse_api_certificate(cert)
                
                logger.info(f"Processed {len(certificates)} certificates from page {page_count}")
                
                next_record_key = data.get("nextRecordKey")
                if not next_record_key:
                    break
                    
            except Exception as e:
                raise RuntimeError(f"Failed to fetch certificates from API: {str(e)}")
        
        logger.info(f"API fetch complete. Retrieved {len(all_certificates)} certificates.")
        self.api_certificates = all_certificates
        return all_certificates
    
    def fetch_catalog_certificates(self, issuer_filter: Optional[str] = None, days_back: int = 365) -> Dict[str, Dict]:
        """
        Fetch certificates from Certificate Catalog dataset
        
        Args:
            issuer_filter: Optional issuer filter (e.g., 'Amazon', 'DigiCert')
            days_back: How many days back to look for certificates
            
        Returns:
            Dictionary mapping ARNs to certificate data
        """
        logger.info(f"Fetching certificates from Certificate Catalog (issuer filter: {issuer_filter or 'None'})...")
        
        # Build SQL query
        base_query = """
        SELECT DISTINCT
            certificate_arn,
            certificate_id,
            nominal_issuer,
            not_valid_before_utc_timestamp,
            not_valid_after_utc_timestamp,
            last_usage_observation_utc_timestamp,
            first_observation_utc_timestamp,
            last_observation_utc_timestamp
        FROM 
            CYBR_DB.PHDP_CYBR.CERTIFICATE_CATALOG_CERTIFICATE_USAGE 
        WHERE 
            certificate_arn LIKE '%arn:aws:acm%'
            AND not_valid_after_utc_timestamp >= DATEADD('DAY', -{days_back}, CURRENT_TIMESTAMP)
        """.format(days_back=days_back)
        
        if issuer_filter:
            base_query += f"\n            AND nominal_issuer = '{issuer_filter}'"
        
        base_query += "\n        ORDER BY certificate_arn"
        
        try:
            snowflake_connector = self._get_snowflake_connector()
            result = snowflake_connector.execute_query(base_query)
            
            catalog_certificates = {}
            for row in result:
                arn = row[0]  # certificate_arn
                if arn:
                    catalog_certificates[arn.lower()] = {
                        "certificate_arn": arn,
                        "certificate_id": row[1],
                        "nominal_issuer": row[2],
                        "not_valid_before_utc_timestamp": row[3],
                        "not_valid_after_utc_timestamp": row[4],
                        "last_usage_observation_utc_timestamp": row[5],
                        "first_observation_utc_timestamp": row[6],
                        "last_observation_utc_timestamp": row[7]
                    }
            
            logger.info(f"Catalog fetch complete. Retrieved {len(catalog_certificates)} certificates.")
            self.catalog_certificates = catalog_certificates
            return catalog_certificates
            
        except Exception as e:
            raise RuntimeError(f"Failed to fetch certificates from catalog: {str(e)}")
    
    def _parse_api_certificate(self, cert_config: Dict) -> Dict:
        """Parse certificate data from API response"""
        cert_info = {
            "arn": cert_config.get("amazonResourceName", ""),
            "resource_id": cert_config.get("resourceId", ""),
            "account_name": cert_config.get("accountName", ""),
            "account_id": cert_config.get("accountResourceId", ""),
            "source": cert_config.get("source", ""),
        }
        
        # Extract configuration details
        config_list = cert_config.get("configurationList", [])
        for config in config_list:
            config_name = config.get("configurationName", "")
            config_value = config.get("configurationValue", "")
            
            if config_name == "issuer":
                cert_info["issuer"] = config_value
            elif config_name == "subject":
                cert_info["subject"] = config_value
            elif config_name == "status":
                cert_info["status"] = config_value
            elif config_name == "domainName":
                cert_info["domain_name"] = config_value
            elif config_name == "inUseBy":
                cert_info["in_use_by"] = config_value
            elif config_name == "notBefore":
                cert_info["not_before"] = config_value
            elif config_name == "notAfter":
                cert_info["not_after"] = config_value
            elif config_name == "type":
                cert_info["type"] = config_value
        
        return cert_info
    
    def compare_datasets(self) -> Dict:
        """
        Compare API and catalog datasets to identify gaps and inconsistencies
        
        Returns:
            Comprehensive comparison results
        """
        logger.info("Performing dataset comparison analysis...")
        
        if not self.api_certificates:
            raise ValueError("No API certificates loaded. Call fetch_api_certificates() first.")
        
        if not self.catalog_certificates:
            raise ValueError("No catalog certificates loaded. Call fetch_catalog_certificates() first.")
        
        api_arns = set(self.api_certificates.keys())
        catalog_arns = set(self.catalog_certificates.keys())
        
        # Calculate overlaps and gaps
        common_arns = api_arns.intersection(catalog_arns)
        api_only_arns = api_arns - catalog_arns
        catalog_only_arns = catalog_arns - api_arns
        
        # Build comparison results
        comparison_results = {
            "analysis_timestamp": datetime.now().isoformat(),
            "dataset_summary": {
                "api_certificate_count": len(api_arns),
                "catalog_certificate_count": len(catalog_arns),
                "common_certificates": len(common_arns),
                "api_only_certificates": len(api_only_arns),
                "catalog_only_certificates": len(catalog_only_arns)
            },
            "coverage_analysis": {
                "api_coverage_percentage": round((len(common_arns) / len(catalog_arns)) * 100, 2) if catalog_arns else 0,
                "catalog_coverage_percentage": round((len(common_arns) / len(api_arns)) * 100, 2) if api_arns else 0
            },
            "gap_analysis": {
                "api_only_sample": list(api_only_arns)[:10],
                "catalog_only_sample": list(catalog_only_arns)[:10]
            },
            "data_quality_analysis": {},
            "issuer_comparison": {},
            "status_analysis": {}
        }
        
        # Perform detailed analysis on common certificates
        if common_arns:
            comparison_results["data_quality_analysis"] = self._analyze_data_quality(common_arns)
            comparison_results["issuer_comparison"] = self._analyze_issuer_consistency(common_arns)
            comparison_results["status_analysis"] = self._analyze_status_patterns()
        
        self.comparison_results = comparison_results
        logger.info("Dataset comparison analysis complete.")
        
        return comparison_results
    
    def _analyze_data_quality(self, common_arns: Set[str]) -> Dict:
        """Analyze data quality for certificates in both datasets"""
        quality_issues = {
            "missing_expiration_api": 0,
            "missing_expiration_catalog": 0,
            "expiration_date_mismatches": 0,
            "missing_usage_data": 0,
            "status_inconsistencies": 0
        }
        
        sample_issues = {
            "expiration_mismatches": [],
            "missing_usage_examples": [],
            "status_inconsistency_examples": []
        }
        
        for arn in list(common_arns)[:100]:  # Sample analysis to avoid performance issues
            api_cert = self.api_certificates[arn]
            catalog_cert = self.catalog_certificates[arn]
            
            # Check expiration date availability
            api_expiry = api_cert.get("not_after", "")
            catalog_expiry = catalog_cert.get("not_valid_after_utc_timestamp")
            
            if not api_expiry:
                quality_issues["missing_expiration_api"] += 1
            if not catalog_expiry:
                quality_issues["missing_expiration_catalog"] += 1
            
            # Check for expiration date mismatches (if both exist)
            if api_expiry and catalog_expiry:
                try:
                    api_date = datetime.fromisoformat(api_expiry.replace('Z', '+00:00'))
                    catalog_date = catalog_expiry
                    
                    # Allow 1-day difference for timezone/formatting issues
                    if abs((api_date.date() - catalog_date.date()).days) > 1:
                        quality_issues["expiration_date_mismatches"] += 1
                        if len(sample_issues["expiration_mismatches"]) < 5:
                            sample_issues["expiration_mismatches"].append({
                                "arn": arn,
                                "api_expiry": api_expiry,
                                "catalog_expiry": catalog_expiry.isoformat() if catalog_expiry else None
                            })
                except (ValueError, TypeError):
                    pass
            
            # Check usage data availability
            usage_observation = catalog_cert.get("last_usage_observation_utc_timestamp")
            if not usage_observation:
                quality_issues["missing_usage_data"] += 1
                if len(sample_issues["missing_usage_examples"]) < 5:
                    sample_issues["missing_usage_examples"].append(arn)
        
        return {
            "quality_metrics": quality_issues,
            "sample_issues": sample_issues
        }
    
    def _analyze_issuer_consistency(self, common_arns: Set[str]) -> Dict:
        """Analyze issuer consistency between datasets"""
        issuer_analysis = {
            "total_compared": 0,
            "consistent_issuers": 0,
            "inconsistent_issuers": 0,
            "api_issuer_distribution": {},
            "catalog_issuer_distribution": {},
            "inconsistency_examples": []
        }
        
        for arn in list(common_arns)[:100]:  # Sample analysis
            api_cert = self.api_certificates[arn]
            catalog_cert = self.catalog_certificates[arn]
            
            api_issuer = api_cert.get("issuer", "Unknown")
            catalog_issuer = catalog_cert.get("nominal_issuer", "Unknown")
            
            issuer_analysis["total_compared"] += 1
            
            # Count issuer distributions
            issuer_analysis["api_issuer_distribution"][api_issuer] = \
                issuer_analysis["api_issuer_distribution"].get(api_issuer, 0) + 1
            issuer_analysis["catalog_issuer_distribution"][catalog_issuer] = \
                issuer_analysis["catalog_issuer_distribution"].get(catalog_issuer, 0) + 1
            
            # Check consistency
            if api_issuer.lower() == catalog_issuer.lower():
                issuer_analysis["consistent_issuers"] += 1
            else:
                issuer_analysis["inconsistent_issuers"] += 1
                if len(issuer_analysis["inconsistency_examples"]) < 5:
                    issuer_analysis["inconsistency_examples"].append({
                        "arn": arn,
                        "api_issuer": api_issuer,
                        "catalog_issuer": catalog_issuer
                    })
        
        return issuer_analysis
    
    def _analyze_status_patterns(self) -> Dict:
        """Analyze certificate status patterns in API data"""
        status_patterns = {
            "api_status_distribution": {},
            "usage_by_status": {},
            "expired_certificates": []
        }
        
        for arn, cert in self.api_certificates.items():
            status = cert.get("status", "Unknown")
            in_use_by = cert.get("in_use_by", "[]")
            not_after = cert.get("not_after", "")
            
            # Count status distribution
            status_patterns["api_status_distribution"][status] = \
                status_patterns["api_status_distribution"].get(status, 0) + 1
            
            # Analyze usage by status
            is_in_use = in_use_by != "[]" and in_use_by != ""
            if status not in status_patterns["usage_by_status"]:
                status_patterns["usage_by_status"][status] = {"in_use": 0, "not_in_use": 0}
            
            if is_in_use:
                status_patterns["usage_by_status"][status]["in_use"] += 1
            else:
                status_patterns["usage_by_status"][status]["not_in_use"] += 1
            
            # Check for expired certificates
            if not_after:
                try:
                    expiry_date = datetime.fromisoformat(not_after.replace('Z', '+00:00'))
                    if expiry_date < datetime.now(expiry_date.tzinfo):
                        status_patterns["expired_certificates"].append({
                            "arn": arn,
                            "expiry_date": not_after,
                            "status": status,
                            "in_use": is_in_use
                        })
                except (ValueError, TypeError):
                    pass
        
        # Limit expired certificates list
        status_patterns["expired_certificates"] = status_patterns["expired_certificates"][:20]
        
        return status_patterns
    
    def generate_summary_report(self) -> Dict:
        """Generate a comprehensive summary report"""
        if not self.comparison_results:
            raise ValueError("No comparison results available. Run compare_datasets() first.")
        
        results = self.comparison_results
        
        summary = {
            "analysis_timestamp": results["analysis_timestamp"],
            "overall_assessment": "",
            "key_findings": [],
            "recommendations": [],
            "dataset_health_score": 0
        }
        
        # Calculate health score (0-100)
        dataset_summary = results["dataset_summary"]
        coverage_analysis = results["coverage_analysis"]
        
        # Base score on coverage and common certificates
        if dataset_summary["api_certificate_count"] > 0 and dataset_summary["catalog_certificate_count"] > 0:
            coverage_score = (coverage_analysis["api_coverage_percentage"] + coverage_analysis["catalog_coverage_percentage"]) / 2
            common_ratio = dataset_summary["common_certificates"] / max(dataset_summary["api_certificate_count"], dataset_summary["catalog_certificate_count"])
            health_score = (coverage_score + common_ratio * 100) / 2
            summary["dataset_health_score"] = round(health_score, 1)
        
        # Generate key findings
        if dataset_summary["api_only_certificates"] > 0:
            summary["key_findings"].append(
                f"Found {dataset_summary['api_only_certificates']} certificates in API but not in catalog"
            )
        
        if dataset_summary["catalog_only_certificates"] > 0:
            summary["key_findings"].append(
                f"Found {dataset_summary['catalog_only_certificates']} certificates in catalog but not in API"
            )
        
        # Data quality findings
        if "data_quality_analysis" in results:
            quality_metrics = results["data_quality_analysis"]["quality_metrics"]
            if quality_metrics["expiration_date_mismatches"] > 0:
                summary["key_findings"].append(
                    f"Found {quality_metrics['expiration_date_mismatches']} expiration date mismatches between datasets"
                )
        
        # Generate overall assessment
        if summary["dataset_health_score"] >= 90:
            summary["overall_assessment"] = "Excellent data consistency between API and catalog"
        elif summary["dataset_health_score"] >= 75:
            summary["overall_assessment"] = "Good data consistency with minor gaps"
        elif summary["dataset_health_score"] >= 50:
            summary["overall_assessment"] = "Moderate data consistency issues require attention"
        else:
            summary["overall_assessment"] = "Significant data consistency issues detected"
        
        # Generate recommendations
        if dataset_summary["api_only_certificates"] > 10:
            summary["recommendations"].append("Investigate why API certificates are missing from catalog")
        
        if dataset_summary["catalog_only_certificates"] > 10:
            summary["recommendations"].append("Review catalog data for obsolete certificate records")
        
        if "data_quality_analysis" in results:
            quality_metrics = results["data_quality_analysis"]["quality_metrics"]
            if quality_metrics["missing_usage_data"] > 5:
                summary["recommendations"].append("Improve certificate usage tracking in catalog")
        
        return summary
    
    def export_results(self, output_file: str, format_type: str = "json"):
        """
        Export comparison results to file
        
        Args:
            output_file: Output file path
            format_type: Export format ('json' or 'csv')
        """
        if not self.comparison_results:
            raise ValueError("No comparison results to export. Run compare_datasets() first.")
        
        if format_type.lower() == "json":
            export_data = {
                "summary": self.generate_summary_report(),
                "detailed_results": self.comparison_results
            }
            
            with open(output_file, 'w') as f:
                json.dump(export_data, f, indent=2, default=str)
                
        elif format_type.lower() == "csv":
            # Create CSV with certificate comparison data
            rows = []
            
            # Common certificates
            api_arns = set(self.api_certificates.keys())
            catalog_arns = set(self.catalog_certificates.keys())
            all_arns = api_arns.union(catalog_arns)
            
            for arn in all_arns:
                row = {"certificate_arn": arn}
                
                if arn in self.api_certificates:
                    api_cert = self.api_certificates[arn]
                    row.update({
                        "in_api": True,
                        "api_issuer": api_cert.get("issuer", ""),
                        "api_status": api_cert.get("status", ""),
                        "api_expiry": api_cert.get("not_after", ""),
                        "api_in_use": api_cert.get("in_use_by", "") != "[]"
                    })
                else:
                    row.update({
                        "in_api": False,
                        "api_issuer": "",
                        "api_status": "",
                        "api_expiry": "",
                        "api_in_use": None
                    })
                
                if arn in self.catalog_certificates:
                    catalog_cert = self.catalog_certificates[arn]
                    row.update({
                        "in_catalog": True,
                        "catalog_issuer": catalog_cert.get("nominal_issuer", ""),
                        "catalog_expiry": catalog_cert.get("not_valid_after_utc_timestamp", ""),
                        "last_usage": catalog_cert.get("last_usage_observation_utc_timestamp", "")
                    })
                else:
                    row.update({
                        "in_catalog": False,
                        "catalog_issuer": "",
                        "catalog_expiry": "",
                        "last_usage": ""
                    })
                
                rows.append(row)
            
            df = pd.DataFrame(rows)
            df.to_csv(output_file, index=False)
        
        logger.info(f"Results exported to {output_file} in {format_type.upper()} format")
    
    def print_summary(self):
        """Print formatted summary to console"""
        if not self.comparison_results:
            logger.error("No comparison results available. Run compare_datasets() first.")
            return
        
        summary = self.generate_summary_report()
        results = self.comparison_results
        
        print("\n" + "="*80)
        print("CERTIFICATE CATALOG COMPARISON REPORT")
        print("="*80)
        print(f"Analysis Timestamp: {summary['analysis_timestamp']}")
        print(f"Dataset Health Score: {summary['dataset_health_score']}/100")
        print(f"Overall Assessment: {summary['overall_assessment']}")
        
        print("\nDATASET SUMMARY:")
        print("-" * 20)
        ds = results["dataset_summary"]
        print(f"API Certificates        : {ds['api_certificate_count']:,}")
        print(f"Catalog Certificates    : {ds['catalog_certificate_count']:,}")
        print(f"Common Certificates     : {ds['common_certificates']:,}")
        print(f"API Only Certificates   : {ds['api_only_certificates']:,}")
        print(f"Catalog Only Certificates: {ds['catalog_only_certificates']:,}")
        
        print("\nCOVERAGE ANALYSIS:")
        print("-" * 20)
        ca = results["coverage_analysis"]
        print(f"API Coverage of Catalog    : {ca['api_coverage_percentage']:5.1f}%")
        print(f"Catalog Coverage of API    : {ca['catalog_coverage_percentage']:5.1f}%")
        
        print("\nKEY FINDINGS:")
        print("-" * 15)
        for i, finding in enumerate(summary["key_findings"], 1):
            print(f"{i}. {finding}")
        
        print("\nRECOMMENDATIONS:")
        print("-" * 18)
        for i, rec in enumerate(summary["recommendations"], 1):
            print(f"{i}. {rec}")
        
        print("\n" + "="*80)


def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(
        description="Certificate Catalog Comparison Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Compare all certificates
    python certificate_catalog_comparison.py
    
    # Compare only Amazon-issued certificates  
    python certificate_catalog_comparison.py --issuer Amazon
    
    # Export results to JSON
    python certificate_catalog_comparison.py --output-file comparison.json
    
    # Export results to CSV
    python certificate_catalog_comparison.py --output-file comparison.csv --format csv
        """
    )
    
    parser.add_argument(
        "--issuer",
        type=str,
        help="Filter by certificate issuer (e.g., 'Amazon', 'DigiCert')"
    )
    
    parser.add_argument(
        "--output-file", 
        type=str,
        help="Export results to file"
    )
    
    parser.add_argument(
        "--format",
        choices=["json", "csv"],
        default="json",
        help="Export format (default: json)"
    )
    
    parser.add_argument(
        "--days-back",
        type=int,
        default=365,
        help="How many days back to look in catalog (default: 365)"
    )
    
    args = parser.parse_args()
    
    try:
        # Initialize environment
        logger.info("Initializing environment...")
        env = set_env_vars()
        
        # Create comparison tool
        comparator = CertificateCatalogComparison(env)
        
        # Fetch data from both sources
        logger.info("Fetching certificate data from both sources...")
        api_certs = comparator.fetch_api_certificates(issuer_filter=args.issuer)
        catalog_certs = comparator.fetch_catalog_certificates(
            issuer_filter=args.issuer,
            days_back=args.days_back
        )
        
        if not api_certs and not catalog_certs:
            logger.warning("No certificates found in either dataset. Exiting.")
            return
        
        # Perform comparison
        comparison_results = comparator.compare_datasets()
        
        # Print summary
        comparator.print_summary()
        
        # Export results if requested
        if args.output_file:
            comparator.export_results(args.output_file, args.format)
        
        logger.info("Certificate catalog comparison completed successfully.")
        
    except Exception as e:
        logger.error(f"Comparison failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
import datetime
import json
import unittest.mock as mock
from typing import Optional

import pandas as pd
from freezegun import freeze_time
from requests import Response

import pipelines.pl_automated_monitoring_cloud_custodian.pipeline as pipeline
from etip_env import set_env_vars
from pipelines.pl_automated_monitoring_cloud_custodian import transform
from tests.config_pipeline.helpers import ConfigPipelineTestCase


# Update unit tests for each new control added to the pipeline #
def _tier_threshold_df():
    return pd.DataFrame(
        {
            "control_id": [
                "CTRL-1077109",
                "CTRL-1077109",
                "CTRL-1080546",
                "CTRL-1077108",
                "CTRL-1078029",
                "CTRL-1078029",
                "CTRL-1077108",
                "CTRL-1077109",
                "CTRL-1106206",
                "CTRL-1106206",
                "CTRL-1106204",
                "CTRL-1106205",
            ],
            "monitoring_metric_id": [2, 1, 3, 4, 6, 5, 7, 2, 8, 9, 10, 11],
            "monitoring_metric_tier": [
                "Tier 2",
                "Tier 1",
                "Tier 0",
                "Tier 2",
                "Tier 2",
                "Tier 1",
                "Tier 1",
                "Tier 2",
                "Tier 1",
                "Tier 2",
                "Tier 1",
                "Tier 1",
            ],
            "warning_threshold": [
                75.0,
                97.0,
                None,
                97.0,
                97.0,
                95.0,
                97.0,
                80.0,
                90.0,
                95.0,
                95.0,
                95.0,
            ],
            "alerting_threshold": [
                50.0,
                95.0,
                100.0,
                95.0,
                95.0,
                90.0,
                95.0,
                60.0,
                85.0,
                90.0,
                90.0,
                90.0,
            ],
            "control_executor": [
                "Individual_1",
                "Individual_2",
                "Individual_3",
                "Individual_4",
                "Individual_5",
                "Individual_6",
                "Individual_7",
                "Individual_1",
                "Individual_8",
                "Individual_8",
                "Individual_9",
                "Individual_9",
            ],
            "metric_threshold_start_date": [
                datetime.datetime(2024, 11, 5, 12, 9, 00, 21180),
                datetime.datetime(2024, 11, 5, 12, 9, 00, 21180),
                datetime.datetime(2024, 11, 5, 12, 9, 00, 21180),
                datetime.datetime(2024, 11, 5, 12, 9, 00, 21180),
                datetime.datetime(2024, 11, 5, 12, 9, 00, 21180),
                datetime.datetime(2024, 11, 5, 12, 9, 00, 21180),
                datetime.datetime(2024, 11, 5, 12, 9, 00, 21180),
                datetime.datetime(2024, 10, 5, 12, 9, 00, 21180),
                datetime.datetime(2024, 10, 5, 12, 9, 00, 21180),
                datetime.datetime(2024, 10, 5, 12, 9, 00, 21180),
                datetime.datetime(2024, 10, 5, 12, 9, 00, 21180),
                datetime.datetime(2024, 10, 5, 12, 9, 00, 21180),
            ],
            "metric_threshold_end_date": [
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                datetime.datetime(2024, 11, 5, 12, 9, 00, 21180),
                None,
                None,
                None,
                None,
            ],
        }
    )

# Tier 1 Datasets


def _tier_1_metric_output_df():
    return pd.DataFrame(
        {
            "monitoring_metric_id": [5, 7, 1, 11, 10, 8],
            "control_id": [
                "CTRL-1078029",
                "CTRL-1077108",
                "CTRL-1077109",
                "CTRL-1106205",
                "CTRL-1106204",
                "CTRL-1106206",
            ],
            "monitoring_metric_value": [89.0, 100.0, 96.0, 0.0, 0.0, 100.0],
            "monitoring_metric_status": [
                "Red",
                "Green",
                "Yellow",
                "Red",
                "Red",
                "Green",
            ],
            "metric_value_numerator": [89, 250, 96, 0, 0, 125],
            "metric_value_denominator": [100, 250, 100, 0, 0, 125],
            "resources_info": [
                [
                    '{"account_number": "Account 3", "region": "us-east-1", "environment": "Prod", "division": "Bank Tech", "asv": "ASV-3", "control_id": "AC-3.AWS.80.v01", "resource_type": "AWS::EBS::Snapshot", "total_evaluations_count": 5, "inventory_count": 5, "service_name": "EBS"}',
                    '{"account_number": "Account 4", "region": "us-east-2", "environment": "CDE", "division": "Enterprise Platforms Tech", "asv": "ASV-4", "control_id": "AC-3.AWS.80.v01", "resource_type": "AWS::EBS::Snapshot", "total_evaluations_count": 15, "inventory_count": 20, "service_name": "EBS"}',
                ],
                None,
                [
                    '{"account_number": "Account 2", "region": "global", "environment": "Prod", "division": "Cyber Tech", "asv": "ASV-2", "control_id": "SC-8.AWS.43.v02", "resource_type": "AWS::CloudFront::Distribution", "total_evaluations_count": 4, "inventory_count": 4, "service_name": "CloudFront"}'
                ],
                None,
                None,
                None,
            ],
            "control_monitoring_utc_timestamp": [
                datetime.datetime(2024, 11, 5, 12, 9, 00, 21180),
                datetime.datetime(2024, 11, 5, 12, 9, 00, 21180),
                datetime.datetime(2024, 11, 5, 12, 9, 00, 21180),
                datetime.datetime(2024, 11, 5, 12, 9, 00, 21180),
                datetime.datetime(2024, 11, 5, 12, 9, 00, 21180),
                datetime.datetime(2024, 11, 5, 12, 9, 00, 21180),
            ],
        }
    )


def _total_inventory_df():
    return pd.DataFrame(
        {
            "control_id": [
                "SC-8.AWS.43.v02",
                "SC-8.AWS.43.v02",
                "SC-8.AWS.43.v02",
                "SC-8.AWS.42.v01",
                "AC-3.AWS.80.v01",
                "AC-3.AWS.80.v01",
                "CM-8.AWS.95.v01",
                "CM-6.AWS. WHILE74.v01",
                "SI-4.AWS.4973.v02",
            ],
            "control_component": ["A", "A", "C", "A", "A", "A", "H", "A", "A"],
            "fuse_id": [
                "CTRL-1077109",
                "CTRL>xAI-1077109",
                "CTRL-TEST",
                "CTRL-1077108",
                "CTRL-1078029",
                "CTRL-1078029",
                "CTRL-TEST2",
                "CTRL-1106206",
                "CTRL-1106206",
            ],
            "compliance_state": [
                "True",
                "N/A",
                "N/A",
                "True",
                "True",
                "N/A",
                "True",
                "True",
                "True",
            ],
            "total_resources": [96, 4, 94, 250, 89, 11, 325, 50, 75],
            "service_name": [
                "CloudFront",
                "CloudFront",
                "CloudFront",
                "CloudFront",
                "EBS",
                "EBS",
                "EC2",
                "GuardDuty",
                "GuardDuty",
            ],
        }
    )

def _not_evaluated_df():
    return pd.DataFrame(
        {
            "account_number": [
                "Account 2",
                "Account 3",
                "Account 4",
                "Account 5",
            ],
            "region": ["global", "us-east-1", "us-east-2", "ap-northeast-3"],
            "environment": ["Prod", "Prod", "CDE", "CDE"],
            "division": [
                "Cyber Tech",
                "Bank Tech",
                "Enterprise Platforms Tech",
                "AXT",
            ],
            "asv": ["ASV-2", "ASV-3", "ASV-4", "ASV-5"],
            "control_id": [
                "SC-8.AWS.43.v02",
                "AC-3.AWS.80.v01",
                "AC-3.AWS.80.v01",
                "IA-2.AWS.12.v01",
            ],
            "resource_type": [
                "AWS::CloudFront::Distribution",
                "AWS::EBS::Snapshot",
                "AWS::EBS::Snapshot",
                "AWS::EC2::Group",
            ],
            "total_evaluations_count": [4, 5, 15, 3],
            "inventory_count": [4, 5, 20, 3],
            "service_name": ["CloudFront", "EBS", "EBS", "EC2"],
        }
    )


def _tier_1_total_inventory_empty_df():
    return pd.DataFrame(
        {
            "control_id": [],
            "control_component": [],
            "fuse_id": [],
            "compliance_state": [],
            "total_resources": [],
            "service_name": [],
        }
    )


def _tier_1_not_evaluated_empty_df():
    return pd.DataFrame(
        {
            "account_number": [],
            "region": [],
            "environment": [],
            "division": [],
            "asv": [],
            "control_id": [],
            "resource_type": [],
            "total_evaluations_count": [],
            "inventory_count": [],
            "service_name": [],
        }
    )


def _tier_1_empty_df_output():
    return pd.DataFrame(
        {
            "monitoring_metric_id": [5, 7, 1, 11, 10, 8],
            "control_id": [
                "CTRL-1078029",
                "CTRL-1077108",
                "CTRL-1077109",
                "CTRL-1106205",
                "CTRL-1106204",
                "CTRL-1106206",
            ],
            "monitoring_metric_value": [0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
            "monitoring_metric_status": ["Red", "Red", "Red", "Red", "Red", "Red"],
            "metric_value_numerator": [0, 0, 0, 0, 0, 0],
            "metric_value_denominator": [0, 0, 0, 0, 0, 0],
            "resources_info": [
                None,
                None,
                None,
                None,
                None,
                None,
            ],
            "control_monitoring_utc_timestamp": [
                datetime.datetime(2024, 11, 5, 12, 9, 00, 21180),
                datetime.datetime(2024, 11, 5, 12, 9, 00, 21180),
                datetime.datetime(2024, 11, 5, 12, 9, 00, 21180),
                datetime.datetime(2024, 11, 5, 12, 9, 00, 21180),
                datetime.datetime(2024, 11, 5, 12, 9, 00, 21180),
                datetime.datetime(2024, 11, 5, 12, 9, 00, 21180),
            ],
        }
    )

# Tier 2 Datasets


def _tier_2_metric_output_df():
    return pd.DataFrame(
        {
            "monitoring_metric_id": [4, 2, 6],
            "control_id": [
                "CTRL-1077108",
                "CTRL-1077109",
                "CTRL-1078029",
            ],
            "monitoring_metric_value": [100.0, 75.0, 50.0],
            "monitoring_metric_status": ["Green", "Green", "Red"],
            "metric_value_numerator": [8, 3, 1],
            "metric_value_denominator": [8, 4, 2],
            "resources_info": [
                None,
                [
                    '{"executionId": "04151428-72bc-404f-5433-6723dc2fdd19", "awsRegion": "us-east-1", "environment": "cde"}'
                ],
                [
                    '{"executionId": "04151428-72bc-404f-9783-6723dc2fdd19", "awsRegion": "us-east-1", "environment": "dev"}'
                ],
            ],
            "control_monitoring_utc_timestamp": [
                datetime.datetime(2024, 11, 5, 12, 9, 00, 21180),
                datetime.datetime(2024, 11, 5, 12, 9, 00, 21180),
                datetime.datetime(2024, 11, 5, 12, 9, 00, 21180),
            ],
        }
    )


def _tier_2_metric_output_empty_df():
    return pd.DataFrame(
        {
            "monitoring_metric_id": [6, 4, 2],
            "control_id": [
                "CTRL-1078029",
                "CTRL-1077108",
                "CTRL-1077109",
            ],
            "monitoring_metric_value": [0.0, 0.0, 0.0],
            "monitoring_metric_status": ["Red", "Red", "Red"],
            "metric_value_numerator": [0, 0, 0],
            "metric_value_denominator": [0, 0, 0],
            "resources_info": [
                None,
                None,
                None,
            ],
            "control_monitoring_utc_timestamp": [
                datetime.datetime(2024, 11, 5, 12, 9, 00, 21180),
                datetime.datetime(2024, 11, 5, 12, 9, 00, 21180),
                datetime.datetime(2024, 11, 5, 12, 9, 00, 21180),
            ],
        }
    )


EMPTY_API_RESPONSE_DATA = {"items": []}

API_RESPONSE_DATA_WITH_NEXT_KEY = {
    "items": [
        {
            "awsRegion": "us-east-2",
            "controlCategory": ["public access"],
            "controlId": ["CM-6.AWS.74"],
            "executionId": "4785849f-d227-40a6-ba86-4a1a61d36087",
            "controlRisk": ["critical"],
            "custodianResource": "aws.guardduty",
            "environment": "cde",
            "fromTimestamp": "1730762560",
            "policyName": "example-guardduty-1",
            "testRunStatus": "PASS",
            "toTimestamp": "1730762668",
        },
        {
            "awsRegion": "us-east-1",
            "controlCategory": ["public access"],
            "controlId": ["SI-4.AWS.4973"],
            "executionId": "4785849f-d227-40a6-ba86-12345687",
            "controlRisk": ["critical"],
            "custodianResource": "aws.guardduty",
            "environment": "prod",
            "fromTimestamp": "1730762560",
            "policyName": "example-guardduty-2",
            "testRunStatus": "FAIL",
            "toTimestamp": "1730762668",
        },
        {
            "awsRegion": "us-east-1",
            "controlCategory": ["public access"],
            "controlId": ["AC-3.AWS.80"],
            "executionId": "4785849f-d227-40a6-ba86-4a1a61d36087",
            "controlRisk": ["critical"],
            "custodianResource": "aws.ebs-snapshot",
            "environment": "dev",
            "fromTimestamp": "1730762560",
            "policyName": "monitor-ebssnpsht-sharing-compliant",
            "testRunStatus": "PASS",
            "toTimestamp": "1730762668",
        },
        {
            "awsRegion": "us-east-1",
            "controlCategory": ["public access"],
            "controlId": ["AC-3.AWS.18"],
            "executionId": "04151428-72bc-404f-9b19-6723dc2fdd19",
            "controlRisk": ["critical"],
            "custodianResource": "aws.message-broker",
            "environment": "cde",
            "fromTimestamp": "1730765219",
            "policyName": "monitor-mssgbrkr-no-public-access-compliant",
            "testRunStatus": "FAIL",
            "toTimestamp": "1730766338",
        },
        {
            "awsRegion": "us-east-1",
            "controlCategory": ["public access"],
            "controlId": ["AC-3.AWS.18"],
            "executionId": "04151428-72bc-404f-1234-6723dc2fdd19",
            "controlRisk": ["critical"],
            "custodianResource": "aws.message-broker",
            "environment": "cde",
            "fromTimestamp": "1730764002",
            "policyName": "enterprise-mssgbrkr-no-public-delete",
            "testRunStatus": "FAIL",
            "toTimestamp": "1730765219",
        },
        {
            "awsRegion": "us-east-1",
            "controlCategory": ["public access"],
            "controlId": ["AC-3.AWS.18"],
            "executionId": "04151428-72bc-404f-5678-6723dc2fdd19",
            "controlRisk": ["critical"],
            "custodianResource": "aws.message-broker",
            "environment": "cde",
            "fromTimestamp": "1730762822",
            "policyName": "enterprise-mssgbrkr-no-public-tag",
            "testRunStatus": "PASS",
            "toTimestamp": "1730764002",
        },
        {
            "awsRegion": "us-east-1",
            "controlCategory": ["public access"],
            "controlId": ["SC-8.AWS.43"],
            "executionId": "04151428-72bc-404f-9101-6723dc2fdd19",
            "controlRisk": ["critical"],
            "custodianResource": "aws.distribution",
            "environment": "cde",
            "fromTimestamp": "1730760735",
            "policyName": "enterprise-dstrbtn-signed-url-newchange-disable",
            "testRunStatus": "PASS",
            "toTimestamp": "1730762034",
        },
        {
            "awsRegion": "us-east-1",
            "controlCategory": ["public access"],
            "controlId": ["SC-8.AWS.43"],
            "executionId": "04151428-72bc-404f-1314-6723dc2fdd19",
            "controlRisk": ["critical"],
            "custodianResource": "aws.distribution",
            "environment": "cde",
            "fromTimestamp": "1730759994",
            "policyName": "enterprise-dstrbtn-signed-url-newchange-disable",
            "testRunStatus": "PASS",
            "toTimestamp": "1730760735",
        },
        {
            "awsRegion": "us-east-1",
            "controlCategory": ["public access"],
            "controlId": ["SC-8.AWS.42"],
            "executionId": "04151428-72bc-404f-1516-6723dc2fdd19",
            "controlRisk": ["critical"],
            "custodianResource": "aws.distribution",
            "environment": "cde",
            "fromTimestamp": "1730757816",
            "policyName": "monitor-dstrbtn-default-url-cert-exception",
            "testRunStatus": "PASS",
            "toTimestamp": "1730758416",
        },
        {
            "awsRegion": "us-east-1",
            "controlCategory": ["public access"],
            "controlId": ["SC-8.AWS.42"],
            "executionId": "04151428-72bc-404f-2021-6723dc2fdd19",
            "controlRisk": ["critical"],
            "custodianResource": "aws.distribution",
            "environment": "cde",
            "fromTimestamp": "1730757186",
            "policyName": "monitor-dstrbtn-default-url-cert-compliant",
            "testRunStatus": "PASS",
            "toTimestamp": "1730757816",
        },
        {
            "awsRegion": "us-east-1",
            "controlCategory": ["public access"],
            "controlId": ["SC-8.AWS.42"],
            "executionId": "04151428-72bc-404f-2345-6723dc2fdd19",
            "controlRisk": ["critical"],
            "custodianResource": "aws.distribution",
            "environment": "cde",
            "fromTimestamp": "1730756126",
            "policyName": "enterprise-dstrbtn-default-url-cert-change-notify",
            "testRunStatus": "PASS",
            "toTimestamp": "1730757186",
        },
        {
            "awsRegion": "us-east-1",
            "controlCategory": ["public access"],
            "controlId": ["SC-8.AWS.42"],
            "executionId": "04151428-72bc-404f-2676-6723dc2fdd19",
            "controlRisk": ["critical"],
            "custodianResource": "aws.distribution",
            "environment": "cde",
            "fromTimestamp": "1730755494",
            "policyName": "enterprise-dstrbtn-default-url-cert-existing-notify",
            "testRunStatus": "PASS",
            "toTimestamp": "1730756125",
        },
        {
            "awsRegion": "us-east-1",
            "controlCategory": ["encryption at rest"],
            "controlId": ["SC-28.AWS.11"],
            "executionId": "04151428-72bc-404f-5675-6723dc2fdd19",
            "controlRisk": ["medium"],
            "custodianResource": "aws.sagemaker-notebook",
            "environment": "cde",
            "fromTimestamp": "1730697721",
            "policyName": "monitor-sgmkrntbk-kms-encryption-compliant",
            "testRunStatus": "PASS",
            "toTimestamp": "1730698286",
        },
        {
            "awsRegion": "us-east-1",
            "controlCategory": ["encryption at rest"],
            "controlId": ["SC-28.AWS.11"],
            "executionId": "04151428-72bc-404f-8934-6723dc2fdd19",
            "controlRisk": ["medium"],
            "custodianResource": "aws.sagemaker-notebook",
            "environment": "cde",
            "fromTimestamp": "1730697166",
            "policyName": "enterprise-sgmkrntbk-encryption-existing-notify",
            "testRunStatus": "PASS",
            "toTimestamp": "1730697721",
        },
    ],
    "nextRecordKey": "key",
}

API_RESPONSE_DATA_WITHOUT_NEXT_KEY = {
    "items": [
        {
            "awsRegion": "us-east-1",
            "controlCategory": ["public access"],
            "controlId": ["AC-3.AWS.80"],
            "executionId": "04151428-72bc-404f-9783-6723dc2fdd19",
            "controlRisk": ["critical"],
            "custodianResource": "aws.ebs-snapshot",
            "environment": "dev",
            "fromTimestamp": "1730762560",
            "policyName": "monitor-ebssnpsht-sharing-compliant",
            "testRunStatus": "FAIL",
            "toTimestamp": "1730762668",
        },
        {
            "awsRegion": "us-east-1",
            "controlCategory": ["public access"],
            "controlId": ["AC-3.AWS.18"],
            "executionId": "04151428-72bc-404f-9874-6723dc2fdd19",
            "controlRisk": ["critical"],
            "custodianResource": "aws.message-broker",
            "environment": "cde",
            "fromTimestamp": "1730765219",
            "policyName": "monitor-mssgbrkr-no-public-access-compliant",
            "testRunStatus": "FAIL",
            "toTimestamp": "1730766338",
        },
        {
            "awsRegion": "us-east-1",
            "controlCategory": ["public access"],
            "controlId": ["AC-3.AWS.18"],
            "executionId": "04151428-72bc-404f-5343-6723dc2fdd19",
            "controlRisk": ["critical"],
            "custodianResource": "aws.message-broker",
            "environment": "cde",
            "fromTimestamp": "1730764002",
            "policyName": "enterprise-mssgbrkr-no-public-delete",
            "testRunStatus": "FAIL",
            "toTimestamp": "1730765219",
        },
        {
            "awsRegion": "us-east-1",
            "controlCategory": ["public access"],
            "controlId": ["AC-3.AWS.18"],
            "executionId": "04151428-72bc-404f-6873-6723dc2fdd19",
            "controlRisk": ["critical"],
            "custodianResource": "aws.message-broker",
            "environment": "cde",
            "fromTimestamp": "1730762822",
            "policyName": "enterprise-mssgbrkr-no-public-tag",
            "testRunStatus": "PASS",
            "toTimestamp": "1730764002",
        },
        {
            "awsRegion": "us-east-1",
            "controlCategory": ["public access"],
            "controlId": ["SC-8.AWS.43"],
            "executionId": "04151428-72bc-404f-5433-6723dc2fdd19",
            "controlRisk": ["critical"],
            "custodianResource": "aws.distribution",
            "environment": "cde",
            "fromTimestamp": "1730760735",
            "policyName": "enterprise-dstrbtn-signed-url-newchange-disable",
            "testRunStatus": "FAIL",
            "toTimestamp": "1730762034",
        },
        {
            "awsRegion": "us-east-1",
            "controlCategory": ["public access"],
            "controlId": ["SC-8.AWS.43"],
            "executionId": "04151428-72bc-404f-8763-6723dc2fdd19",
            "controlRisk": ["critical"],
            "custodianResource": "aws.distribution",
            "environment": "cde",
            "fromTimestamp": "1730759994",
            "policyName": "enterprise-dstrbtn-signed-url-newchange-disable",
            "testRunStatus": "PASS",
            "toTimestamp": "1730760735",
        },
        {
            "awsRegion": "us-east-1",
            "controlCategory": ["public access"],
            "controlId": ["SC-8.AWS.42"],
            "executionId": "04151428-72bc-404f-6543-6723dc2fdd19",
            "controlRisk": ["critical"],
            "custodianResource": "aws.distribution",
            "environment": "cde",
            "fromTimestamp": "1730757816",
            "policyName": "monitor-dstrbtn-default-url-cert-exception",
            "testRunStatus": "PASS",
            "toTimestamp": "1730758416",
        },
        {
            "awsRegion": "us-east-1",
            "controlCategory": ["public access"],
            "controlId": ["SC-8.AWS.42"],
            "executionId": "04151428-72bc-404f-7564-6723dc2fdd19",
            "controlRisk": ["critical"],
            "custodianResource": "aws.distribution",
            "environment": "cde",
            "fromTimestamp": "1730757186",
            "policyName": "monitor-dstrbtn-default-url-cert-compliant",
            "testRunStatus": "PASS",
            "toTimestamp": "1730757816",
        },
        {
            "awsRegion": "us-east-1",
            "controlCategory": ["public access"],
            "controlId": ["SC-8.AWS.42"],
            "executionId": "04151428-72bc-404f-0987-6723dc2fdd19",
            "controlRisk": ["critical"],
            "custodianResource": "aws.distribution",
            "environment": "cde",
            "fromTimestamp": "1730756126",
            "policyName": "enterprise-dstrbtn-default-url-cert-change-notify",
            "testRunStatus": "PASS",
            "toTimestamp": "1730757186",
        },
        {
            "awsRegion": "us-east-1",
            "controlCategory": ["public access"],
            "controlId": ["SC-8.AWS.42"],
            "executionId": "04151428-72bc-404f-6534-6723dc2fdd19",
            "controlRisk": ["critical"],
            "custodianResource": "aws.distribution",
            "environment": "cde",
            "fromTimestamp": "1730755494",
            "policyName": "enterprise-dstrbtn-default-url-cert-existing-notify",
            "testRunStatus": "PASS",
            "toTimestamp": "1730756125",
        },
        {
            "awsRegion": "us-east-1",
            "controlCategory": ["encryption at rest"],
            "controlId": ["SC-28.AWS.11"],
            "executionId": "04151428-72bc-404f-5632-6723dc2fdd19",
            "controlRisk": ["medium"],
            "custodianResource": "aws.sagemaker-notebook",
            "environment": "cde",
            "fromTimestamp": "1730697721",
            "policyName": "monitor-sgmkrntbk-kms-encryption-compliant",
            "testRunStatus": "PASS",
            "toTimestamp": "1730698286",
        },
        {
            "awsRegion": "us-east-1",
            "controlCategory": ["encryption at rest"],
            "controlId": ["SC-28.AWS.11"],
            "executionId": "04151428-72bc-404f-6533-6723dc2fdd19",
            "controlRisk": ["medium"],
            "custodianResource": "aws.sagemaker-notebook",
            "environment": "cde",
            "fromTimestamp": "1730697166",
            "policyName": "enterprise-sgmkrntbk-encryption-existing-notify",
            "testRunStatus": "PASS",
            "toTimestamp": "1730697721",
        },
    ]
}

def generate_mock_response(content: Optional[dict] = None, status_code: int = 200):
    mock_response = Response()
    mock_response.status_code = status_code
    if content:
        mock_response._content = json.dumps(content).encode("utf-8")
    return mock_response


class Test_Automated_Monitoring(ConfigPipelineTestCase):
    # Tier 1 Unit Tests
    def test_tier_1_transform(self):
        # Test Tier 1 Transform
        with freeze_time("2024-11-05 12:09:00.021180"):
            tier_1_transform = transform.tier_1_transform_cloud_custodian(
                _tier_threshold_df(),
                _total_inventory_df(),
                _not_evaluated_df(),
            )
            pd.testing.assert_frame_equal(tier_1_transform, _tier_1_metric_output_df())

    def test_empty_dataframe(self):
        with freeze_time("2024-11-05 12:09:00.021180"):
            tier_1_transform = transform.tier_1_transform_cloud_custodian(
                _tier_threshold_df(),
                _tier_1_total_inventory_empty_df(),
                _tier_1_not_evaluated_empty_df(),
            )
            pd.testing.assert_frame_equal(tier_1_transform, _tier_1_empty_df_output())

    # Tier 2 Unit Tests

    @mock.patch(
        "pipelines.pl_automated_monitoring_cloud_custodian.pipeline.OauthApi",
    )
    @mock.patch(
        "pipelines.pl_automated_monitoring_cloud_custodian.pipeline.refresh",
        return_value="Bearer test_token",
    )
    def test_ipat_extract(self, _, mock_oauth_api):
        env = set_env_vars("qa")
        mock_responses = [
            generate_mock_response(API_RESPONSE_DATA_WITH_NEXT_KEY, 200),
            generate_mock_response(API_RESPONSE_DATA_WITHOUT_NEXT_KEY, 200),
        ]
        mock_api_connector = mock.Mock()
        mock_api_connector.send_request.side_effect = mock_responses
        mock_oauth_api.return_value = mock_api_connector
        with freeze_time("2024-11-05 12:09:00.021180"):
            df = pipeline.PLAmCloudCustodianPipeline(env)._ipat_extract(
                _tier_threshold_df(),
                _total_inventory_df(),
            )
            assert len(df) == 3
            pd.testing.assert_frame_equal(df, _tier_2_metric_output_df())

    @mock.patch(
        "pipelines.pl_automated_monitoring_cloud_custodian.pipeline.OauthApi",
    )
    @mock.patch(
        "pipelines.pl_automated_monitoring_cloud_custodian.pipeline.refresh",
        return_value="Bearer test_token",
    )
    def test_ipat_extract_empty(self, _, mock_oauth_api):
        env = set_env_vars("qa")
        mock_responses = [
            generate_mock_response(EMPTY_API_RESPONSE_DATA, 200),
        ]
        mock_api_connector = mock.Mock()
        mock_api_connector.send_request.side_effect = mock_responses
        mock_oauth_api.return_value = mock_api_connector
        with freeze_time("2024-11-05 12:09:00.021180"):
            df = pipeline.PLAmCloudCustodianPipeline(env)._ipat_extract(
                _tier_threshold_df(),
                _tier_1_total_inventory_empty_df(),
            )
            assert len(df) == 3
            pd.testing.assert_frame_equal(df, _tier_2_metric_output_empty_df())

    # Pipeline Run

    def test_run_nonprod(self):
        env = set_env_vars("qa")
        with mock.patch(
            "pipelines.pl_automated_monitoring_cloud_custodian.pipeline.PLAmCloudCustodianPipeline"
        ) as mock_pipeline:
            pipeline.run(env)
            mock_pipeline.assert_called_once_with(env)
            mock_pipeline.return_value.run.assert_called_once_with(
                load=True, dq_actions=True
            )

    def test_run_prod(self):
        env = set_env_vars("prod")
        with mock.patch(
            "pipelines.pl_automated_monitoring_cloud_custodian.pipeline.PLAmCloudCustodianPipeline"
        ) as mock_pipeline:
            pipeline.run(env)
            mock_pipeline.assert_called_once_with(env)
            mock_pipeline.return_value.run.assert_called_once_with(
                load=True, dq_actions=True
            )
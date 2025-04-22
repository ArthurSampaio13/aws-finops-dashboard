import csv
import json
import os
from datetime import date, datetime, timedelta
from typing import List, Optional, Tuple

from collections import defaultdict

from boto3.session import Session
from rich.console import Console

from aws_finops_dashboard.aws_client import get_account_id
from aws_finops_dashboard.types import BudgetInfo, CostData, EC2Summary, ProfileData

from collections import defaultdict

console = Console()


def get_cost_data(session: Session, time_range: Optional[int] = None) -> CostData:
    """
    Get cost data for an AWS account.

    Args:
        session: The boto3 session to use
        time_range: Optional time range in days for cost data (default: current month)
    """
    ce = session.client("ce")
    budgets = session.client("budgets", region_name="us-east-1")
    today = date.today()

    if time_range:
        end_date = today
        start_date = today - timedelta(days=time_range)
        previous_period_end = start_date - timedelta(days=1)
        previous_period_start = previous_period_end - timedelta(days=time_range)

    else:
        start_date = today.replace(day=1)
        end_date = today

        # Last calendar month
        previous_period_end = start_date - timedelta(days=1)
        previous_period_start = previous_period_end.replace(day=1)

    account_id = get_account_id(session)

    try:
        this_period = ce.get_cost_and_usage(
            TimePeriod={"Start": start_date.isoformat(), "End": end_date.isoformat()},
            Granularity="MONTHLY",
            Metrics=["UnblendedCost"],
        )
    except Exception as e:
        console.log(f"[yellow]Error getting current period cost: {e}[/]")
        this_period = {"ResultsByTime": [{"Total": {"UnblendedCost": {"Amount": 0}}}]}

    try:
        previous_period = ce.get_cost_and_usage(
            TimePeriod={
                "Start": previous_period_start.isoformat(),
                "End": previous_period_end.isoformat(),
            },
            Granularity="MONTHLY",
            Metrics=["UnblendedCost"],
        )
    except Exception as e:
        console.log(f"[yellow]Error getting previous period cost: {e}[/]")
        previous_period = {
            "ResultsByTime": [{"Total": {"UnblendedCost": {"Amount": 0}}}]
        }

    try:
        current_period_cost_by_service = ce.get_cost_and_usage(
            TimePeriod={"Start": start_date.isoformat(), "End": end_date.isoformat()},
            Granularity="DAILY" if time_range else "MONTHLY",
            Metrics=["UnblendedCost"],
            GroupBy=[{"Type": "DIMENSION", "Key": "SERVICE"}],
        )
    except Exception as e:
        console.log(f"[yellow]Error getting current period cost by service: {e}[/]")
        current_period_cost_by_service = {"ResultsByTime": [{"Groups": []}]}

    # Aggregate cost by service across all days
    aggregated_service_costs = defaultdict(float)

    for result in current_period_cost_by_service.get("ResultsByTime", []):
        for group in result.get("Groups", []):
            service = group["Keys"][0]
            amount = float(group["Metrics"]["UnblendedCost"]["Amount"])
            aggregated_service_costs[service] += amount

    # Reformat into groups by service
    aggregated_groups = [
        {
            "Keys": [service],
            "Metrics": {
                "UnblendedCost": {
                    "Amount": str(amount)
                }
            }
        }
        for service, amount in aggregated_service_costs.items()
    ]

    budgets_data: List[BudgetInfo] = []
    try:
        response = budgets.describe_budgets(AccountId=account_id)
        for budget in response["Budgets"]:
            budgets_data.append(
                {
                    "name": budget["BudgetName"],
                    "limit": float(budget["BudgetLimit"]["Amount"]),
                    "actual": float(budget["CalculatedSpend"]["ActualSpend"]["Amount"]),
                    "forecast": float(
                        budget["CalculatedSpend"]
                        .get("ForecastedSpend", {})
                        .get("Amount", 0.0)
                    )
                    or None,
                }
            )
    except Exception as e:
        console.log(f"[yellow]Error getting budget data: {e}[/]")
        pass

    current_period_cost = 0.0
    for period in this_period.get("ResultsByTime", []):
        if "Total" in period and "UnblendedCost" in period["Total"]:
            current_period_cost += float(period["Total"]["UnblendedCost"]["Amount"])

    previous_period_cost = 0.0
    for period in previous_period.get("ResultsByTime", []):
        if "Total" in period and "UnblendedCost" in period["Total"]:
            previous_period_cost += float(period["Total"]["UnblendedCost"]["Amount"])

    current_period_name = (
        f"Current {time_range} days cost" if time_range else "Current month's cost"
    )
    previous_period_name = f"Previous {time_range} days cost" if time_range else "Last month's cost"

    return {
        "account_id": account_id,
        "current_month": current_period_cost,
        "last_month": previous_period_cost,
        "current_month_cost_by_service": aggregated_groups,
        "budgets": budgets_data,
        "current_period_name": current_period_name,
        "previous_period_name": previous_period_name,
        "time_range": time_range,
        "current_period_start": start_date.isoformat(),
        "current_period_end": end_date.isoformat(),
        "previous_period_start": previous_period_start.isoformat(),
        "previous_period_end": previous_period_end.isoformat(),
    }


def process_service_costs(
    cost_data: CostData,
) -> Tuple[List[str], List[Tuple[str, float]]]:
    """Process and format service costs from cost data."""
    service_costs: List[str] = []
    service_cost_data: List[Tuple[str, float]] = []

    for group in cost_data["current_month_cost_by_service"]:
        if "Keys" in group and "Metrics" in group:
            service_name = group["Keys"][0]
            cost_amount = float(group["Metrics"]["UnblendedCost"]["Amount"])
            if cost_amount > 0.001:
                service_cost_data.append((service_name, cost_amount))

    service_cost_data.sort(key=lambda x: x[1], reverse=True)

    if not service_cost_data:
        service_costs.append("No costs associated with this account")
    else:
        for service_name, cost_amount in service_cost_data:
            service_costs.append(f"{service_name}: ${cost_amount:.2f}")

    return service_costs, service_cost_data

def categorize_aws_services(service_costs: List[Tuple[str, float]]) -> dict[str, float]:
    """
    Categorize AWS services into groups like compute, storage, networking, etc.
    
    Args:
        service_costs: List of tuples containing (service_name, cost)
        
    Returns:
        Dictionary mapping categories to their total costs
    """
    # Define service to category mapping
    service_categories = {
        # Compute
        "Amazon Elastic Compute Cloud": "Compute",
        "EC2 - Other": "Compute",
        "Amazon Elastic Container Service": "Compute",
        "Amazon EKS": "Compute",
        "AWS Lambda": "Compute",
        "Amazon Elastic Container Registry": "Compute",
        "AWS Fargate": "Compute",
        "Amazon Lightsail": "Compute",
        "EC2 Container Registry": "Compute",
        "Amazon Elastic Kubernetes Service": "Compute",
        "Amazon EC2 Container Service": "Compute",
        
        # Storage
        "Amazon Simple Storage Service": "Storage",
        "Amazon Elastic Block Store": "Storage",
        "Amazon Elastic File System": "Storage",
        "Amazon FSx": "Storage",
        "Amazon S3 Glacier": "Storage",
        "Storage Gateway": "Storage",
        "AWS Backup": "Storage",
        
        # Database
        "Amazon Relational Database Service": "Database",
        "Amazon DynamoDB": "Database",
        "Amazon ElastiCache": "Database",
        "Amazon Redshift": "Database",
        "Amazon Neptune": "Database",
        "Amazon DocumentDB": "Database",
        "Amazon Timestream": "Database",
        "Amazon Quantum Ledger Database": "Database",
        "Amazon Keyspaces": "Database",
        "Amazon Aurora": "Database",
        
        # Networking & Content Delivery
        "Amazon Virtual Private Cloud": "Networking",
        "Amazon CloudFront": "Networking",
        "Amazon Route 53": "Networking",
        "Elastic Load Balancing": "Networking",
        "AWS Direct Connect": "Networking",
        "Amazon API Gateway": "Networking",
        "Amazon VPC": "Networking",
        "AWS Global Accelerator": "Networking",
        "AWS Transit Gateway": "Networking",
        
        # Analytics
        "Amazon Athena": "Analytics",
        "Amazon EMR": "Analytics",
        "Amazon Kinesis": "Analytics",
        "Amazon Managed Streaming for Apache Kafka": "Analytics",
        "Amazon OpenSearch Service": "Analytics",
        "Amazon QuickSight": "Analytics",
        "AWS Glue": "Analytics",
        "Amazon Elasticsearch Service": "Analytics",
        "Amazon Data Firehose": "Analytics",
        
        # Machine Learning
        "Amazon SageMaker": "Machine Learning",
        "Amazon Comprehend": "Machine Learning",
        "Amazon Rekognition": "Machine Learning",
        "Amazon Polly": "Machine Learning",
        "Amazon Translate": "Machine Learning",
        "Amazon Lex": "Machine Learning",
        "Amazon Forecast": "Machine Learning",
        "Amazon Textract": "Machine Learning",
        
        # Security & Identity
        "AWS Key Management Service": "Security",
        "AWS WAF": "Security",
        "Amazon GuardDuty": "Security",
        "AWS Shield": "Security",
        "AWS Certificate Manager": "Security",
        "AWS Secrets Manager": "Security",
        "AWS Identity and Access Management": "Security",
        "AWS IAM": "Security",
        "Amazon Inspector": "Security",
        "AWS Directory Service": "Security",
        
        # Management & Governance
        "AWS CloudTrail": "Management",
        "Amazon CloudWatch": "Management",
        "AWS Config": "Management",
        "AWS Systems Manager": "Management",
        "AWS CloudFormation": "Management",
        "AWS Organizations": "Management",
        "AWS Control Tower": "Management",
        "AWS Trusted Advisor": "Management",
        "AWS Cost Explorer": "Management",
        
        # Developer Tools
        "AWS CodeBuild": "Developer Tools",
        "AWS CodeCommit": "Developer Tools",
        "AWS CodeDeploy": "Developer Tools",
        "AWS CodePipeline": "Developer Tools",
        "AWS CodeStar": "Developer Tools",
        "AWS X-Ray": "Developer Tools",
        
        # Application Integration
        "Amazon Simple Queue Service": "Integration",
        "Amazon Simple Notification Service": "Integration",
        "Amazon MQ": "Integration",
        "AWS Step Functions": "Integration",
        "Amazon AppFlow": "Integration",
        "Amazon EventBridge": "Integration",
        
        # Customer Engagement
        "Amazon Connect": "Customer Engagement",
        "Amazon Pinpoint": "Customer Engagement",
        "Amazon Simple Email Service": "Customer Engagement",
        
        # Support & Billing
        "AWS Support": "Support & Billing",
        "AWS Billing": "Support & Billing",
        "Tax": "Support & Billing"
    }
    
    # Initialize category totals
    category_totals = defaultdict(float)
    
    # Process each service cost
    for service_name, cost in service_costs:
        # Try to find an exact match
        category = service_categories.get(service_name, None)
        
        # If no exact match, try partial match
        if category is None:
            matched = False
            for known_service, cat in service_categories.items():
                if known_service.lower() in service_name.lower() or service_name.lower() in known_service.lower():
                    category = cat
                    matched = True
                    break
            
            # If still no match, categorize as Other
            if not matched:
                category = "Other"
        
        # Add cost to appropriate category
        category_totals[category] += cost
    
    return category_totals


def format_budget_info(budgets: List[BudgetInfo]) -> List[str]:
    """Format budget information for display."""
    budget_info: List[str] = []
    for budget in budgets:
        budget_info.append(f"{budget['name']} limit: ${budget['limit']}")
        budget_info.append(f"{budget['name']} actual: ${budget['actual']:.2f}")
    return budget_info


def format_ec2_summary(ec2_data: EC2Summary) -> List[str]:
    """Format EC2 instance summary for display."""
    ec2_summary_text: List[str] = []
    for state, count in sorted(ec2_data.items()):
        if count > 0:
            state_color = (
                "bright_green"
                if state == "running"
                else "bright_yellow" if state == "stopped" else "bright_cyan"
            )
            ec2_summary_text.append(f"[{state_color}]{state}: {count}[/]")

    if not ec2_summary_text:
        ec2_summary_text = ["No instances found"]

    return ec2_summary_text


def export_to_csv(
    data: List[ProfileData], 
    filename: str, 
    output_dir: Optional[str] = None,
    previous_period_dates: str = "N/A",
    current_period_dates: str = "N/A",
) -> Optional[str]:
    """Export dashboard data to a CSV file."""
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        base_filename = f"{filename}_{timestamp}.csv"

        if output_dir:
            os.makedirs(output_dir, exist_ok=True)
            output_filename = os.path.join(output_dir, base_filename)
        else:
            output_filename = base_filename

        previous_period_header = f"Cost for period\n({previous_period_dates})"
        current_period_header = f"Cost for period\n({current_period_dates})" 

        with open(output_filename, "w", newline="") as csvfile:
            fieldnames = [
                "CLI Profile",
                "AWS Account ID",
                previous_period_header,
                current_period_header,
                "Cost By Service",
                "Cost By Category",  # Nova coluna
                "Budget Status",
                "EC2 Instances",
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for row in data:

                services_data = "\n".join(
                    [
                        f"{service}: ${cost:.2f}"
                        for service, cost in row["service_costs"]
                    ]
                )
                
                category_costs = categorize_aws_services(row["service_costs"])
                categories_data = "\n".join(
                    [
                        f"{category}: ${cost:.2f}"
                        for category, cost in sorted(category_costs.items(), key=lambda x: x[1], reverse=True)
                    ]
                )

                budgets_data = (
                    "\n".join(row["budget_info"])
                    if row["budget_info"]
                    else "No budgets"
                )

                ec2_data_summary = "\n".join(
                    [
                        f"{state}: {count}"
                        for state, count in row["ec2_summary"].items()
                        if count > 0
                    ]
                )

                writer.writerow(
                    {
                        "CLI Profile": row["profile"],
                        "AWS Account ID": row["account_id"],
                        previous_period_header: f"${row['last_month']:.2f}",
                        current_period_header: f"${row['current_month']:.2f}",
                        "Cost By Service": services_data or "No costs",
                        "Cost By Category": categories_data or "No costs",  # Nova coluna
                        "Budget Status": budgets_data or "No budgets",
                        "EC2 Instances": ec2_data_summary or "No instances",
                    }
                )
        console.print(
            f"[bright_green]Exported dashboard data to {os.path.abspath(output_filename)}[/]"
        )
        return os.path.abspath(output_filename)
    except Exception as e:
        console.print(f"[bold red]Error exporting to CSV: {str(e)}[/]")
        return None


def export_to_json(
    data: List[ProfileData], filename: str, output_dir: Optional[str] = None
) -> Optional[str]:
    """Export dashboard data to a JSON file."""
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        base_filename = f"{filename}_{timestamp}.json"

        if output_dir:
            os.makedirs(output_dir, exist_ok=True)
            output_filename = os.path.join(output_dir, base_filename)
        else:
            output_filename = base_filename

        with open(output_filename, "w") as jsonfile:
            json.dump(data, jsonfile, indent=4)

        console.print(
            f"[bright_green]Exported dashboard data to {os.path.abspath(output_filename)}[/]"
        )
        return os.path.abspath(output_filename)
    except Exception as e:
        console.print(f"[bold red]Error exporting to JSON: {str(e)}[/]")
        return None

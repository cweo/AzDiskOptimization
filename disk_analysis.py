# Disk analysis script to recommend disk SKU based on disk throughput and IOPS usage (not taking into account SLA)
# Author: Johan Barthas
# Example usage (after az login):
# python disk_analysis.py --use_disks_df --PAYG_discount 0.17
import json
import argparse
import pandas as pd
# import multiprocessing
from tqdm import tqdm
from pathlib import Path
from azure.identity import DefaultAzureCredential
from azure.mgmt.subscription import SubscriptionClient
from modules.compute import get_list_disks_df, get_tier, get_lower_recommended_tier
from modules.metrics import get_disk_throughput_IOPS
from modules.helpers import power_of_two
from modules.pricing import get_tier_pricing, summarize


parser = argparse.ArgumentParser()
parser.add_argument("--use_disks_df", action="store_true",
                    help="Use saved disk_df.csv file if exists or pull and create it if not.")
parser.add_argument("--PAYG_discount", type=float, default=0,
                    help="Enteprise discount on PAYG between 0 and 1. Default is 0.")
parser.add_argument("--timerange_days_metrics", type=str, default=3,
                    help="Number of days to use for metrics. Default is 3.")
parser.add_argument("--interval_ISO_metrics", type=str, default="PT1M",
                    help="Interval ISO to use for metrics (granularity). Default is PT1M.")


def main(args):
    # Get token from Azure AD powershell - for example, use "Connect-AzAccount" to get token
    credential = DefaultAzureCredential()
    # Get list of disks
    path = Path("data/disks_df.pkl")
    if path.is_file() and args.use_disks_df:
        print("Using saved disk_df.pkl file.")
        disks_df = pd.read_pickle(path)
    else:
        subscription_client = SubscriptionClient(credential)
        disks_df = get_list_disks_df(credential, subscription_client)
        path.parent.mkdir(parents=True, exist_ok=True)
        disks_df.to_pickle(path)
    # print(disks_df.columns)
    # Get throughput and IOPS for each disk
    path_throughout_IOPS = Path("data/disks_throughput_IOPS_df.pkl")
    if path_throughout_IOPS.is_file() and args.use_disks_df:
        print("Using saved disks_throughput_IOPS_df.pkl file.")
        disks_throughput_IOPS_df = pd.read_pickle(path_throughout_IOPS)
    else:
        token = credential.get_token(
                "https://management.azure.com/.default").token
        list_disks_throughput_IOPS = []
        for _, disk in tqdm(disks_df.iterrows(), total=disks_df.shape[0], desc="Getting disk throughput and IOPS..."):
            throughput, IOPS = get_disk_throughput_IOPS(
                disk.id, token, interval_ISO=args.interval_ISO_metrics, timerange_days=args.timerange_days_metrics)
            list_disks_throughput_IOPS.append({
                "id": disk.id,
                "throughput": throughput,
                "IOPS": IOPS
            })

        disks_throughput_IOPS_df = pd.DataFrame(list_disks_throughput_IOPS)
        path_throughout_IOPS.parent.mkdir(parents=True, exist_ok=True)
        disks_throughput_IOPS_df.to_pickle(path_throughout_IOPS)
    # print(disks_throughput_IOPS_df.head())

    disk_skus = json.load(open("modules/disk_sku.json", "r"))
    STANDARD_HDD = disk_skus["STANDARD_HDD"]
    STANDARD_SSD = disk_skus["STANDARD_SSD"]
    PREMIUM_SSD = disk_skus["PREMIUM_SSD"]
    # Get current tier and size for each disk
    list_disks_tier_size = []
    for _, disk in disks_df.iterrows():
        list_disks_tier_size.append({
            "id": disk.id,
            "tier": get_tier(power_of_two(disk.disk_size_gb), disk.sku_name, STANDARD_HDD, STANDARD_SSD, PREMIUM_SSD),
            "size": disk.disk_size_gb
        })
    disks_tier_size_df = pd.DataFrame(list_disks_tier_size)
    # print(disks_tier_size_df.head())
    # Merge throughput and IOPS with tier and size
    disks_metrics_tier = disks_tier_size_df.merge(
        disks_throughput_IOPS_df, on="id")
    # print(disks_metrics_tier.head())
    # Get recommended tier and size for each disk
    list_disks_recommendation = []
    for _, disk in disks_metrics_tier.iterrows():
        list_disks_recommendation.append({
            "id": disk.id,
            "current_tier": disk.tier,
            "recommended_tier": get_lower_recommended_tier(disk.tier, disk.throughput, disk.IOPS, STANDARD_HDD, STANDARD_SSD),
            "iops": disk.IOPS,
        })
    disks_recommendation_df = pd.DataFrame(list_disks_recommendation)
    # disks_recommendation_df.to_csv("data/disks_recommendation.csv", index=False)
    disks_redundancy_recommendation_df = disks_recommendation_df.merge(
        disks_df[["id", "sku_name", "location"]], on="id")
    # print(disks_redundancy_recommendation_df.head())
    tier_location_pricing_df = pd.concat([disks_redundancy_recommendation_df[["current_tier", "location"]].rename(columns={"current_tier": "tier"}, inplace=False), disks_redundancy_recommendation_df[[
                                         "recommended_tier", "location"]].rename(columns={"recommended_tier": "tier"}, inplace=False)]).drop_duplicates()

    path_tier_pricing = Path("data/tier_pricing_df.pkl")
    if path_tier_pricing.is_file() and args.use_disks_df:
        print("Using saved tier_pricing_df.pkl file.")
        tier_pricing_df = pd.read_pickle(path_tier_pricing)
    else:
        token = credential.get_token(
            "https://management.azure.com/.default").token
        list_tier_pricing = []
        for _, item in tqdm(tier_location_pricing_df.iterrows(), total=tier_location_pricing_df.shape[0], desc="Getting pricing for current and recommended tier..."):
            fixed_pricing, variable_pricing = get_tier_pricing(
                item.tier, item.location, redundancy="LRS")
            list_tier_pricing.append({
                "tier": item.tier,
                "location": item.location,
                "fixed_pricing": fixed_pricing,
                "variable_pricing": variable_pricing
            })
        tier_pricing_df = pd.DataFrame(list_tier_pricing)
        path_tier_pricing.parent.mkdir(parents=True, exist_ok=True)
        tier_pricing_df.to_pickle(path_tier_pricing)
    # Merge pricing with recommendation
    disks_recommendation_pricing_df = disks_redundancy_recommendation_df[["id", "current_tier", "recommended_tier", "location", "iops"]].merge(tier_pricing_df, left_on=["current_tier", "location"], right_on=[
                                                                                                                                               "tier", "location"], how="left").drop(columns=["tier"]).rename(columns={"fixed_pricing": "current_fixed_pricing", "variable_pricing": "current_variable_pricing"})
    disks_recommendation_pricing_df = disks_recommendation_pricing_df.merge(tier_pricing_df, left_on=["recommended_tier", "location"], right_on=["tier", "location"], how="left").drop(
        columns=["tier"]).rename(columns={"fixed_pricing": "recommended_fixed_pricing", "variable_pricing": "recommended_variable_pricing"})
    disks_recommendation_pricing_df["estimated_current_variable_pricing"] = disks_recommendation_pricing_df[[
        "iops", "current_variable_pricing"]].apply(lambda x: x.iops / 10000 * 720 * 3600 * x.current_variable_pricing, axis=1)
    disks_recommendation_pricing_df["estimated_recommended_variable_pricing"] = disks_recommendation_pricing_df[[
        "iops", "recommended_variable_pricing"]].apply(lambda x: x.iops / 10000 * 720 * 3600 * x.recommended_variable_pricing, axis=1)
    # print(disks_recommendation_pricing_df.head())
    # Display recommendations
    summarize(disks_recommendation_pricing_df,
              PAYG_discount=args.PAYG_discount)


if __name__ == "__main__":
    args = parser.parse_args()
    main(args)

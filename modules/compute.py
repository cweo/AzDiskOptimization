from tqdm import tqdm
import pandas as pd
from azure.identity import DefaultAzureCredential
from azure.mgmt.subscription import SubscriptionClient
from azure.mgmt.compute import ComputeManagementClient


def get_list_disks_df(credential: DefaultAzureCredential, subscription_client: SubscriptionClient) -> pd.DataFrame:
    disk_list = []

    def schema_lambda(x): return {
        "id": x.id,
        "name": x.name,
        "subscription_id": x.id.split('/')[2],
        "rg_id": x.id.split('/')[4],
        "location": x.location,
        "managed_by": x.managed_by,
        "managed_by_extended": x.managed_by_extended,
        "disk_state": x.disk_state,
        "sku_name": x.sku.name,
        "tier": x.tier,
        "disk_size_gb": x.disk_size_gb,
        "disk_iops_read_write": x.disk_iops_read_write,
        "disk_m_bps_read_write": x.disk_m_bps_read_write,
        "tags": x.tags
    }
    sub_list = list(subscription_client.subscriptions.list())
    for subscription in tqdm(sub_list, desc="Searching disks in subscriptions...", ncols=len(sub_list)):
        # print(subscription.display_name, subscription.id)
        compute_client = ComputeManagementClient(
            credential, subscription.subscription_id)
        disk_list.extend(
            map(schema_lambda, compute_client.disks.list())
        )
    disk_df = pd.DataFrame.from_dict(disk_list)
    return disk_df


def get_tier(disk_size: int, sku_name: str, STANDARD_HDD: dict, STANDARD_SSD: dict, PREMIUM_SSD: dict) -> str:
    if sku_name == "Standard_LRS":
        allowed_disk_sizes = list(
            map(lambda x: x["size"], STANDARD_HDD.values()))
        allowed_valued = min(
            max(min(allowed_disk_sizes), disk_size), max(allowed_disk_sizes))
        return list(STANDARD_HDD.keys())[allowed_disk_sizes.index(allowed_valued)]
    elif sku_name == "StandardSSD_LRS":
        allowed_disk_sizes = list(
            map(lambda x: x["size"], STANDARD_SSD.values()))
        allowed_valued = min(
            max(min(allowed_disk_sizes), disk_size), max(allowed_disk_sizes))
        return list(STANDARD_SSD.keys())[allowed_disk_sizes.index(allowed_valued)]
    elif sku_name == "Premium_LRS":
        allowed_disk_sizes = list(
            map(lambda x: x["size"], PREMIUM_SSD.values()))
        allowed_valued = min(
            max(min(allowed_disk_sizes), disk_size), max(allowed_disk_sizes))
        return list(PREMIUM_SSD.keys())[allowed_disk_sizes.index(allowed_valued)]
    else:
        raise ValueError(f"{sku_name} is not a supported disk SKU.")


def get_lower_recommended_tier(current_tier: str, throughput: int, IOPS: int, STANDARD_HDD: dict, STANDARD_SSD: dict, minimal_tier: str = "STANDARD_HDD"):
    # Limit1: burstable Bandwidth and IOPS is not taken into account
    # Limit2: Only lower tiers are considered and not upper tiers
    if current_tier[0] == 'P':
        standard_hdd_tier_equivalent = f"S{max(4, int(current_tier[1:]))}"
        standard_ssd_tier_equivalent = f"E{current_tier[1:]}"
        if (minimal_tier == "STANDARD_HDD") and throughput <= STANDARD_HDD[standard_hdd_tier_equivalent]["throughput"] and IOPS <= STANDARD_HDD[standard_hdd_tier_equivalent]["IOPS"]:
            return standard_hdd_tier_equivalent
        elif (minimal_tier == "STANDARD_HDD" or minimal_tier == "STANDARD_SSD") and throughput <= STANDARD_SSD[standard_ssd_tier_equivalent]["throughput"] and IOPS <= STANDARD_SSD[standard_ssd_tier_equivalent]["IOPS"]:
            return standard_ssd_tier_equivalent
        else:
            return current_tier
    elif current_tier[0] == 'E':
        standard_hdd_tier_equivalent = f"S{max(4, int(current_tier[1:]))}"
        if minimal_tier == "STANDARD_HDD" and throughput <= STANDARD_HDD[standard_hdd_tier_equivalent]["throughput"] and IOPS <= STANDARD_HDD[standard_hdd_tier_equivalent]["IOPS"]:
            return standard_hdd_tier_equivalent
        else:
            return current_tier
    return current_tier

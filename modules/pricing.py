import json
import requests
import pandas as pd
from operator import itemgetter
from pathlib import Path
from datetime import date


def get_pricing_REST_filters(location: str, tier: str, redundancy: str = "LRS"):
    sku_name = f"{tier} {redundancy}"
    disk_type_filter = " or ".join(
        [
            "productName eq 'Standard HDD Managed Disks'",
            "productName eq 'Standard SSD Managed Disks'",
            "productName eq 'Premium SSD Managed Disks'"
        ]
    )
    filters = {
        "armRegionName": location
    }
    return " and ".join([f"{key} eq '{value}'" for key, value in filters.items()]) + f" and ({disk_type_filter})" + f" and skuName eq '{sku_name}'"


def get_tier_pricing(tier: str, location: str, redundancy: str = "LRS"):
    response = requests.get(
        f"https://prices.azure.com/api/retail/prices?$filter={get_pricing_REST_filters(location, tier, redundancy)}")
    retail_prices = json.loads(response.content)
    try:
        retail_prices = retail_prices['Items']
    except KeyError as e:
        print(
            f"Error {e} when searching for pricing for tier {tier} in region {location}")
        return 0, 0
    indexer = list(map(itemgetter('meterName', 'type'), retail_prices))
    if tier.startswith("P"):
        try:
            price = retail_prices[indexer.index(
                (f"{tier} {redundancy} Disk", "Consumption"))]["retailPrice"]
        except ValueError as e:
            print(
                f"Error {e} when searching for pricing for tier {tier} in region {location}")
            price = 0
        return price, 0
    else:
        try:
            price_fixed = retail_prices[indexer.index(
                (f"{tier} Disks", "Consumption"))]["retailPrice"]
        except ValueError as e:
            print(
                f"Error {e} when searching for fixed pricing for tier {tier} in region {location}")
            price_fixed = 0
        try:
            price_variable = retail_prices[indexer.index(
                ("Disk Operations", "Consumption"))]["retailPrice"]
        except ValueError as e:
            print(
                f"Error {e} when searching for variable pricing for tier {tier} in region {location}")
            price_variable = 0

        return price_fixed, price_variable


def summarize(df: pd.DataFrame, save_csv: bool = True, csv_name: str = "disk_recommendations", fill: int = 150, PAYG_discount: float = 10**-15) -> None:
    if PAYG_discount < 0 or PAYG_discount > 1:
        raise ValueError("PAYG discount must be a value between 0 and 1")
    if save_csv:
        csv_path = Path(f"output/{csv_name}_{date.today()}.csv")
        if not(csv_path.is_file()):
            print(f"Saving recommendation results to {csv_path}.")
            csv_path.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(csv_path)
    cost_agg_df = df.agg({'current_fixed_pricing': ['sum'], 'estimated_current_variable_pricing': [
                         'sum'], 'recommended_fixed_pricing': ['sum'], 'estimated_recommended_variable_pricing': ['sum']})
    fixed_current_cost = round(
        cost_agg_df.current_fixed_pricing.iloc[0] * (1 - PAYG_discount), 2)
    variable_current_cost = round(
        cost_agg_df.estimated_current_variable_pricing.iloc[0] * (1 - PAYG_discount), 2)
    fixed_projected_cost = round(
        cost_agg_df.recommended_fixed_pricing.iloc[0] * (1 - PAYG_discount), 2)
    variable_projected_cost = round(
        cost_agg_df.estimated_recommended_variable_pricing.iloc[0] * (1 - PAYG_discount), 2)
    current_cost = fixed_current_cost + variable_current_cost
    projected_cost = fixed_projected_cost + variable_projected_cost
    print("#"*fill)
    print("# DISCLAIMER: The cost estimates are based on the current pricing of the Azure services and are subject to change.".ljust(fill - 2), "#")
    print("# DISCLAIMER: The recommendations does not align with WAF Reliability as we do not take into account wanted SLAs for machines.".ljust(fill - 2), "#")
    print("# DISCLAIMER: Implementing these recommendations may not be possible due to other constraints such as VM size, OS, etc.".ljust(fill - 2), "#")
    print(f"# USING {PAYG_discount}% DISCOUNT".ljust(fill - 2), "#")
    print(f"# ESTIMATED TOTAL: Current cost: ${current_cost} / month || After recommendations: ${projected_cost} / month -> ${round(projected_cost - current_cost, 2)} ({100 * round((projected_cost - current_cost) / current_cost, 2)}%) / month".ljust(fill - 2), "#")
    print(f"# ESTIMATED FIXED: Current cost: ${fixed_current_cost} / month || After recommendations: ${fixed_projected_cost} / month -> ${round(fixed_projected_cost - fixed_current_cost, 2)} ({100 * round((fixed_projected_cost - fixed_current_cost) / fixed_current_cost, 2)}%) / month".ljust(fill - 2), "#")
    print(f"# ESTIMATED VARIABLE: Current cost: ${variable_current_cost} / month || After recommendations: ${variable_projected_cost} / month -> ${round(variable_projected_cost - variable_current_cost, 2)} ({100 * round((variable_projected_cost - variable_current_cost) / variable_current_cost, 2)}%) / month".ljust(fill - 2), "#")
    # print(f"# ESTIMATED (fixed + variable) cost after recommendations implemented: ${projected_cost} / month".ljust(fill - 2), "#")
    # print(f"# ESTIMATED (fixed + variable) savings: ${projected_cost - current_cost} / month -> {100 * round((projected_cost - current_cost) / current_cost, 2)}%".ljust(fill - 2), "#")
    print("#"*fill)

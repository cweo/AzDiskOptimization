import requests
import json
import numpy as np
from typing import List
from datetime import datetime, timedelta, timezone
from requests.exceptions import HTTPError


def sum_metrics(timeseries_avg_max_dict, perf_metric: str = "IOPS"):
    if perf_metric == "throughput_mbps":
        return (timeseries_avg_max_dict["Composite Disk Read Bytes/sec"]["average"] + timeseries_avg_max_dict["Composite Disk Write Bytes/sec"]["average"]) / (1024 * 1024), (timeseries_avg_max_dict["Composite Disk Read Bytes/sec"]["maximum"] + timeseries_avg_max_dict["Composite Disk Write Bytes/sec"]["maximum"]) / (1024 * 1024)
    elif perf_metric == "IOPS":
        return timeseries_avg_max_dict["Composite Disk Read Operations/sec"]["average"] + timeseries_avg_max_dict["Composite Disk Write Operations/sec"]["average"], timeseries_avg_max_dict["Composite Disk Read Operations/sec"]["maximum"] + timeseries_avg_max_dict["Composite Disk Write Operations/sec"]["maximum"]
    raise ValueError(
        "{perf_metric} is not a supported type of metric, allowed values: IOPS, throughput_mbps.")


def get_timeseries_average_max(timeseries_dict: List[dict]) -> tuple:
    def get_avg_max_lambda(x): return (x["average"], x["maximum"])
    try:
        timeseries_dict = timeseries_dict[0]["data"]
    except Exception as e:
        # print(f"Issue gathering timerseries in get_timeseries_average_max, exception: {e}")
        return {"average": 0, "maximum": 0}
    try:
        timeseries_array = np.array(list(map(get_avg_max_lambda, timeseries_dict)))
    except Exception as e:
        # print(f"Issue parsing average_max metric, exception: {e}")
        return {"average": 0, "maximum": 0}
    return {"average": timeseries_array.mean(), "maximum": timeseries_array.max()}


def get_disk_throughput_IOPS(disk_id: str, token: str, interval_ISO: str = "PT1M", timerange_days: int = 1):
    start_date = (datetime.now(timezone.utc) - timedelta(days=timerange_days)
                  ).replace(second=0, microsecond=0).isoformat().split("+")[0] + "Z"
    end_date = datetime.now(timezone.utc).replace(
        second=0, microsecond=0).isoformat().split("+")[0] + "Z"
    METRICS_NAME = "Composite Disk Read Bytes/sec,Composite Disk Read Operations/sec,Composite Disk Write Bytes/sec,Composite Disk Write Operations/sec"
    try:
        response = requests.get(
            f"https://management.azure.com{disk_id}/providers/Microsoft.Insights/metrics?api-version=2018-01-01&timespan={start_date}/{end_date}&aggregation=Average,maximum&metricnames={METRICS_NAME}&interval={interval_ISO}", headers={"Authorization": f"Bearer {token}"})

        # If the response was successful, no Exception will be raised
        response.raise_for_status()

    except HTTPError as http_err:
        print(f'HTTP error occurred: {http_err}')  # Python 3.10
        return 0, 0
    except Exception as err:
        print(f'Other error occurred: {err}')  # Python 3.10
        return 0, 0
    else:
        metrics = json.loads(response.content)
        timeseries_avg_max = [{"name": x["name"]["value"], "value": get_timeseries_average_max(
            x["timeseries"])} for i, x in enumerate(metrics["value"])]
        timeseries_avg_max_dict = {
            item["name"]: item["value"] for item in timeseries_avg_max}
        throughput_mbps = sum_metrics(
            timeseries_avg_max_dict, "throughput_mbps")
        IOPS = sum_metrics(timeseries_avg_max_dict, "IOPS")
        if throughput_mbps[1] and IOPS[1]:
            return throughput_mbps[1], IOPS[1]  # return max values
        return 0, 0

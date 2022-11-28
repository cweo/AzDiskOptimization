# AzDiskOptimization
Disk analysis script to recommend disk SKU based on disk throughput and IOPS usage

Example usage:
```bash
  az login
  python disk_analysis.py --use_disks_df --PAYG_discount 0.17 
```

Arguments:
* ```--use_disks_df```: Use saved disk_df.csv file if exists or pull and create it if not.
* ```--PAYG_discount```: Enteprise discount on PAYG between 0 and 1. Default is 0.
* ```--timerange_days_metrics```: Number of days to use for metrics. Default is 3.
* ```--interval_ISO_metrics```: Interval ISO to use for metrics (granularity). Default is PT1M.


Current known limitations:
* Does not take into account SLA to be met by applications using these disks (WAF Reliability).
* Recommendation is done by comparing throughput and IOPS compared to available Premium SSD, Standard SSD and Standard HDD, not using the __generated price__.
* Recommendations are done only to lower tier.
* Premium SSD v2 is not supported.

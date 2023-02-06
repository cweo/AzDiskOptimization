# Disable warning write-host
$WarningPreference = 'SilentlyContinue'

# Connect-AzAccount
# Get all subscriptions accessible by current identity
$subscriptions = Get-AzSubscription


# Get all the disks in all subscriptions and their 
$subscriptions | ForEach-Object {
  Set-AzContext -SubscriptionId $_.Id > $null # avoid writing host of Set-AzContext
  $disks = Get-AzDisk

  $disks | ForEach-Object -Parallel {
    $metrics = Get-AzMetric -ResourceId $_.Id -AggregationType Maximum -ResultType Data -MetricName "Composite Disk Read Bytes/sec","Composite Disk Read Operations/sec","Composite Disk Write Bytes/sec","Composite Disk Write Operations/sec" -StartTime (Get-Date).AddDays(-14) -EndTime (Get-Date) -TimeGrain 00:01:00
    $operations = $metrics | Where-Object {$_.Name.Value -like "*Operations*"}
    $operationsData = @()
    for ($i = 0; $i -lt $operations.Count; $i++) {
      $operationsData += $operations[$i].Timeseries.Data.Maximum
    }
    $IOPS = ($operationsData | Measure -Max | % {$_.Maximum})
    $throughput = $metrics | Where-Object {$_.Name.Value -like "*Bytes*"}
    $throughputData = @()
    for ($i = 0; $i -lt $throughput.Count; $i++) {
      $throughputData += $throughput[$i].Timeseries.Data.Maximum
    }
    $throughputMBPS = ($throughputData | Measure -Max | % {$_.Maximum}) / 1024 / 1024
    Write-Output @{
      diskId = $_.Id
      tier = $_.Tier
      sku = $_.Sku.Name
      size = $_.DiskSizeGB
      max_IOPS = $IOPS
      max_throughput = $throughputMBPS
    }
    # Write-Output "$($disk.Id) | tier: $($disk.Tier) | sku: $($disk.Sku.Tier) | size: $($disk.DiskSizeGB)GB | max_IOPS: $($IOPS) | max_throughput: $($throughputMBPS)"
  } 
    
} | Select-Object -Property diskId,tier,sku,size, max_IOPS, max_throughput |
Export-Csv -Path .\data\diskMetrics.csv -NoTypeInformation


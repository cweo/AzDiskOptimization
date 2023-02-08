# Disable warning write-host
$WarningPreference = 'SilentlyContinue'

# Connect-AzAccount
# Get all subscriptions accessible by current identity
$subscriptions = Get-AzSubscription


# Get all the disks in all subscriptions



Write-Host "Getting all disks in all subscriptions..."
$disks = $subscriptions | ForEach-Object -Parallel {
  Set-AzContext -SubscriptionId $_.Id > $null # avoid writing host of Set-AzContext
  $disks_sub = Get-AzDisk
  $disks_sub
} 
    
Write-Output "Total number of disks: $($disks.Count)"


$disks | ForEach-Object -Parallel {
  $metrics = Get-AzMetric -ResourceId $_.Id -AggregationType Maximum -ResultType Data -MetricName "Composite Disk Read Bytes/sec","Composite Disk Read Operations/sec","Composite Disk Write Bytes/sec","Composite Disk Write Operations/sec" -StartTime (Get-Date).AddDays(-14) -EndTime (Get-Date) -TimeGrain 01:00:00
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
    id = $_.Id
    tier = $_.Tier
    sku = $_.Sku.Name
    size = $_.DiskSizeGB
    IOPS = $IOPS
    throughput = $throughputMBPS
    location = $_.Location
  }} | Select-Object -Property id,tier,sku,size, IOPS, throughput, location |
Export-Csv -Path .\data\diskMetrics.csv -NoTypeInformation


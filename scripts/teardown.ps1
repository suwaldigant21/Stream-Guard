<#
.SYNOPSIS
    StreamGuard teardown: stop local Python processes and Docker containers.
.DESCRIPTION
    - Kills producer.py, pyspark_consumer.py and uvicorn (mock_vendor_api)
      processes for this project, plus any Spark (SparkSubmit) JVMs they spawned.
    - Runs `docker compose down` (add -Volumes to also wipe Redpanda topic data).
.NOTES
    Safe: only matches processes whose command line contains producer.py,
    pyspark_consumer or mock_vendor_api:app (or a SparkSubmit JVM), so unrelated
    Python/uvicorn/java processes are untouched.
    Datasets stay on disk (CSV + Parquet cache), so a restart is fast.
#>
param([switch]$Volumes)

$ErrorActionPreference = "Continue"
Push-Location $PSScriptRoot

$patterns = @("producer.py", "mock_vendor_api:app", "pyspark_consumer")

$procs = Get-CimInstance Win32_Process | Where-Object {
    $cmd = $_.CommandLine
    foreach ($pat in $patterns) {
        if ($cmd -and $cmd.Contains($pat)) { return $true }
    }
    return $false
}

if ($procs) {
    foreach ($p in $procs) {
        Write-Host "Stopping PID $($p.ProcessId): $($p.Name)"
        Stop-Process -Id $p.ProcessId -Force
    }
} else {
    Write-Host "No StreamGuard processes running."
}

# PySpark spawns Java (SparkSubmit) JVMs that don't carry our script name -
# stop those too so no orphans are left behind.
$sparkJvms = Get-CimInstance Win32_Process -Filter "Name = 'java.exe'" | Where-Object {
    $_.CommandLine -match "SparkSubmit"
}
foreach ($p in $sparkJvms) {
    Write-Host "Stopping Spark JVM PID $($p.ProcessId)"
    Stop-Process -Id $p.ProcessId -Force
}

if ($Volumes) {
    docker compose down -v
} else {
    docker compose down
}

Pop-Location

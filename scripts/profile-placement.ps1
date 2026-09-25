param(
    [string]$BaseUrl = "http://127.0.0.1:8090",
    [string]$AdminToken = "",
    [int]$Seconds = 30,
    [int]$Concurrency = 4,
    [string]$OutputDir = "profiles"
)

$ErrorActionPreference = "Stop"
$stamp = Get-Date -Format "yyyyMMdd-HHmmss"
$dir = Join-Path $OutputDir $stamp
New-Item -ItemType Directory -Force -Path $dir | Out-Null

$headers = @{}
if ($AdminToken -ne "") {
    $headers["X-Takl-Admin-Token"] = $AdminToken
}

$stopFile = Join-Path $dir "stop"
$jobs = @()
for ($i = 0; $i -lt $Concurrency; $i++) {
    $jobs += Start-Job -ArgumentList $BaseUrl, $stopFile -ScriptBlock {
        param($Url, $Stop)
        while (-not (Test-Path $Stop)) {
            try {
                Invoke-WebRequest -Uri "$Url/api/v1/runners/best" -UseBasicParsing | Out-Null
            } catch {
            }
        }
    }
}

try {
    $cpu = Join-Path $dir "cpu.pprof"
    $heap = Join-Path $dir "heap.pprof"
    Invoke-WebRequest -Uri "$BaseUrl/debug/pprof/profile?seconds=$Seconds" -Headers $headers -OutFile $cpu -UseBasicParsing
    Invoke-WebRequest -Uri "$BaseUrl/debug/pprof/heap" -Headers $headers -OutFile $heap -UseBasicParsing
} finally {
    New-Item -ItemType File -Force -Path $stopFile | Out-Null
    $jobs | Stop-Job | Out-Null
    $jobs | Remove-Job | Out-Null
}

$cpuPath = Join-Path $dir "cpu.pprof"
$heapPath = Join-Path $dir "heap.pprof"
$cpuSvg = Join-Path $dir "cpu.svg"
$heapSvg = Join-Path $dir "heap.svg"

if (Get-Command go -ErrorAction SilentlyContinue) {
    & go tool pprof -svg -output $cpuSvg $cpuPath
    & go tool pprof -svg -output $heapSvg $heapPath
}

Write-Output $dir
Write-Output "go tool pprof -http=:0 $cpuPath"

# ──────────────────────────────────────────────────────────────────────────────
# run-locally.ps1
#
# Local equivalent of the .github/workflows/main.yml evening-pipeline job.
# Reads .env (loaded by main.py via python-dotenv), installs deps, then runs
# python main.py. Mirrors a manual workflow_dispatch with run_mode=manual.
#
# Usage:
#   .\run-locally.ps1                 # full run
#   .\run-locally.ps1 -SkipInstall    # skip pip install
#   .\run-locally.ps1 -CheckOnly      # just validate .env, don't run main.py
# ──────────────────────────────────────────────────────────────────────────────
[CmdletBinding()]
param(
    [switch]$SkipInstall,
    [switch]$CheckOnly
)

$ErrorActionPreference = "Stop"
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $scriptDir

Write-Host ""
Write-Host "═══════════════════════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host " Stock Reviewer — Local Run (mirrors main.yml evening-pipeline)" -ForegroundColor Cyan
Write-Host "═══════════════════════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host ""

# ── 1. Sanity: .env present ──────────────────────────────────────────────────
if (-not (Test-Path .env)) {
    Write-Host "ERROR: .env file not found in $scriptDir" -ForegroundColor Red
    Write-Host ""
    Write-Host "Create one by copying .env.example:" -ForegroundColor Yellow
    Write-Host "  Copy-Item .env.example .env" -ForegroundColor Yellow
    Write-Host "  notepad .env    # fill in your secrets" -ForegroundColor Yellow
    exit 1
}
Write-Host "[OK]  .env found"

# ── 2. Sanity: python ────────────────────────────────────────────────────────
try {
    $pyVer = & python --version 2>&1
    Write-Host "[OK]  $pyVer"
} catch {
    Write-Host "ERROR: python not on PATH" -ForegroundColor Red
    exit 1
}

# ── 3. Validate required keys in .env ────────────────────────────────────────
$envContent = Get-Content .env -Raw
$required = @("TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID", "GROQ_API_KEY_1")
$missing = @()
foreach ($key in $required) {
    # match KEY= followed by optional whitespace and at least one non-whitespace char
    if ($envContent -notmatch "(?m)^\s*$key\s*=\s*\S") {
        $missing += $key
    }
}
if ($missing.Count -gt 0) {
    Write-Host ""
    Write-Host "ERROR: Required keys missing or empty in .env:" -ForegroundColor Red
    foreach ($k in $missing) { Write-Host "  - $k" -ForegroundColor Red }
    Write-Host ""
    Write-Host "Open .env in notepad and fill these in:" -ForegroundColor Yellow
    Write-Host "  notepad .env" -ForegroundColor Yellow
    exit 1
}
Write-Host "[OK]  Required keys present (TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, GROQ_API_KEY_1)"

if ($CheckOnly) {
    Write-Host ""
    Write-Host "Check-only mode — .env validated. Not running main.py." -ForegroundColor Green
    exit 0
}

# ── 4. Install dependencies (unless -SkipInstall) ────────────────────────────
if (-not $SkipInstall) {
    Write-Host ""
    Write-Host "Installing/updating dependencies from requirements.txt..." -ForegroundColor Cyan
    python -m pip install --upgrade pip --quiet
    pip install -r requirements.txt --quiet
    if ($LASTEXITCODE -ne 0) {
        Write-Host "ERROR: pip install failed" -ForegroundColor Red
        exit 1
    }
    Write-Host "[OK]  Dependencies installed"
} else {
    Write-Host "[SKIP] pip install (--SkipInstall)"
}

# ── 5. Run main.py ───────────────────────────────────────────────────────────
Write-Host ""
Write-Host "═══════════════════════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host " Running main.py ..." -ForegroundColor Cyan
Write-Host "═══════════════════════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host ""

$startTime = Get-Date
python main.py
$exitCode = $LASTEXITCODE
$duration = (Get-Date) - $startTime

Write-Host ""
Write-Host "═══════════════════════════════════════════════════════════════════" -ForegroundColor Cyan
Write-Host (" Finished in {0:hh\:mm\:ss}" -f $duration) -ForegroundColor Cyan
Write-Host "═══════════════════════════════════════════════════════════════════" -ForegroundColor Cyan

# ── 6. Summary of generated artifacts ────────────────────────────────────────
Write-Host ""
Write-Host "Generated artifacts:" -ForegroundColor Cyan
$artifacts = @(
    "shadow_master.xlsx",
    "trade_tracker.json",
    "confidence_history.json",
    "gate_memory.json",
    "tracker.json",
    "portfolio.json",
    # Phase C7e (2026-07-02) additions
    "weekly_metrics.json",
    "watchlist_persist.json",
    "backtest_summary.json",
    "backtest_summary.txt",
    "backtest_raw.csv",
    "backtest_win_rate_by_confidence.csv",
    "threshold_recommendation.txt"
)
foreach ($f in $artifacts) {
    if (Test-Path $f) {
        $info = Get-Item $f
        $sizeKB = [math]::Round($info.Length / 1KB, 1)
        Write-Host ("  [OK]  {0,-32}  {1,8} KB   {2}" -f $f, $sizeKB, $info.LastWriteTime.ToString("HH:mm:ss")) -ForegroundColor Green
    } else {
        Write-Host ("  [--]  {0}" -f $f) -ForegroundColor DarkGray
    }
}

# ── 7. Show any error_log.txt head ───────────────────────────────────────────
if (Test-Path error_log.txt) {
    Write-Host ""
    Write-Host "error_log.txt (first 20 lines):" -ForegroundColor Yellow
    Get-Content error_log.txt -TotalCount 20
}

if ($exitCode -ne 0) {
    Write-Host ""
    Write-Host "main.py exited with code $exitCode" -ForegroundColor Red
}
exit $exitCode

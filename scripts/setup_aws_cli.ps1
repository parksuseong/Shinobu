param(
    [string]$Profile = "default",
    [string]$Region = "ap-northeast-2"
)

$ErrorActionPreference = "Stop"
$UserScripts = ""

function Test-Command($Name) {
    return [bool](Get-Command $Name -ErrorAction SilentlyContinue)
}

function Get-PythonUserScriptsPath() {
    $ver = python -c "import sys; print(f'{sys.version_info.major}{sys.version_info.minor}')" 2>$null
    if (-not $ver) { return "" }
    return Join-Path $env:APPDATA ("Python\Python{0}\Scripts" -f $ver.Trim())
}

$UserScripts = Get-PythonUserScriptsPath

if (-not (Test-Command "aws")) {
    if (Test-Command "winget") {
        Write-Host "Installing AWS CLI via winget..."
        winget install -e --id Amazon.AWSCLI --accept-package-agreements --accept-source-agreements
    }
}

if (-not (Test-Command "aws")) {
    Write-Host "Installing awscli via pip --user fallback..."
    python -m pip install --user awscli
    $UserScripts = Get-PythonUserScriptsPath
    if (Test-Path $UserScripts) {
        $env:PATH = "$UserScripts;$env:PATH"
    }
}

Write-Host "AWS CLI version:"
if (Test-Command "aws") {
    aws --version
} elseif ($UserScripts -and (Test-Path (Join-Path $UserScripts "aws.cmd"))) {
    & (Join-Path $UserScripts "aws.cmd") --version
} else {
    throw "AWS CLI command not found after install attempt."
}

Write-Host ""
Write-Host "Starting aws configure for profile '$Profile'..."
Write-Host "Prepare: AWS Access Key ID, Secret Access Key."
aws configure --profile $Profile

if ($LASTEXITCODE -ne 0) {
    throw "aws configure failed."
}

aws configure set region $Region --profile $Profile
Write-Host "Configured profile '$Profile' with region '$Region'."

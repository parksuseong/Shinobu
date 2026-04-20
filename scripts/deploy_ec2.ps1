param()

$ErrorActionPreference = "Stop"

function Get-ConfigValue($Name, $Default = "") {
    $v = [Environment]::GetEnvironmentVariable($Name)
    if ([string]::IsNullOrWhiteSpace($v)) { return $Default }
    return $v
}

$HostName = Get-ConfigValue "EC2_HOST"
$UserName = Get-ConfigValue "EC2_USER" "ec2-user"
$Port = Get-ConfigValue "EC2_PORT" "22"
$KeyPath = Get-ConfigValue "EC2_SSH_KEY_PATH"
$AppDir = Get-ConfigValue "EC2_APP_DIR"
$Branch = Get-ConfigValue "EC2_BRANCH" "master"
$PreCmd = Get-ConfigValue "EC2_PRE_DEPLOY_COMMAND"
$DeployCmd = Get-ConfigValue "EC2_DEPLOY_COMMAND"
$PostCmd = Get-ConfigValue "EC2_POST_DEPLOY_COMMAND"

if ([string]::IsNullOrWhiteSpace($HostName)) { throw "EC2_HOST is required." }
if ([string]::IsNullOrWhiteSpace($KeyPath)) { throw "EC2_SSH_KEY_PATH is required." }
if ([string]::IsNullOrWhiteSpace($AppDir)) { throw "EC2_APP_DIR is required." }
if ([string]::IsNullOrWhiteSpace($DeployCmd)) { throw "EC2_DEPLOY_COMMAND is required." }
if (-not (Test-Path $KeyPath)) { throw "SSH key file not found: $KeyPath" }

$remoteParts = @()
$remoteParts += "set -e"
$remoteParts += "cd '$AppDir'"
$remoteParts += "git fetch --all --prune"
$remoteParts += "git checkout '$Branch'"
$remoteParts += "git pull origin '$Branch'"
if (-not [string]::IsNullOrWhiteSpace($PreCmd)) { $remoteParts += $PreCmd }
$remoteParts += $DeployCmd
if (-not [string]::IsNullOrWhiteSpace($PostCmd)) { $remoteParts += $PostCmd }
$remote = ($remoteParts -join " && ")

Write-Host ("Deploying to {0}@{1}:{2}" -f $UserName, $HostName, $Port)
ssh -i $KeyPath -p $Port -o StrictHostKeyChecking=accept-new "$UserName@$HostName" $remote

if ($LASTEXITCODE -ne 0) {
    throw "EC2 deploy failed with exit code $LASTEXITCODE"
}

Write-Host "EC2 deploy completed."

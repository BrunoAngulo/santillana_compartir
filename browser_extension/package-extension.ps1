param(
    [string]$SourceDir = "santillana_session_helper",
    [string]$OutputZip = "santillana_session_helper.zip"
)

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$sourcePath = Join-Path $root $SourceDir
$outputPath = Join-Path $root $OutputZip

if (-not (Test-Path $sourcePath)) {
    throw "No existe la carpeta de la extension: $sourcePath"
}

if (Test-Path $outputPath) {
    Remove-Item -LiteralPath $outputPath -Force
}

Compress-Archive -Path (Join-Path $sourcePath '*') -DestinationPath $outputPath -Force
Write-Output "ZIP generado en: $outputPath"

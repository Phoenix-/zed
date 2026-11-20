param(
    [Parameter(Mandatory = $true)][string]$RunnerTemp,
    [string]$StandardCompiler = "${env:ProgramFiles(x86)}\Inno Setup 6\ISCC.exe",
    [string]$InstallerPath
)
$ErrorActionPreference = 'Stop'
if (Test-Path -LiteralPath $StandardCompiler -PathType Leaf) {
    Write-Output $StandardCompiler
    return
}

$expectedHash = 'A549929319F9120716677C685535CA4671E4B937B5FDC87E2585F32D8EB30E8C'
$sourceUrl = 'https://github.com/jrsoftware/issrc/releases/download/is-6_7_4/innosetup-6.7.4.exe'
$work = Join-Path ([IO.Path]::GetFullPath($RunnerTemp)) ('gorynych-inno-6.7.4-' + [guid]::NewGuid().ToString('N'))
New-Item -ItemType Directory -Path $work -ErrorAction Stop | Out-Null
if (-not $InstallerPath) {
    $InstallerPath = Join-Path $work 'innosetup-6.7.4.exe'
    Invoke-WebRequest -UseBasicParsing -Uri $sourceUrl -OutFile $InstallerPath
}
$stream = [IO.File]::OpenRead([IO.Path]::GetFullPath($InstallerPath))
$sha = [Security.Cryptography.SHA256]::Create()
try {
    $actualHash = [BitConverter]::ToString($sha.ComputeHash($stream)).Replace('-', '')
} finally {
    $stream.Dispose()
    $sha.Dispose()
}
if ($actualHash -ne $expectedHash) { throw "Inno Setup 6.7.4 SHA-256 mismatch: $actualHash" }
$compilerDir = Join-Path $work 'compiler'
$arguments = @('/PORTABLE=1', '/CURRENTUSER', '/VERYSILENT', '/SUPPRESSMSGBOXES', '/NORESTART', '/NOICONS', "/DIR=`"$compilerDir`"")
$process = Start-Process -FilePath $InstallerPath -ArgumentList $arguments -Wait -PassThru -WindowStyle Hidden
if ($process.ExitCode -ne 0) { throw "Portable Inno Setup failed with exit code $($process.ExitCode)" }
$compiler = Join-Path $compilerDir 'ISCC.exe'
if (-not (Test-Path -LiteralPath $compiler -PathType Leaf)) { throw "Portable Inno Setup did not provide $compiler" }
Write-Output $compiler

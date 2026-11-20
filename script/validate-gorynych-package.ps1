param(
    [Parameter(Mandatory = $true)][string]$Stage,
    [Parameter(Mandatory = $true)][string]$RemoteServer
)
$ErrorActionPreference = 'Stop'
foreach ($relative in @('zed.exe', 'conpty.dll', 'x64\OpenConsole.exe', 'bin\zed.exe', 'bin\zed', 'tools\auto_update_helper.exe')) {
    $file = Join-Path $Stage $relative
    if (-not (Test-Path -LiteralPath $file -PathType Leaf) -or (Get-Item -LiteralPath $file).Length -eq 0) { throw "Missing or empty package file: $file" }
}
Add-Type -AssemblyName System.IO.Compression.FileSystem
$archive = [IO.Compression.ZipFile]::OpenRead((Join-Path $Stage 'remote-server\zed-remote-server-windows-x86_64.zip'))
try {
    $entries = @($archive.Entries)
    if ($entries.Count -ne 1 -or $entries[0].FullName -ne 'remote_server.exe') { throw 'Invalid remote server archive contents' }
    $sha = [Security.Cryptography.SHA256]::Create()
    try {
        $stream = $entries[0].Open()
        try { $archiveHash = [Convert]::ToBase64String($sha.ComputeHash($stream)) } finally { $stream.Dispose() }
        $stream = [IO.File]::OpenRead($RemoteServer)
        try { $buildHash = [Convert]::ToBase64String($sha.ComputeHash($stream)) } finally { $stream.Dispose() }
        if ($archiveHash -ne $buildHash) { throw 'Remote server differs from the built executable' }
    } finally { $sha.Dispose() }
} finally { $archive.Dispose() }
Write-Host 'Gorynych package stage validated'

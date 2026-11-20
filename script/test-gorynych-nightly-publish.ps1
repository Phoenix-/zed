$ErrorActionPreference = 'Stop'
$workflow = Get-Content -Raw (Join-Path $PSScriptRoot '..\.github\workflows\nightly.yml')
$match = [regex]::Match($workflow, '(?ms)^      - name: Publish the nightly prerelease\r?\n.*?^        run: \|\r?\n(?<body>(?:^          .*\r?\n|^\r?\n|^          \r?\n)+)')
if (-not $match.Success) { throw 'Cannot find nightly publish step' }
$body = [regex]::Replace($match.Groups['body'].Value, '(?m)^          ', '')
$script:calls = [Collections.Generic.List[string]]::new()
$script:deleted = [Collections.Generic.List[string]]::new()
function gh {
    $arguments = @($args | ForEach-Object { [string]$_ })
    $script:calls.Add(($arguments -join ' '))
    $global:LASTEXITCODE = 0
    if ($arguments[0] -eq 'api' -and $arguments -contains '--paginate') {
        $now = [DateTimeOffset]::UtcNow
        "900`tzed-gorynych-setup-900-1.exe`t$($now.AddDays(-1).ToString('o'))"
        "901`tzed-gorynych-setup-901-1.exe`t$($now.AddDays(-2).ToString('o'))"
        for ($index = 1; $index -le 53; $index++) {
            "$index`tzed-gorynych-setup-$index-1.exe`t$($now.AddDays(-30-$index).ToString('o'))"
        }
        "999`tunrelated.exe`t$($now.AddDays(-100).ToString('o'))"
    } elseif ($arguments[0] -eq 'api' -and $arguments -contains 'DELETE') {
        $endpoint = $arguments | Where-Object { $_ -match '/releases/assets/[0-9]+$' }
        if (-not $endpoint) { throw 'Invalid delete endpoint' }
        $script:deleted.Add(($endpoint -split '/')[-1])
    } elseif ($arguments[0] -eq 'api' -and $arguments -contains '.id') {
        '123'
    }
}
$env:GITHUB_REPOSITORY = 'Phoenix-/zed'
$env:GITHUB_SHA = '1234567890'
$env:GITHUB_SERVER_URL = 'https://github.com'
$env:GITHUB_RUN_ID = '1000'
$env:GITHUB_RUN_ATTEMPT = '1'
$env:GORYNYCH_BUILD_ID = '1000-1'
$env:ASSET = 'zed-gorynych-windows-x64.zip'
$env:RUNNER_TEMP = [IO.Path]::GetTempPath()
Invoke-Expression $body
$actual = @($script:deleted | Sort-Object { [int]$_ }) -join ','
if ($actual -ne '49,50,51,52,53') { throw "Unexpected deleted IDs: $actual" }
$installerUploads = @($script:calls | Where-Object { $_ -match '^release upload nightly .*zed-gorynych-setup-1000-1\.exe' })
$portableUploads = @($script:calls | Where-Object { $_ -match '^release upload nightly .*zed-gorynych-windows-x64\.zip' })
if ($installerUploads.Count -ne 1 -or $portableUploads.Count -ne 1) { throw "Unexpected upload counts: installer=$($installerUploads.Count), portable=$($portableUploads.Count)" }
'Nightly publish regression passed: deleted 49-53, kept recent and unrelated assets, uploaded each artifact once'

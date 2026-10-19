# Install respan-gateway from its GitHub release, on Windows.
#
#   irm https://raw.githubusercontent.com/respanai/respan-gateway/main/install.ps1 | iex
#
# $env:RESPAN_GATEWAY_VERSION picks a release (e.g. v0.1.0; the latest by default),
# $env:RESPAN_GATEWAY_INSTALL_DIR where the binary goes
# (%LOCALAPPDATA%\Programs\respan-gateway by default, added to the user PATH).
# The archive is checked against the release's SHA256SUMS before it is unpacked.

$ErrorActionPreference = 'Stop'
$ProgressPreference = 'SilentlyContinue'

$Repo = 'respanai/respan-gateway'
$Bin = 'respan-gateway'

# Only an x86_64 build is published; Windows on ARM runs it emulated.
$Target = 'x86_64-pc-windows-msvc'
if (-not [Environment]::Is64BitOperatingSystem) {
    throw 'respan-gateway needs 64-bit Windows'
}

[Net.ServicePointManager]::SecurityProtocol = [Net.ServicePointManager]::SecurityProtocol -bor [Net.SecurityProtocolType]::Tls12

$Version = $env:RESPAN_GATEWAY_VERSION
if (-not $Version) {
    # The tag the releases/latest page redirects to, without the rate-limited API.
    $request = [Net.WebRequest]::Create("https://github.com/$Repo/releases/latest")
    $request.Method = 'HEAD'
    $response = $request.GetResponse()
    $Version = $response.ResponseUri.Segments[-1]
    $response.Close()
    if ($Version -notlike 'v*') {
        throw "no release of $Repo is published yet; build from source (see the README)"
    }
}
if ($Version -notlike 'v*') { $Version = "v$Version" }

$Dir = $env:RESPAN_GATEWAY_INSTALL_DIR
if (-not $Dir) { $Dir = Join-Path $env:LOCALAPPDATA "Programs\$Bin" }

$Name = "$Bin-$Version-$Target"
$Base = "https://github.com/$Repo/releases/download/$Version"
$Tmp = Join-Path ([IO.Path]::GetTempPath()) ([Guid]::NewGuid())
New-Item -ItemType Directory $Tmp | Out-Null

try {
    Write-Host "Downloading $Name"
    $Zip = Join-Path $Tmp "$Name.zip"
    $Sums = Join-Path $Tmp 'SHA256SUMS'
    Invoke-WebRequest -UseBasicParsing "$Base/$Name.zip" -OutFile $Zip
    Invoke-WebRequest -UseBasicParsing "$Base/SHA256SUMS" -OutFile $Sums

    $pattern = "^([0-9a-fA-F]{64}) \*?$([Regex]::Escape("$Name.zip"))$"
    $found = Select-String -Path $Sums -Pattern $pattern | Select-Object -First 1
    if (-not $found) { throw "$Name.zip is not in SHA256SUMS" }
    $want = $found.Matches[0].Groups[1].Value
    $got = (Get-FileHash -Algorithm SHA256 $Zip).Hash
    if ($got -ne $want) { throw "checksum mismatch for $Name.zip" }

    Expand-Archive -Path $Zip -DestinationPath $Tmp -Force
    New-Item -ItemType Directory -Force $Dir | Out-Null
    Copy-Item -Force (Join-Path $Tmp "$Name\$Bin.exe") (Join-Path $Dir "$Bin.exe")
}
finally {
    Remove-Item -Recurse -Force $Tmp -ErrorAction SilentlyContinue
}

Write-Host "Installed $Bin $Version to $Dir\$Bin.exe"

$UserPath = [Environment]::GetEnvironmentVariable('Path', 'User')
if (($UserPath -split ';') -notcontains $Dir) {
    $NewPath = if ($UserPath) { "$($UserPath.TrimEnd(';'));$Dir" } else { $Dir }
    [Environment]::SetEnvironmentVariable('Path', $NewPath, 'User')
    $env:Path = "$env:Path;$Dir"
    Write-Host "Added $Dir to your user PATH (open a new terminal to pick it up)."
}
Write-Host "Run '$Bin init' to write a config, then '$Bin' to start it."

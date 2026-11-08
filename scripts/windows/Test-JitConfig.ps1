# Validate without altering the string ultimately passed to run.cmd.
function Test-JitConfig([string] $Value) {
    if ($Value -notmatch '^[A-Za-z0-9+/=_-]+$') { return $false }
    if (($Value -match '[+/]' -and $Value -match '[-_]') -or
        $Value -notmatch '^[A-Za-z0-9+/_-]+={0,2}$') { return $false }
    $unpadded = $Value.TrimEnd('=')
    $padding = $Value.Length - $unpadded.Length
    $remainder = $unpadded.Length % 4
    if ($remainder -eq 1) { return $false }
    $requiredPadding = (4 - $remainder) % 4
    if ($padding -ne 0 -and ($padding -ne $requiredPadding -or $requiredPadding -eq 0)) { return $false }
    $normalized = $unpadded.Replace('-', '+').Replace('_', '/') + ('=' * $requiredPadding)
    try {
        $bytes = [Convert]::FromBase64String($normalized)
        # Strict UTF-8, then require a nonempty JSON object as the controller does.
        $utf8 = New-Object System.Text.UTF8Encoding($false, $true)
        $json = $utf8.GetString($bytes)
        if ($json -notmatch '^\s*\{') { return $false }
        $object = ConvertFrom-Json -InputObject $json -ErrorAction Stop
        return ($null -ne $object -and @($object.PSObject.Properties).Count -gt 0)
    } catch { return $false }
}

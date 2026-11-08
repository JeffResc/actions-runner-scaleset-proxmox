$ErrorActionPreference = 'Stop'
. (Join-Path $PSScriptRoot 'Test-JitConfig.ps1')
function Encode([string] $value) { [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($value)) }
$good = Encode '{"x":"ÿÿ"}'
$cases = @(
    @{ value = $good; expected = $true; name = 'standard padded' },
    @{ value = $good.TrimEnd('='); expected = $true; name = 'standard unpadded' },
    @{ value = $good.Replace('+','-').Replace('/','_'); expected = $true; name = 'url padded' },
    @{ value = $good.Replace('+','-').Replace('/','_').TrimEnd('='); expected = $true; name = 'url unpadded' },
    @{ value = (Encode '{}'); expected = $false; name = 'empty object' },
    @{ value = (Encode '[]'); expected = $false; name = 'array' },
    @{ value = (Encode '{invalid}'); expected = $false; name = 'bad json' },
    @{ value = 'abc=def'; expected = $false; name = 'invalid padding' },
    @{ value = 'ab+_'; expected = $false; name = 'mixed alphabets' },
    @{ value = "$good`n"; expected = $false; name = 'newline' }
)
foreach ($case in $cases) {
    if ((Test-JitConfig $case.value) -ne $case.expected) { throw "Failed validation case: $($case.name)" }
}
Write-Host "Passed $($cases.Count) JIT validation cases."

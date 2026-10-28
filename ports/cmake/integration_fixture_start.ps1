param(
    [Parameter(Mandatory)][string]$Executable,
    [Parameter(Mandatory)][string]$WorkingDirectory,
    [Parameter(Mandatory)][string]$StandardOutput,
    [Parameter(Mandatory)][string]$StandardError,
    [string]$Argument = ''
)

$ErrorActionPreference = 'Stop'

# ShellExecute isolates the engine from CTest's inherited capture pipes.
# The command wrapper redirects only the engine's own output files.
$start = [System.Diagnostics.ProcessStartInfo]::new()
$start.FileName = $env:ComSpec
$start.Arguments = if ($Argument -eq '') {
    '/d /s /c ""{0}" > "{1}" 2> "{2}""' -f $Executable, $StandardOutput, $StandardError
} else {
    '/d /s /c ""{0}" "{1}" > "{2}" 2> "{3}""' -f $Executable, $Argument, $StandardOutput, $StandardError
}
$start.WorkingDirectory = $WorkingDirectory
$start.UseShellExecute = $true
$start.WindowStyle = [System.Diagnostics.ProcessWindowStyle]::Hidden
$launcher = [System.Diagnostics.Process]::Start($start)
$engineName = [System.IO.Path]::GetFileName($Executable)

try {
    for ($attempt = 0; $attempt -lt 200; $attempt++) {
        $child = Get-CimInstance Win32_Process -Filter "ParentProcessId = $($launcher.Id)" |
            Where-Object Name -EQ $engineName | Select-Object -First 1
        if ($child) {
            Write-Output $child.ProcessId
            return
        }
        if ($launcher.HasExited) {
            # The caller checks the process and reports the captured engine error.
            Write-Output $launcher.Id
            return
        }
        Start-Sleep -Milliseconds 25
    }
    throw 'Engine process did not start'
} catch {
    if (-not $launcher.HasExited) {
        & taskkill /PID $launcher.Id /T /F | Out-Null
    }
    throw
}

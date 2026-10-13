[CmdletBinding()]
param(
    [string]$PostgresBin = 'C:/Program Files/PostgreSQL/18/bin',
    [string]$RedisExe = 'C:/Redis/redis-server.exe',
    [string]$Bun = 'bun',
    [int]$DatabasePort = 55459,
    [int]$RedisPort = 56459,
    [int]$ApiPort = 55122,
    [ValidatePattern('^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$')]
    [string]$EdgePlatformId = '00000000-0000-7000-8000-000000000001',
    [string[]]$TestFiles = @('system-orm-integration.ts', 'business-orm-integration.ts', 'auth-http-sse-integration.ts', 'device-http-sse-integration.ts', 'protocol-offset-integration.ts'),
    [switch]$UploadRecovery,
    [switch]$ReplayCounterMigration,
    [switch]$PacketDebugMigration,
    [switch]$IndustrialProtocolsMigration,
    [switch]$DeadLetterQueryMigration,
    [switch]$OpenMqttMigration
)
$ErrorActionPreference = 'Stop'
$repository = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$build = Join-Path $repository 'build'
$fixture = Join-Path $build ('system-orm-fixture-' + [Guid]::NewGuid().ToString('N'))
$data = Join-Path $fixture 'postgres'
$backend = Join-Path $build 'Release/iot-engine.exe'
foreach ($file in @($backend, $RedisExe, (Join-Path $PostgresBin 'initdb.exe'), (Join-Path $PostgresBin 'pg_ctl.exe'))) {
    if (!(Test-Path -LiteralPath $file -PathType Leaf)) { throw "Missing test dependency: $file" }
}
foreach ($port in @($DatabasePort,$RedisPort,$ApiPort)) {
    $listener = [Net.Sockets.TcpListener]::new([Net.IPAddress]::Loopback, $port)
    try { $listener.Start() } finally { $listener.Stop() }
}
New-Item -ItemType Directory -Path $fixture | Out-Null
$apiProcess = $null
$redisProcess = $null
$oldPath = $env:Path
$oldDatabaseUrl = $env:ARCHITECTURE_DATABASE_URL
$oldApiBase = $env:TEST_BASE_URL
$oldEdgePlatformId = $env:TEST_EDGE_PLATFORM_ID
$oldRedisUrl = $env:ARCHITECTURE_REDIS_URL
$oldMigrationState = $env:ARCHITECTURE_MIGRATION_STATE
function Stop-OwnedProcess($Process, [string]$ExpectedPath) {
    if (!$Process) { return }
    $current = Get-Process -Id $Process.Id -ErrorAction SilentlyContinue
    if ($current -and $current.StartTime -eq $Process.StartTime -and
        [IO.Path]::GetFullPath($current.Path) -eq [IO.Path]::GetFullPath($ExpectedPath)) {
        Stop-Process -Id $Process.Id -Force
        $current.WaitForExit(10000) | Out-Null
    }
}
function Start-FixtureApi([string]$Label, [string]$ExpectedFailure = '') {
    $executable = Join-Path $fixture 'iot-engine.exe'
    $stdout = Join-Path $fixture ("api-$Label.out")
    $stderr = Join-Path $fixture ("api-$Label.err")
    $process = Start-Process -FilePath $executable -WorkingDirectory $fixture -WindowStyle Hidden -PassThru -RedirectStandardOutput $stdout -RedirectStandardError $stderr
    try {
        for ($attempt = 0; $attempt -lt 80; $attempt++) {
            $process.Refresh()
            if ($process.HasExited) {
                $failureText = [IO.File]::ReadAllText($stdout) + [IO.File]::ReadAllText($stderr)
                if ($ExpectedFailure -and $process.ExitCode -ne 0 -and $failureText -match $ExpectedFailure) { return $process }
                throw "Test API exited unexpectedly in $Label; inspect $fixture"
            }
            if (!$ExpectedFailure) {
                $probe = [Net.Sockets.TcpClient]::new()
                try { $probe.Connect('127.0.0.1',$ApiPort); return $process } catch { } finally { $probe.Dispose() }
            }
            Start-Sleep -Milliseconds 250
        }
        throw "Test API did not reach expected state in $Label; inspect $fixture"
    } catch {
        Stop-OwnedProcess $process $executable
        throw
    }
}
function Invoke-CounterMigrationPhase([string]$Phase) {
    $migrationTest = if ($OpenMqttMigration) { 'tests/open-mqtt-migration-integration.ts' } elseif ($DeadLetterQueryMigration) { 'tests/dead-letter-query-migration-integration.ts' } elseif ($IndustrialProtocolsMigration) { 'tests/industrial-protocol-migration-integration.ts' } elseif ($PacketDebugMigration) { 'tests/packet-debug-migration-integration.ts' } else { 'tests/replay-counter-migration-integration.ts' }
    & $Bun run (Join-Path $repository $migrationTest) $Phase
    if ($LASTEXITCODE -ne 0) { throw "Migration phase $Phase failed; inspect $fixture" }
}
try {
    & (Join-Path $PostgresBin 'initdb.exe') -D $data -U architecture_test --auth=trust -E UTF8 | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'Test database initialization failed.' }
    Add-Content -LiteralPath (Join-Path $data 'postgresql.conf') -Value "shared_preload_libraries = 'timescaledb'"
    $pgArguments = '-D "{0}" -l "{1}" -o "-p {2} -h 127.0.0.1" -w start' -f $data,(Join-Path $fixture 'postgres.log'),$DatabasePort
    $launcher = Start-Process -FilePath (Join-Path $PostgresBin 'pg_ctl.exe') -ArgumentList $pgArguments -WindowStyle Hidden -PassThru
    if (!$launcher.WaitForExit(30000)) { throw 'Test PostgreSQL launcher timed out.' }
    if ($launcher.ExitCode -ne 0) { throw "Test PostgreSQL startup failed; see $fixture" }
    & (Join-Path $PostgresBin 'createdb.exe') -h 127.0.0.1 -p $DatabasePort -U architecture_test iot_architecture
    if ($LASTEXITCODE -ne 0) { throw 'Test database creation failed.' }
    Set-Content -LiteralPath (Join-Path $fixture 'redis.conf') -Encoding ascii -Value "bind 127.0.0.1`nport $RedisPort`nsave `"`"`nappendonly no"
    # A relative config path also works with the Cygwin Redis distribution.
    $redisProcess = Start-Process -FilePath $RedisExe -ArgumentList 'redis.conf' -WorkingDirectory $fixture -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $fixture 'redis.out') -RedirectStandardError (Join-Path $fixture 'redis.err')
    Copy-Item -LiteralPath $backend -Destination (Join-Path $fixture 'iot-engine.exe')
    if ($UploadRecovery) {
        $uploadDirectory = Join-Path $fixture 'firmware'
        New-Item -ItemType Directory -Path $uploadDirectory | Out-Null
        Set-Content -LiteralPath (Join-Path $uploadDirectory '00000000-0000-4000-8000-000000000004.upload') -Value 'abandoned' -Encoding ascii
        Set-Content -LiteralPath (Join-Path $uploadDirectory '00000000-0000-4000-8000-000000000005.bin') -Value 'preserved' -Encoding ascii
        Set-Content -LiteralPath (Join-Path $uploadDirectory 'unrelated.upload') -Value 'unrelated' -Encoding ascii
    }

    @"
JWT_SECRET=architecture-test-only-access-secret-000000000
JWT_REFRESH_SECRET=architecture-test-only-refresh-secret-00000000
DB_HOST=127.0.0.1
DB_PORT=$DatabasePort
DB_USERNAME=architecture_test
DB_PASSWORD=
DB_DATABASE=iot_architecture
REDIS_HOST=127.0.0.1
REDIS_PORT=$RedisPort
HOST=127.0.0.1
PORT=$ApiPort
SERVICE_WORKERS=2
COLLECTOR_WORKERS=1
GB28181_ENABLED=false
VPN_HUB_ENABLED=false
EDGE_PUBLIC_BASE_URL=http://127.0.0.1:$ApiPort
EDGE_PLATFORM_ID=$EdgePlatformId
"@ | Set-Content -LiteralPath (Join-Path $fixture '.env') -Encoding ascii
    $env:Path = (Join-Path $build 'Release') + ';' + $oldPath
    $env:ARCHITECTURE_DATABASE_URL = "postgres://architecture_test@127.0.0.1:$DatabasePort/iot_architecture"
    $env:TEST_BASE_URL = "http://127.0.0.1:$ApiPort"
    $env:TEST_EDGE_PLATFORM_ID = $EdgePlatformId
    $env:ARCHITECTURE_REDIS_URL = "redis://127.0.0.1:$RedisPort"
    $apiProcess = Start-Process -FilePath (Join-Path $fixture 'iot-engine.exe') -WorkingDirectory $fixture -WindowStyle Hidden -PassThru -RedirectStandardOutput (Join-Path $fixture 'api.out') -RedirectStandardError (Join-Path $fixture 'api.err')
    $ready = $false
    for ($attempt = 0; $attempt -lt 60; $attempt++) {
        $apiProcess.Refresh(); $redisProcess.Refresh()
        if ($apiProcess.HasExited -or $redisProcess.HasExited) { throw "Test process exited; inspect logs in $fixture" }
        try {
            $probe = [Net.Sockets.TcpClient]::new()
            $probe.Connect('127.0.0.1',$ApiPort)
            $ready = $true
        } catch { } finally { if ($probe) { $probe.Dispose() } }
        if ($ready) { break }
        Start-Sleep -Milliseconds 250
    }
    if (!$ready) { throw "Test API did not start; inspect logs in $fixture" }
    foreach ($test in $TestFiles) {
        & $Bun run (Join-Path $repository ('tests/' + $test))
        if ($LASTEXITCODE -ne 0) { throw "$test failed; logs retained at $fixture" }
    }
    if ($ReplayCounterMigration -or $PacketDebugMigration -or $IndustrialProtocolsMigration -or $DeadLetterQueryMigration -or $OpenMqttMigration) {
        $env:ARCHITECTURE_MIGRATION_STATE = Join-Path $fixture 'migration-state.json'
        Invoke-CounterMigrationPhase 'initial'
        Stop-OwnedProcess $apiProcess (Join-Path $fixture 'iot-engine.exe')
        $apiProcess = Start-FixtureApi 'restart'
        Invoke-CounterMigrationPhase 'restart'
        Stop-OwnedProcess $apiProcess (Join-Path $fixture 'iot-engine.exe')
        Invoke-CounterMigrationPhase 'prepare-upgrade'
        $apiProcess = Start-FixtureApi 'upgrade'
        Invoke-CounterMigrationPhase 'upgraded'
        Stop-OwnedProcess $apiProcess (Join-Path $fixture 'iot-engine.exe')
        Invoke-CounterMigrationPhase 'prepare-drift'
        $apiProcess = Start-FixtureApi 'drift' 'checksum|migration'
        Invoke-CounterMigrationPhase 'check-drift'
        Invoke-CounterMigrationPhase 'prepare-failure'
        $apiProcess = Start-FixtureApi 'failure' 'outbox_replay_counter|debug_enabled|already exists|protocol_config_protocol_check|live_dead_letters'
        Invoke-CounterMigrationPhase 'check-failure'
        $apiProcess = Start-FixtureApi 'recovered'
        Invoke-CounterMigrationPhase 'upgraded'
        Stop-OwnedProcess $apiProcess (Join-Path $fixture 'iot-engine.exe')
        $apiProcess = Start-FixtureApi 'repeat'
        Invoke-CounterMigrationPhase 'repeated'
    }
} finally {
    Stop-OwnedProcess $apiProcess (Join-Path $fixture 'iot-engine.exe')
    Stop-OwnedProcess $redisProcess $RedisExe
    if (Test-Path -LiteralPath (Join-Path $data 'postmaster.pid')) {
        & (Join-Path $PostgresBin 'pg_ctl.exe') -D $data -m fast -w stop | Out-Null
    }
    $env:Path = $oldPath
    $env:ARCHITECTURE_DATABASE_URL = $oldDatabaseUrl
    $env:TEST_BASE_URL = $oldApiBase
    $env:TEST_EDGE_PLATFORM_ID = $oldEdgePlatformId
    $env:ARCHITECTURE_REDIS_URL = $oldRedisUrl
    $env:ARCHITECTURE_MIGRATION_STATE = $oldMigrationState
    Write-Output "Disposable fixture logs: $fixture"
}

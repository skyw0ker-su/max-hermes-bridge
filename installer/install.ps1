# Установщик «Мост мессенджера MAX для HERMES AGENT»
# Запуск: install.cmd  (или: powershell -ExecutionPolicy Bypass -File install.ps1)
# Токен можно передать параметром -Token или ввести по запросу (ввод скрыт).
param(
    [string]$Token = "",
    [string]$UserId = "",
    [string]$HermesHome = $(if ($env:HERMES_HOME) { $env:HERMES_HOME } else { Join-Path $env:LOCALAPPDATA "hermes" }),
    [switch]$NoRestart
)
$ErrorActionPreference = "Stop"
$ApiBase = "https://platform-api2.max.ru"
$Src = Join-Path (Split-Path -Parent $PSScriptRoot) "max-platform"
$Dst = Join-Path $HermesHome "plugins\max-platform"
$CaDst = Join-Path $HermesHome "max-ca\russian_trusted_ca_bundle.pem"
$EnvFile = Join-Path $HermesHome ".env"

function Say($m) { Write-Host "==> $m" -ForegroundColor Cyan }
function Die($m) { Write-Host "ОШИБКА: $m" -ForegroundColor Red; exit 1 }

if (-not (Test-Path $Src)) { Die "Не найдена папка плагина: $Src" }
if (-not (Test-Path $HermesHome)) { Die "Hermes Agent не найден: $HermesHome (задайте -HermesHome)" }

# --- TLS: цепочка Минцифры отсутствует в хранилище Windows, проверяем по своему корню ---
$caPem = Join-Path $Src "certs\russian_trusted_ca_bundle.pem"
$script:Roots = @()
foreach ($m in [regex]::Matches((Get-Content $caPem -Raw), "-----BEGIN CERTIFICATE-----(.*?)-----END CERTIFICATE-----", "Singleline")) {
    $b = [Convert]::FromBase64String(($m.Groups[1].Value -replace "\s", ""))
    $script:Roots += New-Object Security.Cryptography.X509Certificates.X509Certificate2 (,$b)
}
[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
[Net.ServicePointManager]::ServerCertificateValidationCallback = {
    param($s, $cert, $chain, $errors)
    if ($errors -eq [Net.Security.SslPolicyErrors]::None) { return $true }
    $c = New-Object Security.Cryptography.X509Certificates.X509Chain
    $c.ChainPolicy.RevocationMode = "NoCheck"
    $c.ChainPolicy.VerificationFlags = "AllowUnknownCertificateAuthority"
    foreach ($r in $script:Roots) { [void]$c.ChainPolicy.ExtraStore.Add($r) }
    if (-not $c.Build((New-Object Security.Cryptography.X509Certificates.X509Certificate2 $cert))) { return $false }
    $top = $c.ChainElements[$c.ChainElements.Count - 1].Certificate.Thumbprint
    return [bool]($script:Roots | Where-Object { $_.Thumbprint -eq $top })
}

function MaxGet($path, $tok, $timeout = 30) {
    $r = [Net.HttpWebRequest]::Create("$ApiBase$path")
    $r.Headers.Add("Authorization", $tok)
    $r.Timeout = ($timeout + 15) * 1000
    $r.Proxy = $null
    $resp = $r.GetResponse()
    $txt = (New-Object IO.StreamReader($resp.GetResponseStream(), [Text.Encoding]::UTF8)).ReadToEnd()
    $resp.Close()
    return $txt | ConvertFrom-Json
}

# --- 1. Токен ---
if (-not $Token) {
    $sec = Read-Host "Токен бота MAX (ввод скрыт)" -AsSecureString
    $Token = [Runtime.InteropServices.Marshal]::PtrToStringAuto([Runtime.InteropServices.Marshal]::SecureStringToBSTR($sec))
}
$Token = $Token.Trim()
if (-not $Token) { Die "Токен пустой" }

Say "Проверяю токен..."
try { $me = MaxGet "/me" $Token } catch { Die "Токен не принят или MAX недоступен: $($_.Exception.Message)" }
Write-Host "    Бот: $($me.name) (@$($me.username))" -ForegroundColor Green

# --- 2. Копирование плагина ---
Say "Копирую плагин в $Dst"
New-Item -ItemType Directory -Force (Split-Path $Dst) | Out-Null
if (Test-Path $Dst) { Remove-Item $Dst -Recurse -Force }
Copy-Item $Src $Dst -Recurse
New-Item -ItemType Directory -Force (Split-Path $CaDst) | Out-Null
Copy-Item $caPem $CaDst -Force

# --- 3. Определяем свой user_id (если не задан) ---
function Invoke-Hermes([string[]]$a) {
    $h = Get-Command hermes -CommandType Application,ExternalScript -ErrorAction SilentlyContinue
    if (-not $h) { Write-Host "    'hermes' не найден в PATH: выполните вручную: hermes $($a -join ' ')" -ForegroundColor Yellow; return }
    & $h.Source @a
}
if (-not $UserId) {
    $cur = if (Test-Path $EnvFile) { (Select-String -Path $EnvFile -Pattern "^MAX_ALLOWED_USERS=(.+)$" | Select-Object -First 1) } else { $null }
    if ($cur) { $UserId = $cur.Matches[0].Groups[1].Value.Trim() }
}
if (-not $UserId) {
    Say "Останавливаю шлюз, чтобы поймать ваше сообщение боту"
    Invoke-Hermes @("gateway", "stop")
    Write-Host "    Откройте MAX, найдите бота «$($me.name)» и отправьте ему любое сообщение (жду 90 с)..." -ForegroundColor Yellow
    $deadline = (Get-Date).AddSeconds(90)
    while (-not $UserId -and (Get-Date) -lt $deadline) {
        try {
            $u = MaxGet "/updates?timeout=20&types=message_created,bot_started" $Token 20
            foreach ($up in $u.updates) {
                $id = if ($up.message) { $up.message.sender.user_id } else { $up.user.user_id }
                if ($id) { $UserId = [string]$id; break }
            }
        } catch { Start-Sleep 2 }
    }
    if (-not $UserId) { Write-Host "    Сообщение не получено. Бот будет открыт для всех — задайте MAX_ALLOWED_USERS в .env вручную." -ForegroundColor Yellow }
    else { Write-Host "    Ваш MAX user_id: $UserId" -ForegroundColor Green }
}

# --- 4. .env (без BOM, с резервной копией) ---
Say "Обновляю $EnvFile"
if (Test-Path $EnvFile) { Copy-Item $EnvFile "$EnvFile.bak-max-bridge" -Force }
$lines = if (Test-Path $EnvFile) { [IO.File]::ReadAllLines($EnvFile) } else { @() }
$vals = [ordered]@{ MAX_BOT_TOKEN = $Token; MAX_CA_BUNDLE = $CaDst }
if ($UserId) { $vals["MAX_ALLOWED_USERS"] = $UserId; $vals["MAX_HOME_CHANNEL"] = $UserId }
foreach ($k in $vals.Keys) {
    $lines = @($lines | Where-Object { $_ -notmatch "^$k=" }) + "$k=$($vals[$k])"
}
[IO.File]::WriteAllLines($EnvFile, $lines, (New-Object Text.UTF8Encoding($false)))

# --- 5. Включение плагина и перезапуск ---
Say "Включаю плагин"
Invoke-Hermes @("plugins", "enable", "max-platform")
if (-not $NoRestart) {
    Say "Перезапускаю шлюз"
    Invoke-Hermes @("gateway", "restart")
}
Write-Host "`nГотово. Напишите боту «привет» — ответит Hermes." -ForegroundColor Green

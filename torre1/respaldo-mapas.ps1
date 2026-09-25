# Respaldo del proceso de mapas de VG Agroclima en la Torre 1.
# La vía principal es GitHub Actions (4 veces al día). Este guion corre cada 3 horas por el
# Programador de tareas y solo actúa si el manifiesto publicado tiene más de 14 horas: entonces
# procesa aquí con el mismo procesar.py y publica en Cloudflare Pages. Sin tokens de Claude (R-TAREAS).
# La credencial de Cloudflare se lee de la bóveda en Drive a variables de entorno; nunca se imprime (R19).
param([switch]$Forzar, [int]$MaxPasos = 0, [string]$Rama = "main")
$ErrorActionPreference = "Stop"
$raiz = "C:\vg"; $repo = "$raiz\agroclima-datos"; $salida = "$raiz\sitio-mapas"; $log = "$raiz\respaldo-mapas.log"
function Log($t) { "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') $t" | Out-File -Append -Encoding utf8 $log }

try {
  if (-not $Forzar) {
    $m = Invoke-RestMethod "https://vg-agroclima-datos.pages.dev/manifest.json" -TimeoutSec 60
    $edad = ((Get-Date).ToUniversalTime() - [datetime]::Parse($m.generado).ToUniversalTime()).TotalHours
    if ($edad -lt 14) { Log ("al día: manifiesto de hace {0:N1} h" -f $edad); exit 0 }
    Log ("manifiesto de hace {0:N1} h: GitHub no actualizó, procesa la Torre 1" -f $edad)
  }
  # PowerShell 5.1 convierte en error cualquier línea que un programa escriba por stderr (avisos de npm,
  # de git…): para los programas externos manda su código de salida, no esa salida
  $ErrorActionPreference = "Continue"
  git -C $repo pull -q
  $env:PATH = "$raiz\agroclima-env;$raiz\agroclima-env\Library\bin;" + $env:PATH
  $env:PYTHONIOENCODING = "utf-8"
  if (Test-Path $salida) { Remove-Item -Recurse -Force $salida }
  $args2 = @("$repo\procesar.py", "--salida", $salida); if ($MaxPasos -gt 0) { $args2 += @("--max-pasos", "$MaxPasos") }
  & "$raiz\agroclima-env\python.exe" @args2 2>&1 | ForEach-Object { Log "  $_" }
  if ($LASTEXITCODE -ne 0) { throw "procesar.py salió con $LASTEXITCODE" }
  Copy-Item -Recurse -Force "$repo\estatico\*" $salida

  # la letra de Google Drive cambia (G:, M:, E:…): se busca la bóveda en todas
  $boveda = Get-PSDrive -PSProvider FileSystem | ForEach-Object { "$($_.Root)Mi unidad\PROYECTOS PERSONALES\VG CREDENCIALES\cloudflare\cloudflare-tokens.txt" } | Where-Object { Test-Path $_ } | Select-Object -First 1
  if ($boveda) {
    foreach ($l in Get-Content $boveda) {
      if ($l -match '^\s*API_TOKEN\s*=\s*"?([^"\s]+)') { $env:CLOUDFLARE_API_TOKEN = $Matches[1] }
      if ($l -match '^\s*ACCOUNT_ID\s*=\s*"?([^"\s]+)') { $env:CLOUDFLARE_ACCOUNT_ID = $Matches[1] }
    }
  } elseif (Test-Path "$raiz\cloudflare.dpapi") {
    # Drive no siempre está montado en la Torre 1: copia cifrada con DPAPI de la máquina (solo se descifra
    # aquí), registrada en INDICE-TOKENS.md de la bóveda. Misma credencial: huella sha256 ea21571ac4.
    Add-Type -AssemblyName System.Security
    $d = [Text.Encoding]::UTF8.GetString([Security.Cryptography.ProtectedData]::Unprotect([IO.File]::ReadAllBytes("$raiz\cloudflare.dpapi"), $null, "LocalMachine")).Split("`n")
    $env:CLOUDFLARE_API_TOKEN = $d[0]; $env:CLOUDFLARE_ACCOUNT_ID = $d[1]
  } else { throw "sin credencial de Cloudflare: ni bóveda en Drive ni copia cifrada" }
  Set-Location $raiz  # la tarea arranca en system32, donde wrangler no puede escribir
  $env:WRANGLER_SEND_METRICS = "false"
  & "D:\Program Files\nodejs\npx.cmd" --yes wrangler@3 pages deploy $salida --project-name=vg-agroclima-datos --branch=$Rama --commit-dirty=true 2>&1 | Select-Object -Last 2 | ForEach-Object { Log "  $_" }
  if ($LASTEXITCODE -ne 0) { throw "wrangler salió con $LASTEXITCODE" }
  Log "publicado desde la Torre 1 (rama $Rama)"
} catch {
  Log "ERROR: $($_.Exception.Message)"
  exit 1
} finally {
  Remove-Item Env:CLOUDFLARE_API_TOKEN -ErrorAction SilentlyContinue
}

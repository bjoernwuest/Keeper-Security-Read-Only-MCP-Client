import asyncio
import concurrent.futures
import json
import os
import re
import subprocess
import threading
import time
from contextlib import AsyncExitStack, asynccontextmanager

from fastmcp import FastMCP
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Mount, Route

# Config aus Environment Variables
raw_folders = os.getenv("ALLOWED_FOLDERS", "")
ALLOWED_FOLDERS = [f.strip() for f in raw_folders.split(",") if f.strip()]

try:
  KEEPALIVE_HOURS = float(os.getenv("KEEPALIVE_HOURS", "4"))
except ValueError:
  KEEPALIVE_HOURS = 4.0

KEEPALIVE_INTERVAL_SECONDS = int(KEEPALIVE_HOURS * 3600)
HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", "8000"))

# Cache-Konfiguration (ausschließlich In-Memory):
# - CACHE_REFRESH_SECONDS: Intervall für vollständigen Cache-Refresh (Default: 3600 = 1h).
#   Fallback: CACHE_REFRESH_HOURS (Default: 1.0), falls SECONDS nicht gesetzt.
# - KEEPER_TIMEOUT_SECONDS: max. Wartezeit auf Keeper pro Anfrage (Default: 5s).
#   Antwortet Keeper langsamer, wird aus dem Cache geantwortet.
_raw_refresh_seconds = os.getenv("CACHE_REFRESH_SECONDS", "").strip()
_raw_refresh_hours = os.getenv("CACHE_REFRESH_HOURS", "").strip()
try:
  if _raw_refresh_seconds:
    CACHE_REFRESH_SECONDS = float(_raw_refresh_seconds)
  elif _raw_refresh_hours:
    CACHE_REFRESH_SECONDS = float(_raw_refresh_hours) * 3600
  else:
    CACHE_REFRESH_SECONDS = 3600.0
except ValueError:
  CACHE_REFRESH_SECONDS = 3600.0
if CACHE_REFRESH_SECONDS <= 0:
  CACHE_REFRESH_SECONDS = 3600.0

try:
  KEEPER_TIMEOUT_SECONDS = float(os.getenv("KEEPER_TIMEOUT_SECONDS", "5"))
except ValueError:
  KEEPER_TIMEOUT_SECONDS = 5.0
if KEEPER_TIMEOUT_SECONDS <= 0:
  KEEPER_TIMEOUT_SECONDS = 5.0

# In-Memory Cache: key -> {"value": ..., "ts": float}
# Keys: "list:<folder>", "list:__all__", "search:<folder>:<query>",
#       "search:__all__:<query>", "get:<uid>", "totp:<uid>", "allowed:<folder>",
#       "allowed:__all__"
_CACHE: dict = {}
_CACHE_LOCK = threading.Lock()
_CACHE_LAST_REFRESH = {"started_ts": None, "finished_ts": None, "ok": False, "error": None}
_KEEPER_EXECUTOR = concurrent.futures.ThreadPoolExecutor(
    max_workers=20, thread_name_prefix="keeper"
)


def _cache_get(key: str):
  """Liefert den gecachten Wert oder None (Thread-safe, In-Memory)."""
  with _CACHE_LOCK:
    entry = _CACHE.get(key)
    return entry["value"] if entry is not None else None


def _cache_get_age(key: str) -> float | None:
  """Alter des Cache-Eintrags in Sekunden oder None."""
  with _CACHE_LOCK:
    entry = _CACHE.get(key)
    if entry is None:
      return None
    return time.time() - entry["ts"]


def _cache_set(key: str, value) -> None:
  """Aktualisiert den Cache-Eintrag (Thread-safe, In-Memory)."""
  with _CACHE_LOCK:
    _CACHE[key] = {"value": value, "ts": time.time()}


def _cache_stats() -> dict:
  with _CACHE_LOCK:
    return {
        "entries": len(_CACHE),
        "keys": sorted(_CACHE.keys())[:100],
        "last_refresh": dict(_CACHE_LAST_REFRESH),
    }


def _call_with_cache(key: str, fetch_fn, timeout: float | None = None):
  """Ruft fetch_fn() auf, aktualisiert bei Erfolg den Cache.

  - Antwortet Keeper innerhalb von `timeout` (Default KEEPER_TIMEOUT_SECONDS),
    wird die Keeper-Antwort weitergeleitet und der Cache aktualisiert.
  - Dauert Keeper länger als `timeout` (oder wirft einen Fehler) und es gibt
    einen Cache-Eintrag, wird aus dem Cache geantwortet.
  - Gibt es keinen Cache-Eintrag, wird weiter auf Keeper gewartet (bis zu
    60s zusätzlich), damit die Anfrage trotzdem beantwortet werden kann.

  Returns:
      (result, source): source in {"keeper", "cache-timeout",
        "cache-error", "keeper-slow"}.
  """
  limit = timeout if timeout and timeout > 0 else KEEPER_TIMEOUT_SECONDS
  fut = _KEEPER_EXECUTOR.submit(fetch_fn)
  start = time.time()
  try:
    result = fut.result(timeout=limit)
    _cache_set(key, result)
    return result, "keeper"
  except concurrent.futures.TimeoutError:
    elapsed = time.time() - start
    cached = _cache_get(key)
    if cached is not None:
      age = _cache_get_age(key)
      print(
          f"[CACHE FALLBACK] Keeper-Timeout nach {elapsed:.1f}s (Limit {limit}s)"
          f" für key='{key}' (Cache-Alter: {age:.0f}s) -> antworte aus Cache."
      )
      return cached, "cache-timeout"
    print(
        f"[CACHE MISS] Keeper-Timeout nach {elapsed:.1f}s für key='{key}',"
        " kein Cache vorhanden -> warte weiter auf Keeper..."
    )
    try:
      result = fut.result(timeout=60)
      _cache_set(key, result)
      return result, "keeper-slow"
    except Exception as e:
      raise RuntimeError(
          f"Keeper antwortet nicht (Timeout {limit}s) und kein Cache für"
          f" '{key}' vorhanden: {e}"
      )
  except Exception as e:
    cached = _cache_get(key)
    if cached is not None:
      print(
          f"[CACHE FALLBACK] Keeper-Fehler für key='{key}': {e}"
          " -> antworte aus Cache."
      )
      return cached, "cache-error"
    raise

# Re-Login bei abgelaufener Keeper-Session:
# - Username kommt aus der Env-Variable KEEPER_USERNAME
#   (Fallback: config.json, z.B. /root/.keeper/config.json).
# - Passwort kommt aus der Env-Variable KEEPER_PASSWORD.
KEEPER_CONFIG_CANDIDATES = [
    os.getenv("KEEPER_CONFIG_FILE", ""),
    "/root/.keeper/config.json",
    os.path.join(os.path.expanduser("~"), ".keeper", "config.json"),
    "/app/keeper_data/config.json",
    "keeper_data/config.json",
]

SESSION_EXPIRED_MARKERS = [
    "session expired",
    "session has expired",
    "session needs to be refreshed",
    "not logged in",
    "not authenticated",
    "no active session",
    "login required",
    "login expired",
    "please login",
    "please log in",
    "please logout and login",
    "logout and login again",
    "reauthenticat",
    "invalid session",
    "session token",
    "authentication expired",
    "logged out",
]

# Keeper Record-UID: 22 Zeichen (URL-safe Base64). Titel werden bewusst
# nicht akzeptiert, da Keeper einen Datensatz per Titel nicht zuverlässig liefert.
RECORD_UID_RE = re.compile(r"^[A-Za-z0-9_-]{22}$")


def validate_record_uid(value: str) -> str:
  """Validiert, dass eine echte record_uid übergeben wurde (kein Titel).

  Raises:
      ValueError: wenn kein gültiges UID-Format vorliegt.
  """
  uid = (value or "").strip()
  if not RECORD_UID_RE.match(uid):
    raise ValueError(
        f"Ungültige record_uid '{value}': es muss die record_uid übergeben"
        " werden (22 Zeichen, z.B. aus 'list_secrets' oder 'search_secrets')."
        " Titel werden nicht akzeptiert."
    )
  return uid


def _normalize_vault_entry(entry: dict) -> dict:
  """Mappt einen Keeper-JSON-Eintrag auf ein stabiles Schema mit record_uid."""
  uid = entry.get("uid", "")
  return {
      "record_uid": uid,
      "title": entry.get("name", ""),
      "type": entry.get("type", ""),
      "details": entry.get("details", ""),
      "source": entry.get("source", ""),
  }


def _parse_vault_json(raw: str) -> list[dict]:
  """Parst `keeper ... --format=json` (Liste oder Dict mit records)."""
  text = (raw or "").strip()
  if not text.startswith("[") and not text.startswith("{"):
    return []
  try:
    data = json.loads(text)
  except Exception:
    return []
  if isinstance(data, list):
    return [_normalize_vault_entry(e) for e in data if isinstance(e, dict)]
  if isinstance(data, dict):
    for key in ("records", "results", "data"):
      if isinstance(data.get(key), list):
        return [
            _normalize_vault_entry(e)
            for e in data[key]
            if isinstance(e, dict)
        ]
  return []


def _to_json(payload: dict) -> str:
  return json.dumps(payload, ensure_ascii=False, indent=2)

_login_lock = threading.Lock()


def _redacted_args(args: list[str]) -> str:
  """Baut einen log-sicheren Befehlsstring (Passwort nach -p wird maskiert)."""
  redacted = []
  mask_next = False
  for a in args:
    if mask_next:
      redacted.append("***")
      mask_next = False
      continue
    redacted.append(a)
    if a in ("-p", "--password", "-kp", "--pass"):
      mask_next = True
  return " ".join(redacted)


def is_session_expired(text: str) -> bool:
  """Erkennt anhand der CLI-Ausgabe, ob die Keeper-Session abgelaufen ist."""
  lowered = (text or "").lower()
  return any(marker in lowered for marker in SESSION_EXPIRED_MARKERS)


def find_keeper_config_file() -> str | None:
  """Sucht die Keeper config.json (enthält u.a. den Benutzernamen)."""
  live_env = os.getenv("KEEPER_CONFIG_FILE", "").strip()
  candidates = ([live_env] if live_env else []) + [
      c for c in KEEPER_CONFIG_CANDIDATES if c
  ]
  # Immer auch Standardpfade prüfen (falls Env erst später gesetzt wurde).
  for extra in (
      "/root/.keeper/config.json",
      os.path.join(os.path.expanduser("~"), ".keeper", "config.json"),
  ):
    if extra not in candidates:
      candidates.append(extra)
  for candidate in candidates:
    if candidate and os.path.isfile(candidate):
      return candidate
  return None


def get_keeper_username() -> str | None:
  """Liest den Keeper-Benutzernamen aus der Env-Variable KEEPER_USERNAME."""
  # Primärquelle: Env-Variable. config.json nur noch als Fallback.
  env_user = os.getenv("KEEPER_USERNAME", "").strip()
  if env_user:
    return env_user
  config_path = find_keeper_config_file()
  if not config_path:
    return None
  try:
    with open(config_path, "r", encoding="utf-8") as f:
      data = json.load(f)
    user = str(data.get("user", "")).strip()
    if user:
      print(f"[LOGIN] KEEPER_USERNAME nicht gesetzt, nutze '{user}' aus {config_path}.")
      return user
  except Exception as e:
    print(f"[LOGIN WARNUNG] Konnte Username aus {config_path} nicht lesen: {e}")
  return None


def do_keeper_login() -> bool:
  """Führt eine Neuanmeldung durch (Username aus KEEPER_USERNAME, Passwort aus Env).

  Returns:
      True bei Erfolg, sonst False.
  """
  username = get_keeper_username()
  password = os.getenv("KEEPER_PASSWORD", "")
  if not username:
    print("[LOGIN FEHLGESCHLAGEN] Kein Username gefunden (config.json leer/fehlt).")
    return False
  if not password:
    print(
        "[LOGIN FEHLGESCHLAGEN] KEEPER_PASSWORD ist nicht gesetzt;"
        " Neuanmeldung ohne Passwort nicht möglich."
    )
    return False

  with _login_lock:
    # Doppel-Check: ggf. hat ein paralleler Thread bereits neu angemeldet.
    try:
      check = subprocess.run(
          ["keeper", "whoami"], capture_output=True, text=True, timeout=60
      )
      if check.returncode == 0 and check.stdout.strip():
        print("[LOGIN] Session ist bereits wieder gültig, kein Re-Login nötig.")
        return True
    except Exception:
      pass

    login_variants = [
        ["login", username, "-p", password],
        ["login", "--config-file", username, "-p", password],
    ]
    for variant in login_variants:
      print(f"[LOGIN] Versuche Neuanmeldung als '{username}' (keeper {_redacted_args(variant)})")
      try:
        res = subprocess.run(
            ["keeper"] + variant, capture_output=True, text=True, timeout=180
        )
      except Exception as e:
        print(f"[LOGIN FEHLGESCHLAGEN] keeper {' '.join(variant[:2])}: {e}")
        continue
      combined = f"{res.stdout}\n{res.stderr}"
      # Passwort niemals loggen.
      safe_combined = combined.replace(password, "***") if password else combined
      if res.returncode == 0:
        print(f"[LOGIN] Neuanmeldung erfolgreich: {safe_combined[:200]}...")
        return True
      print(f"[LOGIN FEHLGESCHLAGEN] {safe_combined[:500]}...")
      # Bei falschem Passwort / 2FA-Pflicht bringt die zweite Variante nichts.
      lowered = combined.lower()
      if "invalid" in lowered and "password" in lowered:
        break
    return False


def _run_keeper_once(args: list[str]) -> subprocess.CompletedProcess:
  return subprocess.run(["keeper"] + args, capture_output=True, text=True)


def run_keeper(args: list[str], _retry: bool = True) -> str:
  """Führt einen Keeper-Befehl aus und loggt die Ein- und Ausgaben.

  Erkennt eine abgelaufene Session und versucht einmalig eine Neuanmeldung
  (Username aus KEEPER_USERNAME, Passwort aus KEEPER_PASSWORD).
  """
  print(f"[EXECUTING] keeper {_redacted_args(args)}")
  res = _run_keeper_once(args)

  if res.returncode != 0:
    combined = f"{res.stdout}\n{res.stderr}"
    print(f"[KEEPER ERROR] {res.stderr}")
    if _retry and is_session_expired(combined):
      print("[SESSION] Abgelaufener Keeper-Login erkannt, versuche Neuanmeldung...")
      if do_keeper_login():
        print(f"[RETRY] keeper {_redacted_args(args)}")
        res_retry = _run_keeper_once(args)
        if res_retry.returncode == 0:
          output = res_retry.stdout.strip()
          print(f"[KEEPER RESULT ({len(output)} chars)] {output[:200]}...")
          return output
        print(f"[KEEPER ERROR nach Re-Login] {res_retry.stderr}")
        raise RuntimeError(
            f"Keeper CLI Fehler (auch nach Neuanmeldung):"
            f" {res_retry.stderr or res_retry.stdout}"
        )
      raise RuntimeError(
          f"Keeper-Session abgelaufen und Neuanmeldung fehlgeschlagen:"
          f" {res.stderr or res.stdout}"
      )
    raise RuntimeError(f"Keeper CLI Fehler: {res.stderr or res.stdout}")

  output = res.stdout.strip()
  print(f"[KEEPER RESULT ({len(output)} chars)] {output[:200]}...")
  return output


def run_keeper_totp(record_key: str) -> str:
  """Führt 'keeper totp <record>' mit Re-Login bei abgelaufener Session aus."""
  print(f"[EXECUTING] keeper totp {record_key}")
  res = _run_keeper_once(["totp", record_key])
  full_output = f"{res.stdout}\n{res.stderr}".strip()
  print(f"[KEEPER TOTP OUTPUT] {full_output}")

  if res.returncode != 0 and is_session_expired(full_output):
    print("[SESSION] Abgelaufener Keeper-Login erkannt (totp), versuche Neuanmeldung...")
    if do_keeper_login():
      res = _run_keeper_once(["totp", record_key])
      full_output = f"{res.stdout}\n{res.stderr}".strip()
      print(f"[KEEPER TOTP OUTPUT nach Re-Login] {full_output}")
    else:
      raise RuntimeError(
          "Keeper-Session abgelaufen und Neuanmeldung fehlgeschlagen"
          " (KEEPER_PASSWORD gesetzt?)."
      )
  return full_output


def get_record_uids_for_folder(folder: str) -> set[str]:
  """Ermittelt alle Record-UIDs eines einzelnen Keeper-Ordners."""
  try:
    ls_out = run_keeper(["ls", "--list", folder])
  except Exception as e:
    print(f"[FETCH UIDS FAILED] Ordner {folder}: {e}")
    return set()
  return set(re.findall(r"\b[A-Za-z0-9_-]{22}\b", ls_out))


def get_allowed_record_uids() -> set[str]:
  """Ermittelt alle gültigen Record-UIDs aus den freigegebenen Ordnern."""
  if not ALLOWED_FOLDERS:
    return set()

  allowed_uids = set()
  for folder in ALLOWED_FOLDERS:
    allowed_uids.update(get_record_uids_for_folder(folder))

  return allowed_uids


def get_allowed_uids_cached(folder: str) -> set[str]:
  """Allowed-UIDs mit Cache-Fallback (max. KEEPER_TIMEOUT_SECONDS auf Keeper warten)."""
  key = f"allowed:{folder}" if folder else "allowed:__all__"

  def _fetch() -> set[str]:
    if folder:
      return get_record_uids_for_folder(folder)
    try:
      ls_out = run_keeper(["ls", "--list"])
    except Exception as e:
      print(f"[FETCH UIDS FAILED] alle Ordner: {e}")
      return set()
    return set(re.findall(r"\b[A-Za-z0-9_-]{22}\b", ls_out))

  try:
    result, _source = _call_with_cache(key, _fetch)
    return set(result) if result else set()
  except Exception as e:
    print(f"[ALLOWED UIDS FAILED] Ordner '{folder}': {e}")
    return set()


def get_allowed_uids_fast(folder: str) -> set[str]:
  """Allowed-UIDs für den Request-Pfad: Cache zuerst (stale-while-revalidate).

  Damit eine Tool-Anfrage insgesamt spätestens nach KEEPER_TIMEOUT_SECONDS
  beantwortet werden kann (nur noch EIN Keeper-Call mit Timeout im
  kritischen Pfad), wird ein vorhandener Cache-Eintrag sofort zurückgegeben
  und im Hintergrund aktualisiert. Nur bei Cache-Miss wird auf Keeper
  gewartet (mit Timeout + Cache-Fallback via get_allowed_uids_cached).
  """
  key = f"allowed:{folder}" if folder else "allowed:__all__"
  cached = _cache_get(key)
  if cached is not None:
    def _bg_refresh() -> None:
      try:
        if folder:
          fresh = get_record_uids_for_folder(folder)
        else:
          try:
            ls_out = run_keeper(["ls", "--list"])
          except Exception:
            return
          fresh = set(re.findall(r"\b[A-Za-z0-9_-]{22}\b", ls_out))
        if fresh:
          _cache_set(key, set(fresh))
      except Exception:
        pass

    try:
      _KEEPER_EXECUTOR.submit(_bg_refresh)
    except Exception:
      pass
    return set(cached)
  return get_allowed_uids_cached(folder)


def _refresh_folder_cache(folder: str) -> int:
  """Listet einen Ordner und lädt alle Secret-Details in den Cache. Returns: Anzahl Secrets."""
  count = 0
  try:
    out = run_keeper(["ls", "--list", folder, "--format=json"])
  except Exception as e:
    print(f"[CACHE REFRESH FAILED] list für Ordner '{folder}': {e}")
    return 0
  records = _parse_vault_json(out)
  if records:
    _cache_set(f"list:{folder}", _to_json({"folder": folder, "records": records}))
  else:
    if out.strip():
      _cache_set(
          f"list:{folder}",
          _to_json({"folder": folder, "records": [], "raw": out.strip()}),
      )
    else:
      _cache_set(f"list:{folder}", _to_json({"folder": folder, "records": []}))
  allowed = get_record_uids_for_folder(folder)
  if allowed:
    _cache_set(f"allowed:{folder}", set(allowed))
  # Falls JSON-Parsing keine UIDs lieferte, nutze allowed-Set als Quelle.
  uids = {r.get("record_uid") for r in records if r.get("record_uid")}
  uids.update(allowed)
  uids = {u for u in uids if u and RECORD_UID_RE.match(u)}
  for uid in sorted(uids):
    try:
      raw_json = run_keeper(["get", uid, "--format=json"])
      _cache_set(f"get:{uid}", raw_json)
      count += 1
    except Exception as e:
      print(f"[CACHE REFRESH] get für '{uid}' fehlgeschlagen: {e}")
  print(f"[CACHE REFRESH] Ordner '{folder}': {count} Secrets gecacht.")
  return count


def _refresh_unrestricted_cache() -> int:
  """Listet den gesamten Vault und lädt alle Secret-Details in den Cache."""
  count = 0
  try:
    out = run_keeper(["ls", "--list", "--format=json"])
  except Exception as e:
    print(f"[CACHE REFRESH FAILED] list (unrestricted): {e}")
    return 0
  records = _parse_vault_json(out)
  _cache_set("list:__all__", _to_json({"records": records}))
  try:
    ls_out = run_keeper(["ls", "--list"])
    allowed = set(re.findall(r"\b[A-Za-z0-9_-]{22}\b", ls_out))
  except Exception as e:
    print(f"[CACHE REFRESH FAILED] allowed (unrestricted): {e}")
    allowed = set()
  if allowed:
    _cache_set("allowed:__all__", set(allowed))
  uids = {r.get("record_uid") for r in records if r.get("record_uid")}
  uids.update(allowed)
  uids = {u for u in uids if u and RECORD_UID_RE.match(u)}
  for uid in sorted(uids):
    try:
      raw_json = run_keeper(["get", uid, "--format=json"])
      _cache_set(f"get:{uid}", raw_json)
      count += 1
    except Exception as e:
      print(f"[CACHE REFRESH] get für '{uid}' fehlgeschlagen: {e}")
  print(f"[CACHE REFRESH] unrestricted: {count} Secrets gecacht.")
  return count


def refresh_cache_blocking() -> None:
  """Vollständiger Cache-Refresh (blockierend, für Background-Task)."""
  _CACHE_LAST_REFRESH["started_ts"] = time.time()
  print(
      f"[CACHE] Starte vollständigen Refresh"
      f" (Ordner: {ALLOWED_FOLDERS if ALLOWED_FOLDERS else 'alle'})..."
  )
  try:
    total = 0
    if ALLOWED_FOLDERS:
      for folder in ALLOWED_FOLDERS:
        total += _refresh_folder_cache(folder)
    else:
      total = _refresh_unrestricted_cache()
    _CACHE_LAST_REFRESH["finished_ts"] = time.time()
    _CACHE_LAST_REFRESH["ok"] = True
    _CACHE_LAST_REFRESH["error"] = None
    print(f"[CACHE] Refresh abgeschlossen: {total} Secrets, {_cache_stats()['entries']} Einträge.")
  except Exception as e:
    _CACHE_LAST_REFRESH["finished_ts"] = time.time()
    _CACHE_LAST_REFRESH["ok"] = False
    _CACHE_LAST_REFRESH["error"] = str(e)
    print(f"[CACHE REFRESH FEHLER] {e}")


async def cache_refresh_loop():
  """Hintergrund-Task: initialer Load nach Start + periodischer Refresh."""
  print(
      f"[CACHE] Initialer Load nach Start (Refresh-Intervall:"
      f" {CACHE_REFRESH_SECONDS:.0f}s)..."
  )
  try:
    await asyncio.to_thread(refresh_cache_blocking)
  except Exception as err:
    print(f"[CACHE WARNUNG] Initialer Load fehlgeschlagen: {err}")
  while True:
    await asyncio.sleep(CACHE_REFRESH_SECONDS)
    try:
      await asyncio.to_thread(refresh_cache_blocking)
    except Exception as err:
      print(f"[CACHE WARNUNG] Periodischer Refresh fehlgeschlagen: {err}")


async def keeper_keepalive_loop():
  """Hintergrund-Task für den Session-Keepalive."""
  while True:
    await asyncio.sleep(KEEPALIVE_INTERVAL_SECONDS)
    try:
      output = await asyncio.to_thread(run_keeper, ["sync-down"])
      print(f"[Keepalive] Session erfolgreich synchronisiert.")
    except Exception as err:
      print(f"[Keepalive WARNUNG] Fehler bei Synchronisation: {err}")


def url_prefix_for_folder(folder: str) -> str:
  """Mappt einen Keeper-Ordner auf ein URL-Präfix: '<adresse>/<folder>'.

  Führende/trailing Slashes werden normalisiert, innere Struktur bleibt
  erhalten (z.B. 'A/B' -> '/A/B').
  """
  return "/" + folder.strip().strip("/")


def create_mcp_for_folder(folder: str) -> FastMCP:
  """Erzeugt eine FastMCP-Instanz, die strikt auf EINEN Ordner beschränkt ist."""
  mcp = FastMCP(
      f"Keeper-Vault-{folder}",
      instructions=(
          "### Was ist dieser MCP-Server?\n"
          "Ein sicherer Bridge-Server zum Abrufen von Passwörtern, API-Keys,"
          " 2FA/TOTP-Codes und Secrets aus deinem Keeper Vault.\n\n"
          "### Ordner-Filterung\n"
          f"Der Zugriff ist auf den Keeper-Ordner beschränkt: {folder}.\n\n"
          "### Funktionsweise & Nutzung (Antworten in JSON)\n"
          "1. Nutze 'list_secrets()', um alle verfügbaren Records in"
          f" '{folder}' aufzulisten (JSON mit record_uid/title/details).\n"
          "2. Nutze 'search_secrets(query)', um gezielt nach Begriffen zu"
          f" suchen (JSON mit record_uid/title/details)."
          f" Es werden nur Treffer aus '{folder}' zurückgegeben.\n"
          "3. Nutze 'get_secret(record_uid)', um die Details"
          f" (Benutzername, Passwort, API-Keys) eines Records aus '{folder}'"
          " abzurufen. Es muss die record_uid übergeben werden (kein Titel).\n"
          "4. Nutze 'get_totp(record_uid)', um den aktuellen 2FA /"
          " TOTP-Code für einen Record abzurufen. Es muss die record_uid"
          " übergeben werden (kein Titel)."
      ),
  )

  @mcp.tool()
  def list_secrets() -> str:
    """Listet alle Records im freigegebenen Keeper-Ordner als JSON auf.

    Antwortet mit JSON: {"folder": ..., "records": [{"record_uid": ...,
    "title": ..., "type": ..., "details": ..., "source": ...}]}.
    Die record_uid aus jedem Eintrag für 'get_secret'/'get_totp' verwenden.
    """
    print(f"[TOOL CALL] list_secrets aufgerufen (Ordner: {folder})")

    def _fetch() -> str:
      out = run_keeper(["ls", "--list", folder, "--format=json"])
      records = _parse_vault_json(out)
      if records:
        return _to_json({"folder": folder, "records": records})
      if out.strip():
        # Unerwartetes Format: Rohtext in JSON einbetten statt Schema zu brechen.
        return _to_json({"folder": folder, "records": [], "raw": out.strip()})
      return _to_json({"folder": folder, "records": []})

    try:
      result, source = _call_with_cache(f"list:{folder}", _fetch)
      print(f"[TOOL RESULT] list_secrets Quelle: {source} (Ordner: {folder})")
      return result
    except Exception as e:
      print(f"[LIST FAILED FOR FOLDER {folder}]: {e}")
      return _to_json({"folder": folder, "records": [], "error": str(e)})

  @mcp.tool()
  def search_secrets(query: str) -> str:
    """Durchsucht den freigegebenen Keeper-Ordner und antwortet mit JSON.

    Antwortet mit JSON: {"folder": ..., "query": ...,
    "records": [{"record_uid": ..., "title": ..., "type": ...,
    "details": ..., "source": ...}]}.
    Die record_uid aus jedem Eintrag für 'get_secret'/'get_totp' verwenden.

    Args:
        query: Der Suchbegriff (z.B. 'OpenAI', 'Database', 'GitHub').
    """
    print(f"[TOOL CALL] search_secrets mit query='{query}' (Ordner: {folder})")
    cache_key = f"search:{folder}:{(query or '').strip().lower()}"

    def _fetch() -> str:
      raw_search = run_keeper(["search", query, "--format=json"])
      if not raw_search or "No records found" in raw_search:
        return _to_json(
            {"folder": folder, "query": query, "records": [], "message": (
                f"Keine Treffer im Vault für '{query}' gefunden."
            )}
        )

      allowed_uids = get_allowed_uids_fast(folder)
      records = [
          r for r in _parse_vault_json(raw_search) if r.get("record_uid") in allowed_uids
      ]

      if records:
        return _to_json({"folder": folder, "query": query, "records": records})

      # Falls Keeper kein JSON lieferte (z.B. nur Text), Fallback auf Zeilenfilter.
      if not raw_search.strip().startswith(("[", "{")):
        filtered_lines = []
        for line in raw_search.splitlines():
          line_uids = re.findall(r"\b[A-Za-z0-9_-]{22}\b", line)
          if any(uid in allowed_uids for uid in line_uids):
            filtered_lines.append(line)
        return _to_json(
            {
                "folder": folder,
                "query": query,
                "records": [],
                "message": (
                    f"Keine Treffer für '{query}' im Ordner '{folder}' gefunden."
                    if not filtered_lines
                    else f"Treffer (unstrukturiert): {'; '.join(filtered_lines)}"
                ),
            }
        )

      return _to_json(
          {
              "folder": folder,
              "query": query,
              "records": [],
              "message": f"Keine Treffer für '{query}' im Ordner '{folder}' gefunden.",
          }
      )

    try:
      result, source = _call_with_cache(cache_key, _fetch)
      print(f"[TOOL RESULT] search_secrets Quelle: {source} (Ordner: {folder})")
      return result
    except Exception as e:
      print(f"[SEARCH FAILED FOR FOLDER {folder}]: {e}")
      cached = _cache_get(cache_key)
      if cached is not None:
        return cached
      return _to_json({"folder": folder, "query": query, "records": [], "error": str(e)})

  @mcp.tool()
  def get_secret(record_uid: str) -> str:
    """Holt die vollständigen Details/Passwörter eines Secrets im JSON-Format.

    Es muss die record_uid übergeben werden (kein Titel).

    Prüft vorab, ob der Record im freigegebenen Ordner liegt.

    Args:
        record_uid: Record-UID (22 Zeichen) aus 'list_secrets' oder
          'search_secrets'.
    """
    print(f"[TOOL CALL] get_secret für '{record_uid}' (Ordner: {folder})")

    target_uid = validate_record_uid(record_uid)
    allowed_uids = get_allowed_uids_fast(folder)

    if target_uid in allowed_uids:
      print(
          f"[ACCESS GRANTED] Record-UID '{target_uid}' befindet sich in Ordner"
          f" '{folder}'."
      )

      def _fetch() -> str:
        return run_keeper(["get", target_uid, "--format=json"])

      try:
        result, source = _call_with_cache(f"get:{target_uid}", _fetch)
        print(f"[TOOL RESULT] get_secret Quelle: {source} für '{target_uid}'")
        return result
      except Exception as e:
        print(f"[GET FAILED] '{target_uid}': {e}")
        raise RuntimeError(f"Keeper-Fehler für '{target_uid}': {e}")

    print(
        f"[ACCESS DENIED] Record-UID '{target_uid}' liegt"
        f" nicht in Ordner '{folder}'!"
    )
    raise PermissionError(
        f"Zugriff verweigert: Der Record '{target_uid}' befindet sich"
        f" nicht im freigegebenen Ordner '{folder}'."
    )

  @mcp.tool()
  def get_totp(record_uid: str) -> str:
    """Generiert den aktuellen 2FA / TOTP-Einmalcode und antwortet mit JSON.

    Antwortet mit JSON: {"record_uid": ..., "totp": ...}.

    Es muss die record_uid übergeben werden (kein Titel).

    Prüft vorab, ob der Record im freigegebenen Ordner liegt.

    BEACHTE: der TOTP könnte schon abgelaufen sein. In dem Fall probiere bis zu 3x:
             - TOTP abfragen
             - TOTP nutzen
             - Ergebnis prüfen

    Args:
        record_uid: Record-UID (22 Zeichen) aus 'list_secrets' oder
          'search_secrets'.
    """
    print(f"[TOOL CALL] get_totp für '{record_uid}' (Ordner: {folder})")

    # 1. Validierung + Rechteprüfung (kein Titel-Lookup mehr)
    target_uid = validate_record_uid(record_uid)
    allowed_uids = get_allowed_uids_fast(folder)
    if target_uid not in allowed_uids:
      print(
          f"[ACCESS DENIED TOTP] Record-UID '{target_uid}' liegt nicht in"
          f" Ordner '{folder}'!"
      )
      raise PermissionError(
          f"Zugriff verweigert: Der Record '{target_uid}' befindet"
          f" sich nicht im freigegebenen Ordner '{folder}'."
      )

    # 2. Ausführen von 'keeper totp <UID>' mit Cache-Fallback (max. 5s auf Keeper warten)
    def _fetch_totp() -> str:
      full_output = run_keeper_totp(target_uid)
      # 3. Suche nach einem 6- bis 8-stelligen Zahlen-Code in der Ausgabe
      codes = re.findall(r"\b\d{6,8}\b", full_output)
      if codes:
        return _to_json({"record_uid": target_uid, "totp": codes[0]})
      if full_output:
        return _to_json({"record_uid": target_uid, "totp": None, "raw": full_output})
      raise RuntimeError(
          f"Für den Record '{target_uid}' konnte kein TOTP-Code generiert"
          " werden. Ist dort ein 2FA-Schlüssel hinterlegt?"
      )

    result, source = _call_with_cache(f"totp:{target_uid}", _fetch_totp)
    print(f"[TOOL RESULT] get_totp Quelle: {source} für '{target_uid}'")
    return result

  return mcp


def create_unrestricted_mcp() -> FastMCP:
  """Fallback, wenn ALLOWED_FOLDERS leer ist: Zugriff auf alle Ordner."""
  mcp = FastMCP(
      "Keeper-Vault-Secrets-Manager",
      instructions=(
          "### Was ist dieser MCP-Server?\n"
          "Ein sicherer Bridge-Server zum Abrufen von Passwörtern, API-Keys,"
          " 2FA/TOTP-Codes und Secrets aus deinem Keeper Vault.\n\n"
          "### Ordner-Filterung\n"
          "Der Zugriff ist auf folgende Keeper-Ordner beschränkt: Alle Ordner.\n\n"
          "### Funktionsweise & Nutzung (Antworten in JSON)\n"
          "1. Nutze 'list_secrets()', um alle verfügbaren Records aufzulisten"
          " (JSON mit record_uid/title/details).\n"
          "2. Nutze 'search_secrets(query)', um gezielt nach Begriffen zu suchen"
          " (JSON mit record_uid/title/details).\n"
          "3. Nutze 'get_secret(record_uid)', um die Details"
          " (Benutzername, Passwort, API-Keys) eines Records abzurufen."
          " Es muss die record_uid übergeben werden (kein Titel).\n"
          "4. Nutze 'get_totp(record_uid)', um den aktuellen 2FA /"
          " TOTP-Code für einen Record abzurufen. Es muss die record_uid"
          " übergeben werden (kein Titel)."
      ),
  )

  @mcp.tool()
  def list_secrets() -> str:
    """Listet alle Records im Keeper-Vault als JSON auf.

    Antwortet mit JSON: {"records": [{"record_uid": ..., "title": ...,
    "type": ..., "details": ..., "source": ...}]}.
    """
    print("[TOOL CALL] list_secrets aufgerufen (unrestricted)")

    def _fetch() -> str:
      out = run_keeper(["ls", "--list", "--format=json"])
      records = _parse_vault_json(out)
      return _to_json({"records": records})

    try:
      result, source = _call_with_cache("list:__all__", _fetch)
      print(f"[TOOL RESULT] list_secrets Quelle: {source} (unrestricted)")
      return result
    except Exception as e:
      cached = _cache_get("list:__all__")
      if cached is not None:
        return cached
      return _to_json({"records": [], "error": str(e)})

  @mcp.tool()
  def search_secrets(query: str) -> str:
    """Durchsucht den Keeper-Vault und antwortet mit JSON.

    Antwortet mit JSON: {"query": ...,
    "records": [{"record_uid": ..., "title": ..., "type": ...,
    "details": ..., "source": ...}]}.

    Args:
        query: Der Suchbegriff (z.B. 'OpenAI', 'Database', 'GitHub').
    """
    print(f"[TOOL CALL] search_secrets mit query='{query}' (unrestricted)")
    cache_key = f"search:__all__:{(query or '').strip().lower()}"

    def _fetch() -> str:
      raw = run_keeper(["search", query, "--format=json"])
      if not raw or "No records found" in raw:
        return _to_json(
            {"query": query, "records": [], "message": (
                f"Keine Treffer im Vault für '{query}' gefunden."
            )}
        )
      return _to_json({"query": query, "records": _parse_vault_json(raw)})

    try:
      result, source = _call_with_cache(cache_key, _fetch)
      print(f"[TOOL RESULT] search_secrets Quelle: {source} (unrestricted)")
      return result
    except Exception as e:
      cached = _cache_get(cache_key)
      if cached is not None:
        return cached
      return _to_json({"query": query, "records": [], "error": str(e)})

  @mcp.tool()
  def get_secret(record_uid: str) -> str:
    """Holt die vollständigen Details/Passwörter eines Secrets im JSON-Format.

    Es muss die record_uid übergeben werden (kein Titel).

    Args:
        record_uid: Record-UID (22 Zeichen) aus 'list_secrets' oder
          'search_secrets'.
    """
    print(f"[TOOL CALL] get_secret für '{record_uid}' (unrestricted)")
    target_uid = validate_record_uid(record_uid)
    result, source = _call_with_cache(
        f"get:{target_uid}",
        lambda: run_keeper(["get", target_uid, "--format=json"]),
    )
    print(f"[TOOL RESULT] get_secret Quelle: {source} (unrestricted)")
    return result

  @mcp.tool()
  def get_totp(record_uid: str) -> str:
    """Generiert den aktuellen 2FA / TOTP-Einmalcode und antwortet mit JSON.

    Antwortet mit JSON: {"record_uid": ..., "totp": ...}.

    Es muss die record_uid übergeben werden (kein Titel).

    Args:
        record_uid: Record-UID (22 Zeichen) aus 'list_secrets' oder
          'search_secrets'.
    """
    print(f"[TOOL CALL] get_totp für '{record_uid}' (unrestricted)")
    target_uid = validate_record_uid(record_uid)

    def _fetch() -> str:
      full_output = run_keeper_totp(target_uid)
      codes = re.findall(r"\b\d{6,8}\b", full_output)
      if codes:
        return _to_json({"record_uid": target_uid, "totp": codes[0]})
      if full_output:
        return _to_json({"record_uid": target_uid, "totp": None, "raw": full_output})
      raise RuntimeError(
          f"Für den Record '{target_uid}' konnte kein TOTP-Code generiert"
          " werden. Ist dort ein 2FA-Schlüssel hinterlegt?"
      )

    result, source = _call_with_cache(f"totp:{target_uid}", _fetch)
    print(f"[TOOL RESULT] get_totp Quelle: {source} (unrestricted)")
    return result

  return mcp


async def health(request):
  stats = _cache_stats()
  return JSONResponse(
      {
          "status": "healthy",
          "service": "keeper-mcp-server",
          "cache": {
              "entries": stats["entries"],
              "last_refresh": stats["last_refresh"],
              "refresh_interval_seconds": CACHE_REFRESH_SECONDS,
              "keeper_timeout_seconds": KEEPER_TIMEOUT_SECONDS,
          },
      }
  )


async def cache_status(request):
  stats = _cache_stats()
  return JSONResponse(
      {
          "service": "keeper-mcp-server",
          "refresh_interval_seconds": CACHE_REFRESH_SECONDS,
          "keeper_timeout_seconds": KEEPER_TIMEOUT_SECONDS,
          "entries": stats["entries"],
          "keys": stats["keys"],
          "last_refresh": stats["last_refresh"],
      }
  )


async def index(request):
  return JSONResponse(
      {
          "service": "keeper-mcp-server",
          "allowed_folders": ALLOWED_FOLDERS,
          "endpoints": (
              [f"{url_prefix_for_folder(f)}/mcp" for f in ALLOWED_FOLDERS]
              if ALLOWED_FOLDERS
              else ["/mcp"]
          ),
          "pattern": "<adresse>/<folder>/mcp",
          "cache": {
              "refresh_interval_seconds": CACHE_REFRESH_SECONDS,
              "keeper_timeout_seconds": KEEPER_TIMEOUT_SECONDS,
          },
      }
  )


def build_app() -> Starlette:
  """Baut die Starlette-App mit Pfaden '<adresse>/<folder>/mcp'.

  Jeder Folder aus ALLOWED_FOLDERS erhält eine eigene, strikt auf diesen
  Ordner beschränkte MCP-Instanz. Unbekannte Folder ergeben 404.
  """
  print(
      f"[CONFIG] Cache-Refresh-Intervall: {CACHE_REFRESH_SECONDS:.0f}s"
      f" (CACHE_REFRESH_SECONDS bzw. CACHE_REFRESH_HOURS),"
      f" Keeper-Timeout: {KEEPER_TIMEOUT_SECONDS:.0f}s"
  )
  if not ALLOWED_FOLDERS:
    print("[WARNUNG] ALLOWED_FOLDERS ist leer: Server läuft unrestricted unter /mcp.")
    mcp = create_unrestricted_mcp()
    mcp_app = mcp.http_app(path="/mcp")

    @asynccontextmanager
    async def lifespan(app: Starlette):
      async with AsyncExitStack() as stack:
        await stack.enter_async_context(mcp_app.lifespan(app))
        asyncio.create_task(keeper_keepalive_loop())
        asyncio.create_task(cache_refresh_loop())
        yield

    return Starlette(
        routes=[
            Route("/health", health),
            Route("/cache", cache_status),
            Route("/", index),
            Mount("/", app=mcp_app),
        ],
        lifespan=lifespan,
    )

  # Längste Präfixe zuerst, damit verschachtelte Ordner korrekt matchen.
  folders = sorted(ALLOWED_FOLDERS, key=len, reverse=True)
  mcp_apps = {}
  routes = [Route("/health", health), Route("/cache", cache_status), Route("/", index)]
  for folder in folders:
    prefix = url_prefix_for_folder(folder)
    mcp = create_mcp_for_folder(folder)
    # http_app mit Pfad "/mcp": finale URL = "<adresse>/<folder>/mcp".
    mcp_app = mcp.http_app(path="/mcp")
    mcp_apps[prefix] = mcp_app
    routes.append(Mount(prefix, app=mcp_app))
    print(f"[MOUNT] Ordner '{folder}' -> {prefix}/mcp")

  @asynccontextmanager
  async def lifespan(app: Starlette):
    async with AsyncExitStack() as stack:
      for mcp_app in mcp_apps.values():
        await stack.enter_async_context(mcp_app.lifespan(app))
      asyncio.create_task(keeper_keepalive_loop())
      asyncio.create_task(cache_refresh_loop())
      yield

  return Starlette(routes=routes, lifespan=lifespan)


app = build_app()


if __name__ == "__main__":
  import uvicorn

  try:
    who = run_keeper(["whoami"])
    print(f"Keeper Session aktiv für: {who.strip()}")
  except Exception as e:
    print(f"WARNUNG bei Start-Session-Check: {e}")

  uvicorn.run(app, host=HOST, port=PORT)

"""MyAnimeList loader, pusher, and OAuth token refresh."""
import json
import os
import time
from datetime import datetime, timezone

from anime_sync.http import request_with_retries, CircuitOpenError
from anime_sync.platforms.common import _push_skip_logged
from anime_sync.platforms.status import REVERSE_STATUS, STATUS_MAP
from anime_sync.ids import normalize_ids

import requests

_mal_token_refreshed = False

def _persist_mal_secrets(access_token, refresh_token=None):
    """Best-effort write updated MAL tokens back to GitHub Actions secrets via gh CLI."""
    repo = os.getenv("GITHUB_REPOSITORY", "dreaming144/anime-sync")
    token = os.getenv("SECRETS_WRITE_TOKEN") or os.getenv("GH_TOKEN") or os.getenv("GITHUB_TOKEN")
    if not token:
        print("   -> MAL token refreshed in-process only (no GH token to persist secrets)")
        return False
    env = {**os.environ, "GH_TOKEN": token, "GITHUB_TOKEN": token}
    ok = True
    for name, value in (
        ("MAL_ACCESS_TOKEN", access_token),
        ("MAL_REFRESH_TOKEN", refresh_token),
    ):
        if not value:
            continue
        try:
            import subprocess
            r = subprocess.run(
                ["gh", "secret", "set", name, "-R", repo],
                input=value.encode("utf-8"),
                capture_output=True,
                timeout=60,
                env=env,
            )
            if r.returncode == 0:
                print(f"   -> Persisted GitHub secret {name}")
            else:
                err = (r.stderr or r.stdout or b"").decode("utf-8", "replace")[:200]
                print(f"   -> Could not persist {name}: {err}")
                ok = False
        except (FileNotFoundError, subprocess.TimeoutExpired, OSError) as e:
            print(f"   -> Could not persist {name}: {e}")
            ok = False
    return ok



def refresh_mal_token(force=False):
    """Refresh MAL access token using MAL_REFRESH_TOKEN + client credentials.

    Updates os.environ['MAL_ACCESS_TOKEN'] (and refresh token if rotated).
    Safe to call multiple times — only hits the network once per process unless force=True.
    """
    global _mal_token_refreshed
    if _mal_token_refreshed and not force:
        return os.getenv("MAL_ACCESS_TOKEN")

    refresh = os.getenv("MAL_REFRESH_TOKEN")
    client_id = os.getenv("MAL_CLIENT_ID")
    client_secret = os.getenv("MAL_CLIENT_SECRET")
    if not refresh or not client_id:
        return os.getenv("MAL_ACCESS_TOKEN")

    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh,
        "client_id": client_id,
    }
    if client_secret:
        data["client_secret"] = client_secret

    try:
        r = request_with_retries(
            "POST",
            "https://myanimelist.net/v1/oauth2/token",
            data=data,
            timeout=20,
        )
        if not r.ok:
            print(f"   -> MAL token refresh failed: {r.status_code} {r.text[:200]}")
            # Keep existing access token if any
            return os.getenv("MAL_ACCESS_TOKEN")
        payload = r.json() or {}
        access = payload.get("access_token")
        new_refresh = payload.get("refresh_token")
        if not access:
            print("   -> MAL token refresh returned no access_token")
            return os.getenv("MAL_ACCESS_TOKEN")
        os.environ["MAL_ACCESS_TOKEN"] = access
        if new_refresh:
            os.environ["MAL_REFRESH_TOKEN"] = new_refresh
        _mal_token_refreshed = True
        expires = payload.get("expires_in")
        print(f"   -> MAL access token refreshed (expires_in={expires})")
        _persist_mal_secrets(access, new_refresh or refresh)
        return access
    except requests.RequestException as e:
        print(f"   -> MAL token refresh network error: {e}")
        return os.getenv("MAL_ACCESS_TOKEN")



def _mal_access_expiring_soon(token, skew_seconds=86400):
    """True if JWT access token is missing/expired or within skew of expiry."""
    if not token or token.count(".") < 2:
        return True
    try:
        import base64
        payload = token.split(".")[1]
        payload += "=" * (-len(payload) % 4)
        data = json.loads(base64.urlsafe_b64decode(payload.encode("ascii")))
        exp = int(data.get("exp") or 0)
        return exp < (time.time() + skew_seconds)
    except Exception:
        return True



def ensure_mal_token():
    """Return a usable MAL access token, refreshing when near expiry or missing."""
    token = os.getenv("MAL_ACCESS_TOKEN")
    can_refresh = bool(os.getenv("MAL_REFRESH_TOKEN") and os.getenv("MAL_CLIENT_ID"))
    if can_refresh and _mal_access_expiring_soon(token):
        token = refresh_mal_token() or token
    return token



def load_mal():
    token = ensure_mal_token()
    if not token:
        print("-> MAL skipped (no token)"); return []
    print("-> Fetching MAL...")
    headers = {"Authorization": f"Bearer {token}"}
    url = "https://api.myanimelist.net/v2/users/@me/animelist?fields=list_status,num_episodes&limit=1000&nsfw=true"
    items = []
    page = 1
    while url:
        try:
            r = request_with_retries("GET", url, headers=headers, timeout=30)
        except CircuitOpenError as e:
            print(f"   MAL skipped: {e}"); break
        if not r.ok:
            print(f"   MAL error page {page}: {r.text[:300]}")
            break
        data = r.json()
        for n in data.get("data", []):
            ls = n["list_status"]
            title = n["node"].get("title", "")
            items.append({
                "platform": "mal",
                "ids": normalize_ids({"mal": n["node"]["id"], "title": title}),
                "state": {
                    "status": STATUS_MAP["mal"].get(ls["status"], "plantowatch"),
                    "progress": ls["num_episodes_watched"],
                    "score": ls["score"],
                },
                "updated": datetime.fromisoformat(ls["updated_at"].replace("Z", "+00:00")),
                "title": title,
            })
        url = (data.get("paging") or {}).get("next")
        page += 1
        if url:
            print(f"   MAL page {page}...")
            time.sleep(0.3)
    print(f"   MAL: {len(items)} entries")
    return items


def push_mal(entry, state):
    """Create or update a MAL list entry via PUT /anime/{id}/my_list_status.

    Requires MAL_ACCESS_TOKEN with write:users scope.
    Body is application/x-www-form-urlencoded (not JSON).
    """
    token = ensure_mal_token()
    if not token:
        if "mal" not in _push_skip_logged:
            print("   -> PUSH MAL skipped (no MAL_ACCESS_TOKEN) [silencing further]")
            _push_skip_logged.add("mal")
        return
    mal_id = entry["ids"].get("mal")
    if not mal_id:
        return
    try:
        mal_id = int(mal_id)
    except (ValueError, TypeError):
        print(f"   -> PUSH MAL skipped (invalid mal id: {mal_id})")
        return

    status = REVERSE_STATUS["mal"].get(state["status"])
    if not status:
        print(f"   -> PUSH MAL skipped (unknown status: {state.get('status')})")
        return

    # MAL score is integer 0-10 (0 = unset)
    score = int(round(float(state.get("score") or 0)))
    if score < 0:
        score = 0
    if score > 10:
        score = 10

    payload = {
        "status": status,
        "num_watched_episodes": int(state.get("progress") or 0),
        "score": score,
    }
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    try:
        r = request_with_retries(
            "PUT",
            f"https://api.myanimelist.net/v2/anime/{mal_id}/my_list_status",
            data=payload,
            headers=headers,
            timeout=15,
        )
        if r.ok:
            print(f"   -> PUSH MAL {mal_id} => {state} [{r.status_code}]")
        else:
            print(f"   -> PUSH MAL failed {mal_id}: {r.status_code} {r.text[:200]}")
    except requests.RequestException as e:
        print(f"   -> PUSH MAL network error: {e}")





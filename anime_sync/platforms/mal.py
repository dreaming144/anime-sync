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
    """Fetch the authenticated user's anime list via MAL API v2.

    Explicitly requests nested list_status date fields:
      start_date, finish_date, updated_at, is_rewatching, num_times_rewatched
    See: https://myanimelist.net/apiconfig/references/api/v2
    """
    token = ensure_mal_token()
    if not token:
        print("-> MAL skipped (no token)"); return []
    print("-> Fetching MAL (API v2 + dates)...")
    headers = {"Authorization": f"Bearer {token}"}
    # Nested field selection is required for dates; bare list_status omits them.
    fields = (
        "list_status{"
        "status,score,num_episodes_watched,is_rewatching,"
        "start_date,finish_date,updated_at,num_times_rewatched,priority"
        "},num_episodes,start_date,end_date,media_type"
    )
    url = (
        "https://api.myanimelist.net/v2/users/@me/animelist"
        f"?fields={fields}&limit=1000&nsfw=true"
    )
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
            node = n.get("node") or {}
            ls = n.get("list_status") or {}
            title = node.get("title", "")
            # Prefer user start/finish; fall back never uses anime air dates
            # (node.start_date is series premiere, not user watch date).
            user_start = ls.get("start_date")
            user_finish = ls.get("finish_date")
            items.append({
                "platform": "mal",
                "ids": normalize_ids({"mal": node.get("id"), "title": title}),
                "state": {
                    "status": STATUS_MAP["mal"].get(ls.get("status"), "plantowatch"),
                    "progress": ls.get("num_episodes_watched") or 0,
                    "score": ls.get("score") or 0,
                    "is_rewatching": bool(ls.get("is_rewatching")),
                    "num_times_rewatched": int(ls.get("num_times_rewatched") or 0),
                },
                "dates": {
                    "started_at": user_start,
                    "completed_at": user_finish,
                },
                "updated": datetime.fromisoformat(
                    ls["updated_at"].replace("Z", "+00:00")
                ) if ls.get("updated_at") else datetime.now(timezone.utc),
                "title": title,
                "episodes": node.get("num_episodes"),
                "format": node.get("media_type"),
            })
        url = (data.get("paging") or {}).get("next")
        page += 1
        if url:
            print(f"   MAL page {page}...")
            time.sleep(0.35)
    print(f"   MAL: {len(items)} entries")
    return items


def push_mal(entry, state, dates=None):
    """Create or update a MAL list entry via PUT /anime/{id}/my_list_status (API v2).

    Date rules (oldest-wins):
      - start_date / finish_date only sent when we have a canonical date
      - never bump is_rewatching or num_times_rewatched (avoids rewatch pollution)
      - MAL only updates fields present in the form body

    Dates are zero-padded YYYY-MM-DD as preferred by MAL v2 clients.
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

    from anime_sync.dates import parse_date, to_mal_date, older

    score = int(round(float(state.get("score") or 0)))
    score = max(0, min(10, score))

    body = {
        "status": status,
        "num_watched_episodes": int(state.get("progress") or 0),
        "score": score,
        # Explicitly keep rewatch flags off unless the stored state says otherwise
        "is_rewatching": "true" if state.get("is_rewatching") else "false",
    }
    # Only preserve rewatch count if already on MAL and we're not resetting
    nrr = int(state.get("num_times_rewatched") or 0)
    if nrr > 0:
        body["num_times_rewatched"] = nrr

    dates = dates or entry.get("dates") or {}
    sources = (dates.get("sources") or {}) if isinstance(dates, dict) else {}
    mal_snap = sources.get("mal") or {}

    # Canonical oldest dates
    canon_start = parse_date(dates.get("started_at"))
    canon_finish = parse_date(dates.get("completed_at"))
    # What MAL already has (from last load)
    mal_start = parse_date(mal_snap.get("started_at"))
    mal_finish = parse_date(mal_snap.get("completed_at"))

    # Only write start_date when canonical is older than MAL's or MAL is missing
    start_d = None
    if canon_start and (mal_start is None or canon_start < mal_start):
        start_d = to_mal_date(canon_start)
        body["start_date"] = start_d
    elif canon_start and mal_start is None:
        start_d = to_mal_date(canon_start)
        body["start_date"] = start_d
    # If MAL already has equal or older, skip start_date field (partial update)

    finish_d = None
    if canon_finish and (mal_finish is None or canon_finish < mal_finish):
        finish_d = to_mal_date(canon_finish)
        body["finish_date"] = finish_d

    # If completed and we only have last_watched as completed hint already in completed_at
    if status == "completed" and not finish_d and canon_finish:
        finish_d = to_mal_date(canon_finish)
        body["finish_date"] = finish_d

    try:
        r = request_with_retries(
            "PUT",
            f"https://api.myanimelist.net/v2/anime/{mal_id}/my_list_status",
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/x-www-form-urlencoded",
            },
            data=body,
            timeout=20,
        )
        if r.ok:
            print(
                f"   -> PUSH MAL {mal_id} => {state.get('status')} "
                f"eps={body.get('num_watched_episodes')} "
                f"start={body.get('start_date')} finish={body.get('finish_date')} "
                f"[{r.status_code}]"
            )
        else:
            print(f"   -> PUSH MAL failed {mal_id}: {r.status_code} {r.text[:220]}")
        return r
    except requests.RequestException as e:
        print(f"   -> PUSH MAL network error: {e}")
        return None



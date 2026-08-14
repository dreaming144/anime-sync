# Two-way sync tokens (AniList / MAL / Kitsu)

Code already implements push for all three. You only need secrets.

## Already working
- **SIMKL** — `SIMKL_CLIENT_ID` + `SIMKL_ACCESS_TOKEN` present
- **Kitsu read** — `KITSU_USERNAME`
- **Kitsu write** — uses `KITSU_TOKEN` **or** auto-login via `KITSU_EMAIL` + `KITSU_PASSWORD` (you already have these)

## 1. AniList (`ANILIST_TOKEN`) — ~1 year lifetime

1. Open https://anilist.co/settings/developer  
2. **Create New Client**  
   - Name: `anime-sync`  
   - Redirect URL: `https://anilist.co/api/v2/oauth/pin`  
3. Note **Client ID**  
4. Open in browser (replace CLIENT_ID):

```
https://anilist.co/api/v2/oauth/authorize?client_id=CLIENT_ID&response_type=token
```

5. Approve → copy the **access_token** from the redirect page/URL  
6. GitHub repo → **Settings → Secrets and variables → Actions**  
   - New secret: `ANILIST_TOKEN` = that token  

Also keep `ANILIST_USERNAME` set (you already do).

## 2. MyAnimeList (`MAL_ACCESS_TOKEN`) — expires in weeks

1. https://myanimelist.net/apiconfig → **Create ID**  
   - App Type: `web` or `other`  
   - Redirect URL: `http://localhost:8080`  
2. Generate PKCE verifier (bash):

```bash
python3 -c "import secrets; print(secrets.token_urlsafe(64)[:128])"
```

3. Open authorize URL (replace CLIENT_ID and VERIFIER):

```
https://myanimelist.net/v1/oauth2/authorize?response_type=code&client_id=CLIENT_ID&redirect_uri=http://localhost:8080&code_challenge=VERIFIER&code_challenge_method=plain
```

4. After approve, copy `code` from the browser address bar  
5. Exchange for tokens:

```bash
curl -X POST https://myanimelist.net/v1/oauth2/token \
  -d "client_id=CLIENT_ID" \
  -d "code=THE_CODE" \
  -d "code_verifier=VERIFIER" \
  -d "grant_type=authorization_code" \
  -d "redirect_uri=http://localhost:8080"
```

6. Save secrets:
   - `MAL_ACCESS_TOKEN` = `access_token`  
   - (optional later) `MAL_REFRESH_TOKEN` + `MAL_CLIENT_ID` for auto-refresh  

Token needs list write permission (`write:users` capability of the user token).

## 3. Kitsu — already set up if email/password secrets exist

Preferred: leave `KITSU_EMAIL` + `KITSU_PASSWORD` as you have them.  
The script requests an OAuth token automatically at push time.

Optional: set `KITSU_TOKEN` manually if you prefer not to store password.

## Verify

Actions → **Universal Anime Sync V3.12** → Run workflow.  
Logs should show pushes without “skipped (no … TOKEN)” for platforms you configured.

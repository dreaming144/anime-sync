"""Shared helpers for the HTTP resilience layer."""
from urllib.parse import urlparse


def service_key(url):
    """Map a request URL to a short service name (anilist, mal, jikan, ...)."""
    try:
        host = urlparse(url).netloc.lower()
    except Exception:
        return "unknown"
    if "anilist" in host:
        return "anilist"
    if "myanimelist" in host:
        return "mal"
    if "kitsu" in host:
        return "kitsu"
    if "simkl" in host:
        return "simkl"
    if "yuna.moe" in host or "haglund" in host:
        return "arm"
    if "jikan" in host:
        return "jikan"
    if "anizip" in host:
        return "anizip"
    if "animeapi" in host or "nattadasu" in host:
        return "animeapi"
    if "github" in host:
        return "github"
    return host or "unknown"


# Back-compat alias used throughout the monolith historically
_service_key = service_key

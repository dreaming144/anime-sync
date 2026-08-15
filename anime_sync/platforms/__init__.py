"""Platform loaders and pushers."""
from anime_sync.platforms.anilist import load_anilist, push_anilist
from anime_sync.platforms.kitsu import ensure_kitsu_token, load_kitsu, push_kitsu
from anime_sync.platforms.mal import (
    ensure_mal_token,
    load_mal,
    push_mal,
    refresh_mal_token,
)
from anime_sync.platforms.simkl import load_simkl, push_simkl
from anime_sync.platforms.status import REVERSE_STATUS, STATUS_MAP

PUSHERS = {
    "anilist": push_anilist,
    "mal": push_mal,
    "kitsu": push_kitsu,
    "simkl": push_simkl,
}
LOADERS = {
    "anilist": load_anilist,
    "mal": load_mal,
    "kitsu": load_kitsu,
    "simkl": load_simkl,
}

__all__ = [
    "LOADERS",
    "PUSHERS",
    "REVERSE_STATUS",
    "STATUS_MAP",
    "ensure_kitsu_token",
    "ensure_mal_token",
    "load_anilist",
    "load_kitsu",
    "load_mal",
    "load_simkl",
    "push_anilist",
    "push_kitsu",
    "push_mal",
    "push_simkl",
    "refresh_mal_token",
]

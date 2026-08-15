"""Canonical status maps between internal state and each platform."""
STATUS_MAP = {
    "anilist": {"CURRENT": "watching", "COMPLETED": "completed", "PLANNING": "plantowatch", "DROPPED": "dropped", "PAUSED": "on_hold", "REPEATING": "watching"},
    "mal": {"watching": "watching", "completed": "completed", "plan_to_watch": "plantowatch", "dropped": "dropped", "on_hold": "on_hold"},
    "kitsu": {"current": "watching", "completed": "completed", "planned": "plantowatch", "dropped": "dropped", "on_hold": "on_hold"},
    "simkl": {"watching": "watching", "completed": "completed", "plantowatch": "plantowatch", "dropped": "dropped", "hold": "on_hold"}
}
REVERSE_STATUS = {
    "anilist": {"watching": "CURRENT", "completed": "COMPLETED", "plantowatch": "PLANNING", "dropped": "DROPPED", "on_hold": "PAUSED"},
    "mal": {"watching": "watching", "completed": "completed", "plantowatch": "plan_to_watch", "dropped": "dropped", "on_hold": "on_hold"},
    "kitsu": {"watching": "current", "completed": "completed", "plantowatch": "planned", "dropped": "dropped", "on_hold": "on_hold"},
    "simkl": {"watching": "watching", "completed": "completed", "plantowatch": "plantowatch", "dropped": "dropped", "on_hold": "hold"}
}

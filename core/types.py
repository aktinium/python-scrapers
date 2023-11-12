from typing import TypedDict, Any

ScrapeResult = TypedDict(
    "ScrapeResult", {"url": str, "is_successful": bool, "data": dict[str, Any]}
)

from typing import Any

import requests
from singer_sdk.helpers.jsonpath import extract_jsonpath

from tap_tiktok.clients import TikTokStream


class TikTokBasicReportStream(TikTokStream):

    url_base = "https://business-api.tiktok.com/open_api/v1.3/report/integrated/get/"

    records_jsonpath = "$.data.list[*]"
    next_page_token_jsonpath = "$.page_info.page"

    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        return {**row["dimensions"], **row["metrics"]}

    def get_next_page_token(self, response: requests.Response, previous_token: Any | None) -> Any | None:
        """Return a token for identifying next page or None if no more pages."""
        page_matches = extract_jsonpath("$.data.page_info.page", response.json())
        page_match = next(iter(page_matches), None)
        current_page = page_match
        total_pages_matches = extract_jsonpath("$.data.page_info.total_page", response.json())
        total_pages_match = next(iter(total_pages_matches), None)
        total_pages = total_pages_match
        if current_page < total_pages:
            return current_page + 1
        return None

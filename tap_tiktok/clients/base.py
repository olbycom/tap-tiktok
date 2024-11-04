"""REST client handling, including TikTokStream base class."""

import json
from http import HTTPStatus
from typing import Any, Dict, Optional

import requests
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream

DATE_FORMAT = "%Y-%m-%d"


class TikTokStream(RESTStream):

    url_base = "https://business-api.tiktok.com/open_api/v1.3"
    # url_base = "https://sandbox-ads.tiktok.com/open_api/v1.3"

    records_jsonpath = "$.data.list[*]"

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        headers["Content-Type"] = "application/json"
        headers["Access-Token"] = self.config["access_token"].__str__()
        return headers

    @staticmethod
    def _get_page_info(json_path, json):
        page_matches = extract_jsonpath(json_path, json)
        return next(iter(page_matches), None)

    def get_next_page_token(self, response: requests.Response, previous_token: Optional[Any]) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        current_page = self._get_page_info("$.data.page_info.page", response.json()) or 0
        total_pages = self._get_page_info("$.data.page_info.total_page", response.json()) or 0
        if current_page < total_pages:
            return current_page + 1
        return None

    def get_url_params(self, context: Optional[dict], next_page_token: Optional[Any]) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {"advertiser_id": self.config["advertiser_id"]}
        if next_page_token:
            params["page"] = next_page_token
        params["filtering"] = json.dumps(
            {"primary_status": "STATUS_ALL" if self.config.get("include_deleted") else "STATUS_NOT_DELETE"}
        )
        params["page_size"] = 1000
        return params

    def validate_response(self, response: requests.Response) -> None:
        if response.status_code == HTTPStatus.OK:
            response_json = response.json()
            message = response_json.get("message")
            code = response_json.get("code")
            if message != "OK":
                if self.name == "ad_accounts":
                    # skip this temporarily since it might be a permission issue
                    return super().validate_response(response)
                raise RuntimeError(f"Error calling {response.request.url}. API response: Code ({code}) - {message}")

        return super().validate_response(response)

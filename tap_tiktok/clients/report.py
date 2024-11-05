import abc
import datetime
import json
from typing import Any
from urllib.parse import parse_qs, urlparse

import pendulum
import requests
from singer_sdk import typing as th
from singer_sdk.helpers.jsonpath import extract_jsonpath

from .base import TikTokStream

DATE_FORMAT = "%Y-%m-%d"
STEP_NUM_DAYS = 30


class TikTokReportStream(TikTokStream, metaclass=abc.ABCMeta):

    @property
    @abc.abstractmethod
    def report_specific_properties(self) -> th.PropertiesList:
        """Data level of the report"""

    @property
    @abc.abstractmethod
    def data_level(self) -> str:
        """Data level of the report"""

    @property
    @abc.abstractmethod
    def dimensions(self) -> str:
        """Dimensions of the report"""

    url_base = "https://business-api.tiktok.com/open_api/v1.3/report/integrated/get/"

    records_jsonpath = "$.data.list[*]"
    next_page_token_jsonpath = "$.page_info.page"

    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        return {**row["dimensions"], **row["metrics"]}

    # TODO: Move this to a paginator class
    @staticmethod
    def _get_page_info(json_path, json):
        page_matches = extract_jsonpath(json_path, json)
        return next(iter(page_matches), None)

    # TODO: Move this to a paginator class
    def get_next_page_token(self, response: requests.Response, previous_token: Any | None) -> Any | None:
        """Return a token for identifying next page or None if no more pages."""
        current_page = self._get_page_info("$.data.page_info.page", response.json()) or 0
        total_pages = self._get_page_info("$.data.page_info.total_page", response.json()) or 0
        start_date = datetime.datetime.strptime(
            parse_qs(urlparse(response.request.url).query)["start_date"][0], DATE_FORMAT
        )
        yesterday = datetime.datetime.now(tz=start_date.tzinfo) - datetime.timedelta(days=1)
        end_date = datetime.datetime.strptime(
            parse_qs(urlparse(response.request.url).query)["end_date"][0], DATE_FORMAT
        )
        if current_page < total_pages:
            return {"page": current_page + 1, "start_date": previous_token["start_date"] if previous_token else None}
        elif end_date.date() < yesterday.date():
            return {
                "page": 1,
                "start_date": min(end_date + datetime.timedelta(days=1), yesterday).strftime(DATE_FORMAT),
            }
        return None

    def get_url_params(self, context: dict | None, next_page_token: Any | None) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        if isinstance(next_page_token, dict) and next_page_token["start_date"] is not None:
            start_date = datetime.datetime.strptime(next_page_token["start_date"], DATE_FORMAT)
        else:
            start_date = self.get_starting_timestamp(context)

            # picking up where we left off on the last run (or first run), adjust for lookback if set
            lookback_window = self.config["lookback"]
            if lookback_window > 0:
                # if lookback is configured, we want to refetch data for the entire lookback window
                # (or as far back as the configured start date, whichever is the most recent date)
                start_date = max(
                    min(
                        start_date,
                        datetime.datetime.now(tz=start_date.tzinfo) - datetime.timedelta(days=lookback_window),
                    ),
                    pendulum.parse(self.config["start_date"]),
                )

        yesterday = datetime.datetime.now(tz=start_date.tzinfo) - datetime.timedelta(days=1)
        end_date = min(start_date + datetime.timedelta(days=STEP_NUM_DAYS), yesterday)
        self.logger.info(
            f"Preparing request for {start_date.strftime(DATE_FORMAT)} - {end_date.strftime(DATE_FORMAT)}."
        )
        params: dict = {
            "page_size": 1000,
            "advertiser_id": self.config.get("advertiser_id"),
            "service_type": "AUCTION",
            "report_type": self.report_type,
            "data_level": self.data_level,
            "dimensions": json.dumps(self.dimensions),
            "metrics": json.dumps(self.metrics_keys),
            "start_date": start_date.strftime(DATE_FORMAT),
            "end_date": end_date.strftime(DATE_FORMAT),
            "filtering": json.dumps(
                [
                    {
                        "field_name": self.status_field,
                        "filter_type": "IN",
                        "filter_value": json.dumps(
                            ["STATUS_ALL" if self.config.get("include_deleted") else "STATUS_NOT_DELETE"]
                        ),
                    }
                ]
            ),
        }
        if next_page_token:
            params["page"] = next_page_token["page"]

        return params
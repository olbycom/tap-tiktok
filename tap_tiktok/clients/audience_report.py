import abc
import datetime
import json
from functools import cached_property
from typing import Any
from urllib.parse import parse_qs, urlparse

import pendulum
import requests
from singer_sdk import typing as th
from singer_sdk.helpers.jsonpath import extract_jsonpath

from tap_tiktok.clients import TikTokBasicReportStream

DATE_FORMAT = "%Y-%m-%d"
STEP_NUM_DAYS = 30


class TikTokAudienceReportStream(TikTokBasicReportStream, metaclass=abc.ABCMeta):
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

    report_type = "AUDIENCE"
    path = "/"
    replication_key = "stat_time_day"

    metrics_properties = th.PropertiesList(
        th.Property("spend", th.StringType, description="Total Cost"),
        th.Property("cpc", th.StringType, description="CPC"),
        th.Property("cpm", th.StringType, description="CPM"),
        th.Property("impressions", th.StringType, description="Impressions"),
        th.Property("gross_impressions", th.StringType, description="Gross Impressions (Includes Invalid Impressions)"),
        th.Property("clicks", th.StringType, description="Clicks"),
        th.Property("ctr", th.StringType, description="CTR (%)"),
        th.Property("conversion", th.StringType, description="Conversion"),
        th.Property("cost_per_conversion", th.StringType, description="CPA"),
        th.Property("conversion_rate", th.StringType, description="CVR (%)"),
        th.Property("real_time_conversion", th.StringType, description="Real-time Conversions"),
        th.Property("real_time_cost_per_conversion", th.StringType, description="Real-time CPA"),
        th.Property("real_time_conversion_rate", th.StringType, description="Real-time CVR (%)"),
        th.Property("result", th.StringType, description="Result"),
        th.Property("cost_per_result", th.StringType, description="Cost Per Result"),
        th.Property("result_rate", th.StringType, description="Result Rate (%)"),
        th.Property("real_time_result", th.StringType, description="Real-time Result"),
        th.Property("real_time_cost_per_result", th.StringType, description="Real-time Cost Per Result"),
        th.Property("real_time_result_rate", th.StringType, description="Real-time Result Rate (%)"),
    )

    @cached_property
    def schema(self) -> dict:
        return th.PropertiesList(
            th.Property("campaign_id", th.StringType),
            th.Property("stat_time_day", th.DateTimeType),
            *self.metrics_properties,
            *self.report_specific_properties,
        ).to_dict()

    @cached_property
    def metrics_keys(self) -> list[str]:
        return list(self.metrics_properties.to_dict()["properties"].keys())

    # TODO: Move most of this to a paginator class
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

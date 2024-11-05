import abc
import json
import typing as t
from functools import cached_property
from typing import Any

import pendulum
from singer_sdk import metrics
from singer_sdk import typing as th
from singer_sdk.streams.core import Context

from tap_tiktok.pagination import (
    BaseAPIPaginator,
    DailyReportPaginator,
    HourlyReportPaginator,
)

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

    @property
    @abc.abstractmethod
    def buying_types(self) -> list[str]:
        """The buying type. Values: AUCTION, RESERVATION_TOP_VIEW, RESERVATION_RF"""

    @property
    @abc.abstractmethod
    def report_type(self) -> str:
        """Report type"""

    @property
    @abc.abstractmethod
    def pagination_class(self) -> BaseAPIPaginator:
        """Pagination class that will be used to query the API"""

    @cached_property
    def primary_keys(self) -> list[str]:
        return self.dimensions

    @cached_property
    def metrics_keys(self) -> list[str]:
        return list(self.metrics_properties.to_dict()["properties"].keys())

    url_base = "https://business-api.tiktok.com/open_api/v1.3/report/integrated/get/"

    path = "/"

    records_jsonpath = "$.data.list[*]"
    next_page_token_jsonpath = "$.page_info.page"

    def _get_start_datetime(self, context: Context | None) -> pendulum.DateTime:
        start_date: pendulum.DateTime = self.get_starting_timestamp(context)
        lookback_window = self.config["lookback"]
        if lookback_window > 0:
            start_date = max(
                min(start_date, pendulum.now(tz=start_date.tzinfo).subtract(days=lookback_window)),
                pendulum.parse(self.config["start_date"]),
            )
        return start_date

    def get_new_paginator(self):
        start_date = self._get_start_datetime(context=None)
        return self.pagination_class(start_date)

    def get_url_params(self, context: dict | None, next_page_token: Any | None) -> dict[str, Any]:
        params: dict = {
            "advertiser_id": self.config.get("advertiser_id"),
            "service_type": "AUCTION",
            "report_type": self.report_type,
            "data_level": self.data_level,
            "dimensions": json.dumps(self.dimensions),
            "metrics": json.dumps(self.metrics_keys),
            "filtering": json.dumps(
                [
                    {
                        "field_name": self.status_field,
                        "filter_type": "IN",
                        "filter_value": json.dumps(
                            ["STATUS_ALL" if self.config.get("include_deleted") else "STATUS_NOT_DELETE"]
                        ),
                    },
                    {
                        "field_name": "buying_type",
                        "filter_type": "IN",
                        "filter_value": json.dumps(self.buying_types),
                    },
                ]
            ),
        }
        params = {
            **params,
            **next_page_token,
        }
        self.logger.info(f"Preparing request for {next_page_token['start_date']} - {next_page_token['end_date']}.")

        return params

    def request_records(self, context: Context | None) -> t.Iterable[dict]:
        paginator = self.get_new_paginator()
        decorated_request = self.request_decorator(self._request)
        pages = 0

        with metrics.http_request_counter(self.name, self.path) as request_counter:
            request_counter.context = context

            while not paginator.finished:
                prepared_request = self.prepare_request(
                    context,
                    next_page_token=paginator.current_value,
                )
                resp = decorated_request(prepared_request, context)
                request_counter.increment()
                self.update_sync_costs(prepared_request, resp, context)
                records = iter(self.parse_response(resp))
                # try:
                #     first_record = next(records)
                # except StopIteration:
                #     self.logger.info(
                #         "Pagination stopped after %d pages because no records were "
                #         "found in the last response",
                #         pages,
                #     )
                #     break
                # yield first_record
                yield from records
                pages += 1

                paginator.advance(resp)

    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        return {**row["dimensions"], **row["metrics"]}

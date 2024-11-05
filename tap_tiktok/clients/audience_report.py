import abc
from functools import cached_property

from singer_sdk import typing as th

from .report import TikTokReportStream


class TikTokAudienceReportStream(TikTokReportStream, metaclass=abc.ABCMeta):

    report_type = "AUDIENCE"
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
            th.Property("stat_time_day", th.DateTimeType, description="Group by day"),
            *self.metrics_properties,
            *self.report_specific_properties,
        ).to_dict()

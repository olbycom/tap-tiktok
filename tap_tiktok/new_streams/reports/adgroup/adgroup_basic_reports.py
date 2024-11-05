from singer_sdk import typing as th

from tap_tiktok.clients import TikTokBasicReportStream


class AdgroupBasicReportStream(TikTokBasicReportStream):
    data_level = "AUCTION_ADGROUP"
    status_field = "adgroup_status"


class AdgroupDailyReportStream(AdgroupBasicReportStream):
    name = "adgroup_daily_report"
    dimensions = ["adgroup_id", "stat_time_day"]
    replication_key = "stat_time_day"
    report_specific_properties = th.PropertiesList(
        th.Property("adgroup_id", th.StringType, description="Group by adgroup id"),
        th.Property("stat_time_day", th.DateTimeType, description="Group by day"),
    )


class AdgroupDailyReportStream(AdgroupBasicReportStream):
    name = "adgroup_daily_report"
    dimensions = ["adgroup_id", "stat_time_day"]
    replication_key = "stat_time_day"
    report_specific_properties = th.PropertiesList(
        th.Property("adgroup_id", th.StringType, description="Group by adgroup id"),
        th.Property("stat_time_day", th.DateTimeType, description="Group by day"),
    )

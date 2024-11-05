from singer_sdk import typing as th

from tap_tiktok.clients import (
    TikTokDailyBasicReportStream,
    TikTokHourlyBasicReportStream,
)


class AdgroupDailyBasicReportStream(TikTokDailyBasicReportStream):
    data_level = "AUCTION_ADGROUP"
    status_field = "adgroup_status"


class AdgroupHourlyBasicReportStream(TikTokHourlyBasicReportStream):
    data_level = "AUCTION_ADGROUP"
    status_field = "adgroup_status"


class AdgroupDailyReportStream(AdgroupDailyBasicReportStream):
    name = "adgroup_daily_report"
    dimensions = ["adgroup_id", "stat_time_day"]
    replication_key = "stat_time_day"
    buying_types = ["AUCTION"]
    report_specific_properties = th.PropertiesList(
        th.Property("adgroup_id", th.StringType, description="Group by adgroup id"),
        th.Property("stat_time_day", th.DateTimeType, description="Group by day"),
    )


class AdgroupReservationDailyReportStream(AdgroupDailyBasicReportStream):
    name = "adgroup_reservation_daily_report"
    dimensions = ["adgroup_id", "stat_time_day"]
    replication_key = "stat_time_day"
    buying_types = ["RESERVATION_TOP_VIEW", "RESERVATION_RF"]
    report_specific_properties = th.PropertiesList(
        th.Property("adgroup_id", th.StringType, description="Group by adgroup id"),
        th.Property("stat_time_day", th.DateTimeType, description="Group by day"),
    )


class AdgroupHourlyReportStream(AdgroupHourlyBasicReportStream):
    name = "adgroup_hourly_report"
    dimensions = ["adgroup_id", "stat_time_hour"]
    replication_key = "stat_time_hour"
    buying_types = ["AUCTION"]
    report_specific_properties = th.PropertiesList(
        th.Property("adgroup_id", th.StringType, description="Group by adgroup id"),
        th.Property("stat_time_hour", th.DateTimeType, description="Group by hour"),
    )


class AdgroupReservationHourlyReportStream(AdgroupHourlyBasicReportStream):
    name = "adgroup_reservation_hourly_report"
    dimensions = ["adgroup_id", "stat_time_hour"]
    replication_key = "stat_time_hour"
    buying_types = ["RESERVATION_TOP_VIEW", "RESERVATION_RF"]
    report_specific_properties = th.PropertiesList(
        th.Property("adgroup_id", th.StringType, description="Group by adgroup id"),
        th.Property("stat_time_hour", th.DateTimeType, description="Group by hour"),
    )

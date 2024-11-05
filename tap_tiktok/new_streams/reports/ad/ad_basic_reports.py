from singer_sdk import typing as th

from tap_tiktok.clients import TikTokBasicReportStream


class AdBasicReportStream(TikTokBasicReportStream):
    data_level = "AUCTION_AD"
    status_field = "ad_status"


class AdDailyReportStream(AdBasicReportStream):
    name = "ad_daily_report"
    dimensions = ["ad_id", "stat_time_day"]
    replication_key = "stat_time_day"
    buying_types = ["AUCTION"]
    report_specific_properties = th.PropertiesList(
        th.Property("ad_id", th.StringType, description="Group by ad id"),
        th.Property("stat_time_day", th.DateTimeType, description="Group by day"),
    )


class AdReservationDailyReportStream(AdBasicReportStream):
    name = "ad_reservation_daily_report"
    dimensions = ["ad_id", "stat_time_day"]
    replication_key = "stat_time_day"
    buying_types = ["RESERVATION_TOP_VIEW", "RESERVATION_RF"]
    report_specific_properties = th.PropertiesList(
        th.Property("ad_id", th.StringType, description="Group by ad id"),
        th.Property("stat_time_day", th.DateTimeType, description="Group by day"),
    )

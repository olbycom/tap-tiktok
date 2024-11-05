from singer_sdk import typing as th

from tap_tiktok.clients import (
    TikTokDailyBasicReportStream,
    TikTokHourlyBasicReportStream,
)


class CampaignDailyBasicReportStream(TikTokDailyBasicReportStream):
    data_level = "AUCTION_CAMPAIGN"
    status_field = "campaign_status"


class CampaignHourlyBasicReportStream(TikTokHourlyBasicReportStream):
    data_level = "AUCTION_CAMPAIGN"
    status_field = "campaign_status"


class CampaignDailyReportStream(CampaignDailyBasicReportStream):
    name = "campaign_daily_report"
    dimensions = ["campaign_id", "stat_time_day"]
    replication_key = "stat_time_day"
    buying_types = ["AUCTION"]
    report_specific_properties = th.PropertiesList(
        th.Property("campaign_id", th.StringType, description="Group by campaign id"),
        th.Property("stat_time_day", th.DateTimeType, description="Group by day"),
    )


class CampaignReservationDailyReportStream(CampaignDailyBasicReportStream):
    name = "campaign_reservation_daily_report"
    dimensions = ["campaign_id", "stat_time_day"]
    replication_key = "stat_time_day"
    buying_types = ["RESERVATION_TOP_VIEW", "RESERVATION_RF"]
    report_specific_properties = th.PropertiesList(
        th.Property("campaign_id", th.StringType, description="Group by campaign id"),
        th.Property("stat_time_day", th.DateTimeType, description="Group by day"),
    )


class CampaignHourlyReportStream(CampaignHourlyBasicReportStream):
    name = "campaign_hourly_report"
    dimensions = ["campaign_id", "stat_time_hour"]
    replication_key = "stat_time_hour"
    buying_types = ["AUCTION"]
    report_specific_properties = th.PropertiesList(
        th.Property("campaign_id", th.StringType, description="Group by campaign id"),
        th.Property("stat_time_hour", th.DateTimeType, description="Group by hour"),
    )


class CampaignReservationHourlyReportStream(CampaignHourlyBasicReportStream):
    name = "campaign_reservation_hourly_report"
    dimensions = ["campaign_id", "stat_time_hour"]
    replication_key = "stat_time_hour"
    buying_types = ["RESERVATION_TOP_VIEW", "RESERVATION_RF"]
    report_specific_properties = th.PropertiesList(
        th.Property("campaign_id", th.StringType, description="Group by campaign id"),
        th.Property("stat_time_hour", th.DateTimeType, description="Group by hour"),
    )

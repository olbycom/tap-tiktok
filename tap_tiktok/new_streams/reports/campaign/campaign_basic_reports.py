from singer_sdk import typing as th

from tap_tiktok.clients import TikTokBasicReportStream


class CampaignBasicReportStream(TikTokBasicReportStream):
    data_level = "AUCTION_CAMPAIGN"
    status_field = "campaign_status"


class CampaignDailyReportStream(CampaignBasicReportStream):
    name = "campaign_daily_report"
    dimensions = ["campaign_id", "stat_time_day"]
    replication_key = "stat_time_day"
    report_specific_properties = th.PropertiesList(
        th.Property("campaign_id", th.StringType, description="Group by campaign id"),
        th.Property("stat_time_day", th.DateTimeType, description="Group by day"),
    )


class CampaignDailyReportStream(CampaignBasicReportStream):
    name = "campaign_daily_report"
    dimensions = ["campaign_id", "stat_time_day"]
    replication_key = "stat_time_day"
    report_specific_properties = th.PropertiesList(
        th.Property("campaign_id", th.StringType, description="Group by campaign id"),
        th.Property("stat_time_day", th.DateTimeType, description="Group by day"),
    )


# class CampaignHourlyReportStream(CampaignBasicReportStream):
#     name = "campaign_hourly_report"
#     dimensions = ["campaign_id", "stat_time_hour"]
#     replication_key = "stat_time_hour"
#     report_specific_properties = th.PropertiesList(
#         th.Property("campaign_id", th.StringType, description="Group by campaign id"),
#         th.Property("stat_time_hour", th.DateTimeType, description="Group by hour"),
#     )

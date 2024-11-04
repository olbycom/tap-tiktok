from singer_sdk import typing as th

from tap_tiktok.clients.audience_report import TikTokAudienceReportStream


class CampaignAudienceReportStream(TikTokAudienceReportStream):
    data_level = "AUCTION_CAMPAIGN"
    status_field = "campaign_status"


class CampaignAgeGenderReportStream(CampaignAudienceReportStream):
    name = "campaign_age_gender_report"
    dimensions = ["campaign_id", "age", "gender", "stat_time_day"]
    primary_keys = ["campaign_id", "age", "gender", "stat_time_day"]
    report_specific_properties = th.PropertiesList(
        th.Property("age", th.StringType),
        th.Property("gender", th.StringType),
    )


class CampaignCountryReportStream(CampaignAudienceReportStream):
    name = "campaign_country_report"
    dimensions = ["campaign_id", "country_code", "stat_time_day"]
    primary_keys = ["campaign_id", "country_code", "stat_time_day"]
    report_specific_properties = th.PropertiesList(
        th.Property("country_code", th.StringType),
    )


class CampaignLanguageReportStream(CampaignAudienceReportStream):
    name = "campaign_language_report"
    dimensions = ["campaign_id", "language", "stat_time_day"]
    primary_keys = ["campaign_id", "language", "stat_time_day"]
    report_specific_properties = th.PropertiesList(
        th.Property("language", th.StringType),
    )


class CampaignPlatformReportStream(CampaignAudienceReportStream):
    name = "campaign_platform_report"
    dimensions = ["campaign_id", "platform", "stat_time_day"]
    primary_keys = ["campaign_id", "platform", "stat_time_day"]
    report_specific_properties = th.PropertiesList(
        th.Property("platform", th.StringType),
    )

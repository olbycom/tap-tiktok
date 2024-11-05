from singer_sdk import typing as th

from tap_tiktok.clients import TikTokAudienceReportStream


class CampaignAudienceReportStream(TikTokAudienceReportStream):
    data_level = "AUCTION_CAMPAIGN"
    status_field = "campaign_status"
    buying_types = ["AUCTION", "RESERVATION_TOP_VIEW", "RESERVATION_RF"]


class CampaignAgeGenderReportStream(CampaignAudienceReportStream):
    name = "campaign_age_gender_report"
    dimensions = ["campaign_id", "age", "gender", "stat_time_day"]
    report_specific_properties = th.PropertiesList(
        th.Property("campaign_id", th.StringType, description="Group by campaign id"),
        th.Property("age", th.StringType, description="Group by age"),
        th.Property("gender", th.StringType, description="Group by gender"),
    )


class CampaignCountryReportStream(CampaignAudienceReportStream):
    name = "campaign_country_report"
    dimensions = ["campaign_id", "country_code", "stat_time_day"]
    report_specific_properties = th.PropertiesList(
        th.Property("campaign_id", th.StringType, description="Group by campaign id"),
        th.Property("country_code", th.StringType, description="Group by location code"),
    )


class CampaignLanguageReportStream(CampaignAudienceReportStream):
    name = "campaign_language_report"
    dimensions = ["campaign_id", "language", "stat_time_day"]
    report_specific_properties = th.PropertiesList(
        th.Property("campaign_id", th.StringType, description="Group by campaign id"),
        th.Property("language", th.StringType, description="Group by location"),
    )


class CampaignPlatformReportStream(CampaignAudienceReportStream):
    name = "campaign_platform_report"
    dimensions = ["campaign_id", "platform", "stat_time_day"]
    report_specific_properties = th.PropertiesList(
        th.Property("campaign_id", th.StringType, description="Group by campaign id"),
        th.Property("platform", th.StringType, description="Group by platform"),
    )

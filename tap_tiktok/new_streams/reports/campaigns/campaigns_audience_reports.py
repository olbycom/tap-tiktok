from singer_sdk import typing as th

from tap_tiktok.clients import TikTokAudienceReportStream


class CampaignsAudienceReportStream(TikTokAudienceReportStream):
    data_level = "AUCTION_CAMPAIGN"
    status_field = "campaign_status"
    buying_types = ["AUCTION", "RESERVATION_TOP_VIEW", "RESERVATION_RF"]


class CampaignsAgeGenderReportStream(CampaignsAudienceReportStream):
    name = "campaigns_age_gender_report"
    dimensions = ["campaign_id", "age", "gender", "stat_time_day"]
    report_specific_properties = th.PropertiesList(
        th.Property("campaign_id", th.StringType, description="Group by campaign id"),
        th.Property("age", th.StringType, description="Group by age"),
        th.Property("gender", th.StringType, description="Group by gender"),
    )


class CampaignsCountryReportStream(CampaignsAudienceReportStream):
    name = "campaigns_country_report"
    dimensions = ["campaign_id", "country_code", "stat_time_day"]
    report_specific_properties = th.PropertiesList(
        th.Property("campaign_id", th.StringType, description="Group by campaign id"),
        th.Property("country_code", th.StringType, description="Group by location code"),
    )


class CampaignsLanguageReportStream(CampaignsAudienceReportStream):
    name = "campaigns_language_report"
    dimensions = ["campaign_id", "language", "stat_time_day"]
    report_specific_properties = th.PropertiesList(
        th.Property("campaign_id", th.StringType, description="Group by campaign id"),
        th.Property("language", th.StringType, description="Group by location"),
    )


class CampaignsPlatformReportStream(CampaignsAudienceReportStream):
    name = "campaigns_platform_report"
    dimensions = ["campaign_id", "platform", "stat_time_day"]
    report_specific_properties = th.PropertiesList(
        th.Property("campaign_id", th.StringType, description="Group by campaign id"),
        th.Property("platform", th.StringType, description="Group by platform"),
    )

from singer_sdk import typing as th

from tap_tiktok.clients import TikTokAudienceReportStream


class AdAudienceReportStream(TikTokAudienceReportStream):
    data_level = "AUCTION_AD"
    status_field = "ad_status"


class AdAgeGenderReportStream(AdAudienceReportStream):
    name = "ad_age_gender_report"
    dimensions = ["ad_id", "age", "gender", "stat_time_day"]
    report_specific_properties = th.PropertiesList(
        th.Property("ad_id", th.StringType, description="Group by ad id"),
        th.Property("age", th.StringType, description="Group by age"),
        th.Property("gender", th.StringType, description="Group by gender"),
    )


class AdCountryReportStream(AdAudienceReportStream):
    name = "ad_country_report"
    dimensions = ["ad_id", "country_code", "stat_time_day"]
    report_specific_properties = th.PropertiesList(
        th.Property("ad_id", th.StringType, description="Group by ad id"),
        th.Property("country_code", th.StringType, description="Group by location code"),
    )


class AdLanguageReportStream(AdAudienceReportStream):
    name = "ad_language_report"
    dimensions = ["ad_id", "language", "stat_time_day"]
    report_specific_properties = th.PropertiesList(
        th.Property("ad_id", th.StringType, description="Group by ad id"),
        th.Property("language", th.StringType, description="Group by location"),
    )


class AdPlatformReportStream(AdAudienceReportStream):
    name = "ad_platform_report"
    dimensions = ["ad_id", "platform", "stat_time_day"]
    report_specific_properties = th.PropertiesList(
        th.Property("ad_id", th.StringType, description="Group by ad id"),
        th.Property("platform", th.StringType, description="Group by platform"),
    )

from singer_sdk import typing as th

from tap_tiktok.clients import TikTokAudienceReportStream


class AdsAudienceReportStream(TikTokAudienceReportStream):
    data_level = "AUCTION_AD"
    status_field = "ad_status"
    buying_types = ["AUCTION", "RESERVATION_TOP_VIEW", "RESERVATION_RF"]


class AdsAgeGenderReportStream(AdsAudienceReportStream):
    name = "ads_age_gender_report"
    dimensions = ["ad_id", "age", "gender", "stat_time_day"]
    report_specific_properties = th.PropertiesList(
        th.Property("ad_id", th.StringType, description="Group by ad id"),
        th.Property("age", th.StringType, description="Group by age"),
        th.Property("gender", th.StringType, description="Group by gender"),
    )


class AdsCountryReportStream(AdsAudienceReportStream):
    name = "ads_country_report"
    dimensions = ["ad_id", "country_code", "stat_time_day"]
    report_specific_properties = th.PropertiesList(
        th.Property("ad_id", th.StringType, description="Group by ad id"),
        th.Property("country_code", th.StringType, description="Group by location code"),
    )


class AdsLanguageReportStream(AdsAudienceReportStream):
    name = "ads_language_report"
    dimensions = ["ad_id", "language", "stat_time_day"]
    report_specific_properties = th.PropertiesList(
        th.Property("ad_id", th.StringType, description="Group by ad id"),
        th.Property("language", th.StringType, description="Group by location"),
    )


class AdsPlatformReportStream(AdsAudienceReportStream):
    name = "ads_platform_report"
    dimensions = ["ad_id", "platform", "stat_time_day"]
    report_specific_properties = th.PropertiesList(
        th.Property("ad_id", th.StringType, description="Group by ad id"),
        th.Property("platform", th.StringType, description="Group by platform"),
    )

"""Stream type classes for tap-tiktok."""

import copy
import datetime
import json
from typing import Any, Dict, Iterable, Optional
from urllib.parse import parse_qs, urlparse

import pendulum
import requests
from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.helpers.jsonpath import extract_jsonpath

from tap_tiktok.clients import TikTokDailyBasicReportStream, TikTokStream


class AdAccountsStream(TikTokStream):
    name = "ad_accounts"
    path = "/advertiser/info/"
    primary_keys = ["advertiser_id"]
    records_jsonpath = "$.data.list[*]"
    replication_key = None
    schema = th.PropertiesList(
        th.Property("advertiser_id", th.StringType),
        th.Property("name", th.StringType),
        th.Property("company", th.StringType),
        th.Property("contacter", th.StringType),
        th.Property("promotion_area", th.StringType),
        th.Property("balance", th.NumberType),
        th.Property("license_province", th.StringType),
        th.Property("currency", th.StringType),
        th.Property("promotion_center_city", th.StringType),
        th.Property("display_timezone", th.StringType),
        th.Property("email", th.StringType),
        th.Property("telephone", th.StringType),
        th.Property("phonenumber", th.StringType),
        th.Property("language", th.StringType),
        th.Property("industry", th.StringType),
        th.Property("create_time", th.IntegerType),
        th.Property("address", th.StringType),
        th.Property("role", th.StringType),
        th.Property("reason", th.StringType),
        th.Property("promotion_center_province", th.StringType),
        th.Property("timezone", th.StringType),
        th.Property("license_url", th.StringType),
        th.Property("country", th.StringType),
        th.Property("status", th.StringType),
        th.Property("brand", th.StringType),
        th.Property("license_city", th.StringType),
        th.Property("description", th.StringType),
        th.Property("license_no", th.StringType),
        th.Property("owner_bc_id", th.StringType),
    ).to_dict()

    def get_url_params(self, context: Optional[dict], next_page_token: Optional[Any]) -> Dict[str, Any]:
        params: dict = {
            "advertiser_ids": "{advertiser_ids}".format(advertiser_ids=json.dumps([str(self.config["advertiser_id"])]))
        }
        if next_page_token:
            params["page"] = next_page_token
        return params

    def get_next_page_token(self, response: requests.Response, previous_token: Optional[Any]) -> Optional[Any]:
        return None


class CampaignsStream(TikTokStream):
    name = "campaigns"
    path = "/campaign/get/"
    primary_keys = ["campaign_id"]
    replication_key = None
    schema = th.PropertiesList(
        th.Property("advertiser_id", th.StringType),
        th.Property("campaign_id", th.StringType),
        th.Property("campaign_system_origin", th.StringType),
        th.Property("create_time", th.StringType),
        th.Property("modify_time", th.StringType),
        th.Property("objective_type", th.StringType),
        th.Property("app_promotion_type", th.StringType),
        th.Property("is_search_campaign", th.BooleanType),
        th.Property("is_smart_performance_campaign", th.BooleanType),
        th.Property("campaign_type", th.StringType),
        th.Property("app_id", th.StringType),
        th.Property("is_advanced_dedicated_campaign", th.BooleanType),
        th.Property("campaign_app_profile_page_state", th.StringType),
        th.Property("rf_campaign_type", th.StringType),
        th.Property("campaign_product_source", th.StringType),
        th.Property("campaign_name", th.StringType),
        th.Property("special_industries", th.ArrayType(th.StringType)),
        th.Property("budget_optimize_on", th.BooleanType),
        th.Property("bid_type", th.StringType),
        th.Property("deep_bid_type", th.StringType),
        th.Property("roas_bid", th.NumberType),
        th.Property("optimization_goal", th.StringType),
        th.Property("budget_mode", th.StringType),
        th.Property("budget", th.NumberType),
        th.Property("rta_id", th.StringType),
        th.Property("rta_product_selection_enabled", th.BooleanType),
        th.Property("operation_status", th.StringType),
        th.Property("secondary_status", th.StringType),
        th.Property("postback_window_mode", th.StringType),
        th.Property("is_new_structure", th.BooleanType),
        th.Property("objective", th.StringType),
    ).to_dict()


class AdGroupsStream(TikTokStream):
    name = "ad_groups"
    path = "/adgroup/get/"
    primary_keys = ["adgroup_id"]
    replication_key = None
    schema = schema = th.PropertiesList(
        th.Property("advertiser_id", th.StringType),
        th.Property("campaign_id", th.StringType),
        th.Property("campaign_name", th.StringType),
        th.Property("campaign_system_origin", th.StringType),
        th.Property("adgroup_id", th.StringType),
        th.Property("adgroup_name", th.StringType),
        th.Property("create_time", th.StringType),
        th.Property("modify_time", th.StringType),
        th.Property("shopping_ads_type", th.StringType),
        th.Property("identity_id", th.StringType),
        th.Property("identity_type", th.StringType),
        th.Property("identity_authorized_bc_id", th.StringType),
        th.Property("product_source", th.StringType),
        th.Property("catalog_id", th.StringType),
        th.Property("catalog_authorized_bc_id", th.StringType),
        th.Property("store_id", th.StringType),
        th.Property("store_authorized_bc_id", th.StringType),
        th.Property("promotion_type", th.StringType),
        th.Property("promotion_target_type", th.StringType),
        th.Property("promotion_website_type", th.StringType),
        th.Property("app_id", th.StringType),
        th.Property("app_type", th.StringType),
        th.Property("app_download_url", th.StringType),
        th.Property("pixel_id", th.StringType),
        th.Property("optimization_event", th.StringType),
        th.Property("placement_type", th.StringType),
        th.Property("placements", th.ArrayType(th.StringType)),
        th.Property("search_result_enabled", th.BooleanType),
        th.Property("comment_disabled", th.BooleanType),
        th.Property("video_download_disabled", th.BooleanType),
        th.Property("share_disabled", th.BooleanType),
        th.Property("blocked_pangle_app_ids", th.ArrayType(th.StringType)),
        th.Property("audience_type", th.StringType),
        th.Property("audience_rule", th.ObjectType()),
        th.Property("auto_targeting_enabledTo be deprecated", th.BooleanType),
        th.Property("shopping_ads_retargeting_type", th.StringType),
        th.Property("shopping_ads_retargeting_actions_days", th.NumberType),
        th.Property(
            "included_custom_actions",
            th.ArrayType(
                th.ObjectType(
                    th.Property("code", th.StringType),
                    th.Property("days", th.IntegerType),
                )
            ),
        ),
        th.Property(
            "excluded_custom_actions",
            th.ArrayType(
                th.ObjectType(
                    th.Property("code", th.StringType),
                    th.Property("days", th.IntegerType),
                )
            ),
        ),
        th.Property("shopping_ads_retargeting_custom_audience_relation", th.StringType),
        th.Property("location_ids", th.ArrayType(th.StringType)),
        th.Property("zipcode_ids", th.ArrayType(th.StringType)),
        th.Property("languages", th.ArrayType(th.StringType)),
        th.Property("gender", th.StringType),
        th.Property("age_groups", th.ArrayType(th.StringType)),
        th.Property("spending_power", th.StringType),
        th.Property("household_income", th.ArrayType(th.StringType)),
        th.Property("audience_ids", th.ArrayType(th.StringType)),
        th.Property("smart_audience_enabled", th.BooleanType),
        th.Property("excluded_audience_ids", th.ArrayType(th.StringType)),
        th.Property("interest_category_ids", th.ArrayType(th.StringType)),
        th.Property("interest_keyword_ids", th.ArrayType(th.StringType)),
        th.Property("purchase_intention_keyword_ids", th.ArrayType(th.StringType)),
        th.Property(
            "actions",
            th.ArrayType(
                th.ObjectType(
                    th.Property("action_scene", th.StringType),
                    th.Property("action_period", th.NumberType),
                    th.Property("video_user_actions", th.ArrayType(th.StringType)),
                    th.Property("action_category_ids", th.ArrayType(th.StringType)),
                )
            ),
        ),
        th.Property("smart_interest_behavior_enabled", th.BooleanType),
        th.Property("included_pangle_audience_package_ids", th.ArrayType(th.StringType)),
        th.Property("excluded_pangle_audience_package_ids", th.ArrayType(th.StringType)),
        th.Property("operating_systems", th.ArrayType(th.StringType)),
        th.Property("min_android_version", th.StringType),
        th.Property("ios14_targeting", th.StringType),
        th.Property("min_ios_version", th.StringType),
        th.Property("ios14_quota_type", th.StringType),
        th.Property("device_model_ids", th.ArrayType(th.StringType)),
        th.Property("network_types", th.ArrayType(th.StringType)),
        th.Property("carrier_ids", th.ArrayType(th.StringType)),
        th.Property("isp_ids", th.ArrayType(th.StringType)),
        th.Property("device_price_ranges", th.ArrayType(th.NumberType)),
        th.Property("saved_audience_id", th.StringType),
        th.Property("contextual_tag_ids", th.ArrayType(th.StringType)),
        th.Property("brand_safety_type", th.StringType),
        th.Property("brand_safety_partner", th.StringType),
        th.Property("inventory_filter_enabled", th.BooleanType),
        th.Property("category_exclusion_ids", th.ArrayType(th.StringType)),
        th.Property("vertical_sensitivity_id", th.StringType),
        th.Property("budget_mode", th.StringType),
        th.Property("budget", th.NumberType),
        th.Property("scheduled_budget", th.NumberType),
        th.Property("schedule_type", th.StringType),
        th.Property("schedule_start_time", th.DateTimeType),
        th.Property("schedule_end_time", th.DateTimeType),
        th.Property("predict_impression", th.NumberType),
        th.Property("topview_reach_range", th.ArrayType(th.NumberType)),
        th.Property("pre_discount_cpm", th.NumberType),
        th.Property("cpm", th.NumberType),
        th.Property("discount_type", th.StringType),
        th.Property("discount_amount", th.NumberType),
        th.Property("discount_percentage", th.NumberType),
        th.Property("pre_discount_budget", th.NumberType),
        th.Property(
            "schedule_infos",
            th.ArrayType(
                th.ObjectType(
                    th.Property(
                        "schedules",
                        th.ArrayType(
                            th.ObjectType(
                                th.Property("start_time", th.StringType),
                                th.Property("end_time", th.StringType),
                            )
                        ),
                    ),
                    th.Property("expected_orders", th.ArrayType(th.NumberType)),
                    th.Property("is_draft", th.BooleanType),
                    th.Property("schedule_id", th.StringType),
                )
            ),
        ),
        th.Property("delivery_mode", th.StringType),
        th.Property("dayparting", th.StringType),
        th.Property("optimization_goal", th.StringType),
        th.Property("frequency", th.NumberType),
        th.Property("frequency_schedule", th.NumberType),
        th.Property("secondary_optimization_event", th.StringType),
        th.Property("bid_type", th.StringType),
        th.Property("bid_price", th.NumberType),
        th.Property("conversion_bid_price", th.NumberType),
        th.Property("deep_bid_type", th.StringType),
        th.Property("roas_bid", th.NumberType),
        th.Property("vbo_window", th.StringType),
        th.Property("bid_display_mode", th.StringType),
        th.Property("deep_cpa_bid", th.NumberType),
        th.Property("cpv_video_duration", th.StringType),
        th.Property("next_day_retention", th.NumberType),
        th.Property("click_attribution_window", th.StringType),
        th.Property("engaged_view_attribution_window", th.StringType),
        th.Property("view_attribution_window", th.StringType),
        th.Property("attribution_event_count", th.StringType),
        th.Property("billing_event", th.StringType),
        th.Property("pacing", th.StringType),
        th.Property("operation_status", th.StringType),
        th.Property("secondary_status", th.StringType),
        th.Property("statistic_type", th.StringType),
        th.Property("is_hfss", th.BooleanType),
        th.Property("creative_material_mode", th.StringType),
        th.Property("adgroup_app_profile_page_state", th.StringType),
        th.Property("is_smart_performance_campaign", th.BooleanType),
        th.Property("feed_type", th.StringType),
        th.Property("rf_purchased_type", th.StringType),
        th.Property("purchased_impression", th.NumberType),
        th.Property("purchased_reach", th.NumberType),
        th.Property("rf_estimated_cpr", th.NumberType),
        th.Property("rf_estimated_frequency", th.NumberType),
        th.Property("split_test_group_id", th.StringType),
        th.Property("split_test_status", th.StringType),
        th.Property("is_new_structure", th.BooleanType),
        th.Property("skip_learning_phase", th.BooleanType),
    ).to_dict()


class AdsStream(TikTokStream):
    name = "ads"
    path = "/ad/get/"
    primary_keys = ["ad_id"]
    replication_key = None
    schema = schema = th.PropertiesList(
        th.Property("advertiser_id", th.StringType),
        th.Property("campaign_id", th.StringType),
        th.Property("campaign_name", th.StringType),
        th.Property("campaign_system_origin", th.StringType),
        th.Property("adgroup_id", th.StringType),
        th.Property("adgroup_name", th.StringType),
        th.Property("ad_id", th.StringType),
        th.Property("ad_name", th.StringType),
        th.Property("create_time", th.StringType),
        th.Property("modify_time", th.StringType),
        th.Property("identity_id", th.StringType),
        th.Property("identity_type", th.StringType),
        th.Property("identity_authorized_bc_id", th.StringType),
        th.Property("catalog_id", th.StringType),
        th.Property("product_specific_type", th.StringType),
        th.Property("item_group_ids", th.ArrayType(th.StringType)),
        th.Property("product_set_id", th.StringType),
        th.Property("sku_ids", th.ArrayType(th.StringType)),
        th.Property(
            "showcase_products",
            th.ArrayType(
                th.ObjectType(
                    th.Property("item_group_id", th.StringType),
                    th.Property("store_id", th.StringType),
                    th.Property("catalog_id", th.StringType),
                )
            ),
        ),
        th.Property("ad_format", th.StringType),
        th.Property("vertical_video_strategy", th.StringType),
        th.Property("dynamic_format", th.StringType),
        th.Property("video_id", th.StringType),
        th.Property("image_ids", th.ArrayType(th.StringType)),
        th.Property("carousel_image_index", th.IntegerType),
        th.Property("music_id", th.StringType),
        th.Property("tiktok_item_id", th.StringType),
        th.Property("promotional_music_disabled", th.BooleanType),
        th.Property("item_duet_status", th.StringType),
        th.Property("item_stitch_status", th.StringType),
        th.Property("dark_post_status", th.StringType),
        th.Property("branded_content_disabled", th.BooleanType),
        th.Property("shopping_ads_video_package_id", th.StringType),
        th.Property("ad_text", th.StringType),
        th.Property("ad_texts", th.StringType),
        th.Property("call_to_action", th.StringType),
        th.Property("call_to_action_id", th.StringType),
        th.Property("card_id", th.StringType),
        th.Property("landing_page_url", th.StringType),
        th.Property(
            "utm_params",
            th.ArrayType(
                th.ObjectType(
                    th.Property("key", th.StringType),
                    th.Property("value", th.StringType),
                )
            ),
        ),
        th.Property("page_id", th.NumberType),
        th.Property("cpp_url", th.StringType),
        th.Property("tiktok_page_category", th.StringType),
        th.Property("phone_region_code", th.StringType),
        th.Property("phone_region_calling_code", th.StringType),
        th.Property("phone_number", th.StringType),
        th.Property("deeplink", th.StringType),
        th.Property("deeplink_type", th.StringType),
        th.Property("deeplink_format_type", th.StringType),
        th.Property("shopping_ads_deeplink_type", th.StringType),
        th.Property(
            "deeplink_utm_params",
            th.ArrayType(
                th.ObjectType(
                    th.Property("key", th.StringType),
                    th.Property("value", th.StringType),
                )
            ),
        ),
        th.Property("shopping_ads_fallback_type", th.StringType),
        th.Property("fallback_type", th.StringType),
        th.Property("dynamic_destination", th.StringType),
        th.Property("aigc_disclosure_type", th.StringType),
        th.Property("disclaimer_type", th.StringType),
        th.Property("disclaimer_text", th.ObjectType(th.Property("text", th.StringType))),
        th.Property(
            "disclaimer_clickable_texts",
            th.ArrayType(
                th.ObjectType(
                    th.Property("text", th.StringType),
                    th.Property("url", th.StringType),
                )
            ),
        ),
        th.Property("tracking_pixel_id", th.NumberType),
        th.Property("tracking_app_id", th.StringType),
        th.Property("tracking_offline_event_set_ids", th.ArrayType(th.StringType)),
        th.Property("vast_moat_enabled", th.BooleanType),
        th.Property("viewability_postbid_partner", th.StringType),
        th.Property("viewability_vast_url", th.StringType),
        th.Property("brand_safety_postbid_partner", th.StringType),
        th.Property("brand_safety_vast_url", th.StringType),
        th.Property("impression_tracking_url", th.StringType),
        th.Property("click_tracking_url", th.StringType),
        th.Property("playable_url", th.StringType),
        th.Property("operation_status", th.StringType),
        th.Property("secondary_status", th.StringType),
        th.Property("creative_type", th.StringType),
        th.Property("app_name", th.StringType),
        th.Property("display_name", th.StringType),
        th.Property("avatar_icon_web_uri", th.StringType),
        th.Property("profile_image_url", th.StringType),
        th.Property("creative_authorized", th.BooleanType),
        th.Property("is_aco", th.BooleanType),
        th.Property("is_new_structure", th.BooleanType),
        th.Property("optimization_event", th.StringType),
    ).to_dict()


DATE_FORMAT = "%Y-%m-%d"
STEP_NUM_DAYS = 30


class AdsMetricsByDayStream(TikTokDailyBasicReportStream):
    tiktok_metrics = []
    data_level = "AUCTION_AD"
    report_type = "BASIC"
    dimensions = ["ad_id", "stat_time_day"]

    status_field = "ad_status"

    def get_url_params(self, context: Optional[dict], next_page_token: Optional[Any]) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        if isinstance(next_page_token, dict) and next_page_token["start_date"] is not None:
            start_date = datetime.datetime.strptime(next_page_token["start_date"], DATE_FORMAT)
        else:
            start_date = self.get_starting_timestamp(context) or pendulum.parse(self.config["start_date"])

            # picking up where we left off on the last run (or first run), adjust for lookback if set
            lookback_window = self.config["lookback"]
            if lookback_window > 0:
                # if lookback is configured, we want to refetch data for the entire lookback window
                # (or as far back as the configured start date, whichever is the most recent date)
                start_date = max(
                    min(
                        start_date,
                        datetime.datetime.now(tz=start_date.tzinfo) - datetime.timedelta(days=lookback_window),
                    ),
                    pendulum.parse(self.config["start_date"]),
                )

        yesterday = datetime.datetime.now(tz=start_date.tzinfo) - datetime.timedelta(days=1)
        end_date = min(start_date + datetime.timedelta(days=STEP_NUM_DAYS), yesterday)
        self.logger.warning(
            f"Preparing request for {start_date.strftime(DATE_FORMAT)} - {end_date.strftime(DATE_FORMAT)}."
        )
        params: dict = {
            "page_size": 1000,
            "advertiser_id": self.config.get("advertiser_id"),
            "service_type": "AUCTION",
            "report_type": self.report_type,
            "data_level": self.data_level,
            "dimensions": json.dumps(self.dimensions),
            "metrics": json.dumps(self.tiktok_metrics),
            "start_date": start_date.strftime(DATE_FORMAT),
            "end_date": end_date.strftime(DATE_FORMAT),
            "filtering": json.dumps(
                [
                    {
                        "field_name": self.status_field,
                        "filter_type": "IN",
                        "filter_value": json.dumps(
                            ["STATUS_ALL" if self.config.get("include_deleted") else "STATUS_NOT_DELETE"]
                        ),
                    }
                ]
            ),
        }
        if next_page_token:
            params["page"] = next_page_token["page"]

        return params

    @staticmethod
    def _get_page_info(json_path, json):
        page_matches = extract_jsonpath(json_path, json)
        return next(iter(page_matches), None)

    def get_next_page_token(self, response: requests.Response, previous_token: Optional[Any]) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        current_page = self._get_page_info("$.data.page_info.page", response.json()) or 0
        total_pages = self._get_page_info("$.data.page_info.total_page", response.json()) or 0
        start_date = datetime.datetime.strptime(
            parse_qs(urlparse(response.request.url).query)["start_date"][0], DATE_FORMAT
        )
        yesterday = datetime.datetime.now(tz=start_date.tzinfo) - datetime.timedelta(days=1)
        end_date = datetime.datetime.strptime(
            parse_qs(urlparse(response.request.url).query)["end_date"][0], DATE_FORMAT
        )
        if current_page < total_pages:
            return {"page": current_page + 1, "start_date": previous_token["start_date"] if previous_token else None}
        elif end_date.date() < yesterday.date():
            return {
                "page": 1,
                "start_date": min(end_date + datetime.timedelta(days=1), yesterday).strftime(DATE_FORMAT),
            }
        return None


class CampaignMetricsByDayStream(AdsMetricsByDayStream):
    data_level = "AUCTION_CAMPAIGN"
    dimensions = ["campaign_id", "stat_time_day"]
    status_field = "campaign_status"


AD_ATTRIBUTE_METRICS = [
    "campaign_name",
    "objective_type",
    "campaign_id",
    "adgroup_name",
    "placement_type",
    "adgroup_id",
    "ad_name",
    "ad_text",
    "tt_app_id",
    "tt_app_name",
    "mobile_app_id",
    "promotion_type",
    "dpa_target_audience_type",
]


class AdsAttributeMetricsStream(AdsMetricsByDayStream):
    name = "ads_attribute_metrics"
    tiktok_metrics = AD_ATTRIBUTE_METRICS
    path = "/"
    primary_keys = ["ad_id"]
    replication_key = None
    properties = [
        th.Property("ad_id", th.StringType),
    ]
    properties += [
        th.Property(metric, th.StringType if metric in ["campaign_id", "adgroup_id"] else th.StringType)
        for metric in AD_ATTRIBUTE_METRICS
    ]
    schema = th.PropertiesList(*properties).to_dict()

    def get_url_params(self, context: Optional[dict], next_page_token: Optional[Any]) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {
            "page_size": 1000,
            "advertiser_id": self.config.get("advertiser_id"),
            "service_type": "AUCTION",
            "report_type": "BASIC",
            "data_level": "AUCTION_AD",
            "dimensions": json.dumps(["ad_id"]),
            "metrics": json.dumps(self.tiktok_metrics),
            "query_lifetime": True,
        }
        if next_page_token:
            params["page"] = next_page_token["page"]
        return params

    def get_next_page_token(self, response: requests.Response, previous_token: Optional[Any]) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        current_page = self._get_page_info("$.data.page_info.page", response.json()) or 0
        total_pages = self._get_page_info("$.data.page_info.total_page", response.json()) or 0
        if current_page < total_pages:
            return {"page": current_page + 1}
        return None


BASIC_DATA_METRICS = [
    "spend",
    # "cash_spend", "voucher_spend", # unsupported
    "cpc",
    "cpm",
    "impressions",
    "clicks",
    "ctr",
    "reach",
    "cost_per_1000_reached",
    "conversion",
    "cost_per_conversion",
    "conversion_rate",
    "real_time_conversion",
    "real_time_cost_per_conversion",
    "real_time_conversion_rate",
    "result",
    "cost_per_result",
    "result_rate",
    "real_time_result",
    "real_time_cost_per_result",
    "real_time_result_rate",
    "secondary_goal_result",
    "cost_per_secondary_goal_result",
    "secondary_goal_result_rate",
    "frequency",
]


class AdsBasicDataMetricsByDayStream(AdsMetricsByDayStream):
    name = "ads_basic_data_metrics_by_day"
    tiktok_metrics = BASIC_DATA_METRICS
    path = "/"
    primary_keys = ["ad_id", "stat_time_day"]
    replication_key = "stat_time_day"
    properties = [
        th.Property("ad_id", th.StringType),
        th.Property("stat_time_day", th.DateTimeType),
    ]
    properties += [th.Property(metric, th.StringType) for metric in BASIC_DATA_METRICS]
    schema = th.PropertiesList(*properties).to_dict()


class CampaignsBasicDataMetricsByDayStream(CampaignMetricsByDayStream):
    name = "campaigns_basic_data_metrics_by_day"
    status_field = "campaign_status"
    tiktok_metrics = BASIC_DATA_METRICS
    path = "/"
    primary_keys = ["campaign_id", "stat_time_day"]
    replication_key = "stat_time_day"
    properties = [
        th.Property("campaign_id", th.StringType),
        th.Property("stat_time_day", th.DateTimeType),
    ]
    properties += [th.Property(metric, th.StringType) for metric in BASIC_DATA_METRICS]
    schema = th.PropertiesList(*properties).to_dict()


VIDEO_PLAY_METRICS = [
    "video_play_actions",
    "video_watched_2s",
    "video_watched_6s",
    "average_video_play",
    "average_video_play_per_user",
    "video_views_p25",
    "video_views_p50",
    "video_views_p75",
    "video_views_p100",
]


class AdsVideoPlayMetricsByDayStream(AdsMetricsByDayStream):
    name = "ads_video_play_metrics_by_day"
    tiktok_metrics = VIDEO_PLAY_METRICS
    path = "/"
    primary_keys = ["ad_id", "stat_time_day"]
    replication_key = "stat_time_day"
    properties = [
        th.Property("ad_id", th.StringType),
        th.Property("stat_time_day", th.DateTimeType),
    ]
    properties += [th.Property(metric, th.StringType) for metric in VIDEO_PLAY_METRICS]
    schema = th.PropertiesList(*properties).to_dict()


class CampaignsVideoPlayMetricsByDayStream(CampaignMetricsByDayStream):
    name = "campaigns_video_play_metrics_by_day"
    status_field = "campaign_status"
    tiktok_metrics = VIDEO_PLAY_METRICS
    path = "/"
    primary_keys = ["campaign_id", "stat_time_day"]
    replication_key = "stat_time_day"
    properties = [
        th.Property("campaign_id", th.StringType),
        th.Property("stat_time_day", th.DateTimeType),
    ]
    properties += [th.Property(metric, th.StringType) for metric in VIDEO_PLAY_METRICS]
    schema = th.PropertiesList(*properties).to_dict()


ENGAGEMENT_METRICS = [
    "profile_visits",
    "profile_visits_rate",
    "likes",
    "comments",
    "shares",
    "follows",
    "clicks_on_music_disc",
]


class AdsEngagementMetricsByDayStream(AdsMetricsByDayStream):
    name = "ads_engagement_metrics_by_day"
    tiktok_metrics = ENGAGEMENT_METRICS
    path = "/"
    primary_keys = ["ad_id", "stat_time_day"]
    replication_key = "stat_time_day"
    properties = [
        th.Property("ad_id", th.StringType),
        th.Property("stat_time_day", th.DateTimeType),
    ]
    properties += [th.Property(metric, th.StringType) for metric in ENGAGEMENT_METRICS]
    schema = th.PropertiesList(*properties).to_dict()


class CampaignsEngagementMetricsByDayStream(CampaignMetricsByDayStream):
    name = "campaigns_engagement_metrics_by_day"
    status_field = "campaign_status"
    tiktok_metrics = ENGAGEMENT_METRICS
    path = "/"
    primary_keys = ["campaign_id", "stat_time_day"]
    replication_key = "stat_time_day"
    properties = [
        th.Property("campaign_id", th.StringType),
        th.Property("stat_time_day", th.DateTimeType),
    ]
    properties += [th.Property(metric, th.StringType) for metric in ENGAGEMENT_METRICS]
    schema = th.PropertiesList(*properties).to_dict()


ATTRIBUTION_METRICS = [
    "vta_app_install",
    "vta_conversion",
    "cost_per_vta_conversion",
    "vta_registration",
    "cost_per_vta_registration",
    "vta_purchase",
    "cost_per_vta_purchase",
    "cta_app_install",
    "cta_conversion",
    "cost_per_cta_conversion",
    "cta_registration",
    "cost_per_cta_registration",
    "cta_purchase",
    "cost_per_cta_purchase",
]


class AdsAttributionMetricsByDayStream(AdsMetricsByDayStream):
    name = "ads_attribution_metrics_by_day"
    tiktok_metrics = ATTRIBUTION_METRICS
    path = "/"
    primary_keys = ["ad_id", "stat_time_day"]
    replication_key = "stat_time_day"
    properties = [
        th.Property("ad_id", th.StringType),
        th.Property("stat_time_day", th.DateTimeType),
    ]
    properties += [th.Property(metric, th.StringType) for metric in ATTRIBUTION_METRICS]
    schema = th.PropertiesList(*properties).to_dict()


class CampaignsAttributionMetricsByDayStream(CampaignMetricsByDayStream):
    name = "campaigns_attribution_metrics_by_day"
    status_field = "campaign_status"
    tiktok_metrics = ATTRIBUTION_METRICS
    path = "/"
    primary_keys = ["campaign_id", "stat_time_day"]
    replication_key = "stat_time_day"
    properties = [
        th.Property("campaign_id", th.StringType),
        th.Property("stat_time_day", th.DateTimeType),
    ]
    properties += [th.Property(metric, th.StringType) for metric in ATTRIBUTION_METRICS]
    schema = th.PropertiesList(*properties).to_dict()


PAGE_EVENT_METRICS = [
    "complete_payment_roas",  # Complete Payment ROAS
    "complete_payment",
    "cost_per_complete_payment",
    "complete_payment_rate",
    "value_per_complete_payment",
    "total_complete_payment_rate",  # Complete Payment
    "page_browse_view",
    "cost_per_page_browse_view",
    "page_browse_view_rate",
    "total_page_browse_view_value",
    "value_per_page_browse_view",  # Page View
    "button_click",
    "cost_per_button_click",
    "button_click_rate",
    "value_per_button_click",
    "total_button_click_value",  # Click Button
    "online_consult",
    "cost_per_online_consult",
    "online_consult_rate",
    "value_per_online_consult",
    "total_online_consult_value",  # Contact
    "user_registration",
    "cost_per_user_registration",
    "user_registration_rate",
    "value_per_user_registration",
    "total_user_registration_value",  # Complete Registration
    "product_details_page_browse",
    "cost_per_product_details_page_browse",
    "product_details_page_browse_rate",
    "value_per_product_details_page_browse",
    "total_product_details_page_browse_value",  # View Content
    "web_event_add_to_cart",
    "cost_per_web_event_add_to_cart",
    "web_event_add_to_cart_rate",
    "value_per_web_event_add_to_cart",
    "total_web_event_add_to_cart_value",  # Add to Cart
    "on_web_order",
    "cost_per_on_web_order",
    "on_web_order_rate",
    "value_per_on_web_order",
    "total_on_web_order_value",  # Place an Order
    "initiate_checkout",
    "cost_per_initiate_checkout",
    "initiate_checkout_rate",
    "value_per_initiate_checkout",
    "total_initiate_checkout_value",  # Initiate Checkout
    "add_billing",
    "cost_per_add_billing",
    "add_billing_rate",
    "value_per_add_billing",
    "total_add_billing_value",  # Add Payment Info
    "page_event_search",
    "cost_per_page_event_search",
    "page_event_search_rate",
    "value_per_page_event_search",
    "total_page_event_search_value",  # Search
    "form",
    "cost_per_form",
    "form_rate",
    "value_per_form",
    "total_form_value",  # Submit Form
    "download_start",
    "cost_per_download_start",
    "download_start_rate",
    "value_per_download_start",
    "total_download_start_value",  # Download
    "on_web_add_to_wishlist",
    "cost_per_on_web_add_to_wishlist",
    "on_web_add_to_wishlist_per_click",
    "value_per_on_web_add_to_wishlist",
    "total_on_web_add_to_wishlist_value",  # Add to Wishlist
    "on_web_subscribe",
    "cost_per_on_web_subscribe",
    "on_web_subscribe_per_click",
    "value_per_on_web_subscribe",
    "total_on_web_subscribe_value",  # Subscribe
]


class AdsPageEventMetricsByDayStream(AdsMetricsByDayStream):
    name = "ads_page_event_metrics_by_day"
    tiktok_metrics = PAGE_EVENT_METRICS
    path = "/"
    primary_keys = ["ad_id", "stat_time_day"]
    replication_key = "stat_time_day"
    properties = [
        th.Property("ad_id", th.StringType),
        th.Property("stat_time_day", th.DateTimeType),
    ]
    properties += [th.Property(metric, th.StringType) for metric in PAGE_EVENT_METRICS]
    schema = th.PropertiesList(*properties).to_dict()


class CampaignsPageEventMetricsByDayStream(CampaignMetricsByDayStream):
    name = "campaigns_page_event_metrics_by_day"
    status_field = "campaign_status"
    tiktok_metrics = PAGE_EVENT_METRICS
    path = "/"
    primary_keys = ["campaign_id", "stat_time_day"]
    replication_key = "stat_time_day"
    properties = [
        th.Property("campaign_id", th.StringType),
        th.Property("stat_time_day", th.DateTimeType),
    ]
    properties += [th.Property(metric, th.StringType) for metric in PAGE_EVENT_METRICS]
    schema = th.PropertiesList(*properties).to_dict()


IN_APP_EVENT_METRICS = [
    "real_time_app_install",
    "real_time_app_install_cost",  # Real-time App Install
    "app_install",
    "cost_per_app_install",  # App Install
    "registration",
    "cost_per_registration",
    "registration_rate",
    "total_registration",
    "cost_per_total_registration",  # Registration
    "purchase",
    "cost_per_purchase",
    "purchase_rate",
    "total_purchase",
    "cost_per_total_purchase",
    "value_per_total_purchase",
    "total_purchase_value",
    "total_active_pay_roas",  # Purchase
    "app_event_add_to_cart",
    "cost_per_app_event_add_to_cart",
    "app_event_add_to_cart_rate",
    "total_app_event_add_to_cart",
    "cost_per_total_app_event_add_to_cart",
    "value_per_total_app_event_add_to_cart",
    "total_app_event_add_to_cart_value",  # Add to Cart
    "checkout",
    "cost_per_checkout",
    "checkout_rate",
    "total_checkout",
    "cost_per_total_checkout",
    "value_per_checkout",
    "total_checkout_value",  # Checkout
    "view_content",
    "cost_per_view_content",
    "view_content_rate",
    "total_view_content",
    "cost_per_total_view_content",
    "value_per_total_view_content",
    "total_view_content_value",  # View Content
    "next_day_open",
    "cost_per_next_day_open",
    "next_day_open_rate",
    "total_next_day_open",
    "cost_per_total_next_day_open",  # Day 2 Retention
    "add_payment_info",
    "cost_per_add_payment_info",
    "add_payment_info_rate",
    "total_add_payment_info",
    "cost_total_add_payment_info",  # Add Payment Info
    "add_to_wishlist",
    "cost_per_add_to_wishlist",
    "add_to_wishlist_rate",
    "total_add_to_wishlist",
    "cost_per_total_add_to_wishlist",
    "value_per_total_add_to_wishlist",
    "total_add_to_wishlist_value",  # Add to Wishlist
    "launch_app",
    "cost_per_launch_app",
    "launch_app_rate",
    "total_launch_app",
    "cost_per_total_launch_app",  # Launch App
    "complete_tutorial",
    "cost_per_complete_tutorial",
    "complete_tutorial_rate",
    "total_complete_tutorial",
    "cost_per_total_complete_tutorial",
    "value_per_total_complete_tutorial",
    "total_complete_tutorial_value",  # Complete Tutorial
    "create_group",
    "cost_per_create_group",
    "create_group_rate",
    "total_create_group",
    "cost_per_total_create_group",
    "value_per_total_create_group",
    "total_create_group_value",  # Create Group
    "join_group",
    "cost_per_join_group",
    "join_group_rate",
    "total_join_group",
    "cost_per_total_join_group",
    "value_per_total_join_group",
    "total_join_group_value",  # Join Group
    "create_gamerole",
    "cost_per_create_gamerole",
    "create_gamerole_rate",
    "total_create_gamerole",
    "cost_per_total_create_gamerole",
    "value_per_total_create_gamerole",
    "total_create_gamerole_value",  # Create Role
    "spend_credits",
    "cost_per_spend_credits",
    "spend_credits_rate",
    "total_spend_credits",
    "cost_per_total_spend_credits",
    "value_per_total_spend_credits",
    "total_spend_credits_value",  # Spend Credit
    "achieve_level",
    "cost_per_achieve_level",
    "achieve_level_rate",
    "total_achieve_level",
    "cost_per_total_achieve_level",
    "value_per_total_achieve_level",
    "total_achieve_level_value",  # Achieve Level
    "unlock_achievement",
    "cost_per_unlock_achievement",
    "unlock_achievement_rate",
    "total_unlock_achievement",
    "cost_per_total_unlock_achievement",
    "value_per_total_unlock_achievement",
    "total_unlock_achievement_value",  # Unlock Achievement
    "sales_lead",
    "cost_per_sales_lead",
    "sales_lead_rate",
    "total_sales_lead",
    "cost_per_total_sales_lead",
    "value_per_total_sales_lead",
    "total_sales_lead_value",  # Generate Lead
    "in_app_ad_click",
    "cost_per_in_app_ad_click",
    "in_app_ad_click_rate",
    "total_in_app_ad_click",
    "cost_per_total_in_app_ad_click",
    "value_per_total_in_app_ad_click",
    "total_in_app_ad_click_value",  # In-App Ad Click
    "in_app_ad_impr",
    "cost_per_in_app_ad_impr",
    "in_app_ad_impr_rate",
    "total_in_app_ad_impr",
    "cost_per_total_in_app_ad_impr",
    "value_per_total_in_app_ad_impr",
    "total_in_app_ad_impr_value",  # In-App Ad Impression
    "loan_apply",
    "cost_per_loan_apply",
    "loan_apply_rate",
    "total_loan_apply",
    "cost_per_total_loan_apply",  # Loan Apply
    "loan_credit",
    "cost_per_loan_credit",
    "loan_credit_rate",
    "total_loan_credit",
    "cost_per_total_loan_credit",  # Loan Approval
    "loan_disbursement",
    "cost_per_loan_disbursement",
    "loan_disbursement_rate",
    "total_loan_disbursement",
    "cost_per_total_loan_disbursement",  # Loan Disbursal
    "login",
    "cost_per_login",
    "login_rate",
    "total_login",
    "cost_per_total_login",  # Login
    "ratings",
    "cost_per_ratings",
    "ratings_rate",
    "total_ratings",
    "cost_per_total_ratings",
    "value_per_total_ratings",
    "total_ratings_value",  # Rate
    "search",
    "cost_per_search",
    "search_rate",
    "total_search",
    "cost_per_total_search",  # Search
    "start_trial",
    "cost_per_start_trial",
    "start_trial_rate",
    "total_start_trial",
    "cost_per_total_start_trial",  # Start Trial
    "subscribe",
    "cost_per_subscribe",
    "subscribe_rate",
    "total_subscribe",
    "cost_per_total_subscribe",
    "value_per_total_subscribe",
    "total_subscribe_value",  # Subscribe
]


class AdsInAppEventMetricsByDayStream(AdsMetricsByDayStream):
    name = "ads_in_app_event_metrics_by_day"
    path = "/"
    primary_keys = ["ad_id", "stat_time_day"]
    replication_key = "stat_time_day"
    properties = [
        th.Property("ad_id", th.StringType),
        th.Property("stat_time_day", th.DateTimeType),
    ]
    properties += [th.Property(metric, th.StringType) for metric in IN_APP_EVENT_METRICS]
    schema = th.PropertiesList(*properties).to_dict()

    def request_records(self, context: Optional[dict]) -> Iterable[dict]:
        next_page_token: Any = None
        finished = False
        decorated_request = self.request_decorator(self._request)

        while not finished:
            rows = {}
            for i in range(2):
                chunk_size = len(IN_APP_EVENT_METRICS) // 2
                self.tiktok_metrics = IN_APP_EVENT_METRICS[chunk_size * i : (i + 1) * chunk_size]
                prepared_request = self.prepare_request(context, next_page_token=next_page_token)
                resp = decorated_request(prepared_request, context)
                for row in self.parse_response(resp):
                    primary_key = tuple(row["dimensions"][key] for key in self.primary_keys)
                    if rows.get(primary_key) is None:
                        rows[primary_key] = row
                    else:
                        rows[primary_key]["metrics"] = {**row["metrics"], **rows[primary_key]["metrics"]}
            for row in rows.values():
                yield row
            previous_token = copy.deepcopy(next_page_token)
            next_page_token = self.get_next_page_token(response=resp, previous_token=previous_token)
            if next_page_token and next_page_token == previous_token:
                raise RuntimeError(
                    f"Loop detected in pagination. " f"Pagination token {next_page_token} is identical to prior token."
                )
            # Cycle until get_next_page_token() no longer returns a value
            finished = not next_page_token


class CampaignsInAppEventMetricsByDayStream(CampaignMetricsByDayStream):
    name = "campaigns_in_app_event_metrics_by_day"
    status_field = "campaign_status"
    path = "/"
    primary_keys = ["campaign_id", "stat_time_day"]
    replication_key = "stat_time_day"
    properties = [
        th.Property("campaign_id", th.StringType),
        th.Property("stat_time_day", th.DateTimeType),
    ]
    properties += [th.Property(metric, th.StringType) for metric in IN_APP_EVENT_METRICS]
    schema = th.PropertiesList(*properties).to_dict()

    def request_records(self, context: Optional[dict]) -> Iterable[dict]:
        next_page_token: Any = None
        finished = False
        decorated_request = self.request_decorator(self._request)

        while not finished:
            rows = {}
            for i in range(2):
                chunk_size = len(IN_APP_EVENT_METRICS) // 2
                self.tiktok_metrics = IN_APP_EVENT_METRICS[chunk_size * i : (i + 1) * chunk_size]
                prepared_request = self.prepare_request(context, next_page_token=next_page_token)
                resp = decorated_request(prepared_request, context)
                for row in self.parse_response(resp):
                    primary_key = tuple(row["dimensions"][key] for key in self.primary_keys)
                    if rows.get(primary_key) is None:
                        rows[primary_key] = row
                    else:
                        rows[primary_key]["metrics"] = {**row["metrics"], **rows[primary_key]["metrics"]}
            for row in rows.values():
                yield row
            previous_token = copy.deepcopy(next_page_token)
            next_page_token = self.get_next_page_token(response=resp, previous_token=previous_token)
            if next_page_token and next_page_token == previous_token:
                raise RuntimeError(
                    f"Loop detected in pagination. " f"Pagination token {next_page_token} is identical to prior token."
                )
            # Cycle until get_next_page_token() no longer returns a value
            finished = not next_page_token

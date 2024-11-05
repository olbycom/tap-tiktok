import abc
from functools import cached_property

from singer_sdk import typing as th

from tap_tiktok.pagination import DailyReportPaginator, HourlyReportPaginator

from .report import TikTokReportStream

BASE_METRICS_PROPERTIES_LIST = th.PropertiesList(
    th.Property(
        "spend",
        th.StringType,
        description="Sum of your total ad spend.",
    ),
    th.Property(
        "billed_cost",
        th.StringType,
        description="Sum of your total ad spend, excluding ad credit or coupons used. This metric might delay up to 11 hours, with records only available from September 1, 2023.",
    ),
    th.Property(
        "cpc",
        th.StringType,
        description="Average cost of each click to a specified destination.",
    ),
    th.Property(
        "cpm",
        th.StringType,
        description="Average amount you spent per 1,000 impressions.",
    ),
    th.Property(
        "impressions",
        th.StringType,
        description="Number of times your ads were shown.",
    ),
    th.Property(
        "gross_impressions",
        th.StringType,
        description="Number of times your ads were shown, including invalid impressions.",
    ),
    th.Property(
        "clicks",
        th.StringType,
        description="Number of clicks from your ads to a specified destination.",
    ),
    th.Property(
        "ctr",
        th.StringType,
        description="Percentage of impressions that resulted in a destination click out of all impressions.",
    ),
    th.Property(
        "reach",
        th.StringType,
        description="Number of unique users who saw your ads at least once.",
    ),
    th.Property(
        "cost_per_1000_reached",
        th.StringType,
        description="Average cost to reach 1,000 unique users.",
    ),
    th.Property(
        "frequency",
        th.StringType,
        description="The average number of times each user saw your ad over a given time period.",
    ),
    th.Property(
        "conversion",
        th.StringType,
        description="Number of times your ad resulted in the optimization event you selected.",
    ),
    th.Property(
        "cost_per_conversion",
        th.StringType,
        description="Average amount spent on a conversion.",
    ),
    th.Property(
        "conversion_rate_v2",
        th.StringType,
        description="Percentage of results you received out of all impressions on your ads.",
    ),
    th.Property(
        "real_time_conversion",
        th.StringType,
        description="Number of times your ad resulted in the optimization event you selected.",
    ),
    th.Property(
        "real_time_cost_per_conversion",
        th.StringType,
        description="Average amount spent on a conversion.",
    ),
    th.Property(
        "real_time_conversion_rate_v2",
        th.StringType,
        description="Percentage of conversions you received out of all impressions on your ads.",
    ),
    th.Property(
        "result",
        th.StringType,
        description="Number of times your ad resulted in an intended outcome based on your campaign objective and optimization goal.",
    ),
    th.Property(
        "cost_per_result",
        th.StringType,
        description="Average cost per each result from your ads.",
    ),
    th.Property(
        "result_rate",
        th.StringType,
        description="Percentage of results that happened out of all impressions on your ads.",
    ),
    th.Property(
        "real_time_result",
        th.StringType,
        description="Number of times your ad resulted in an intended outcome based on your campaign objective and optimization goal.",
    ),
    th.Property(
        "real_time_cost_per_result",
        th.StringType,
        description="Average cost per each result from your ads.",
    ),
    th.Property(
        "real_time_result_rate",
        th.StringType,
        description="Percentage of results that happened out of all impressions on your ads.",
    ),
    th.Property(
        "secondary_goal_result",
        th.StringType,
        description="Number of times your ad resulted in an intended outcome based on the secondary goal you selected.",
    ),
    th.Property(
        "cost_per_secondary_goal_result",
        th.StringType,
        description="Average cost per each secondary goal result from your ads.",
    ),
    th.Property(
        "secondary_goal_result_rate",
        th.StringType,
        description="Percentage of secondary goal results that happened out of all impressions on your ads.",
    ),
    th.Property(
        "video_play_actions",
        th.StringType,
        description="Number of times your video started to play. For each video impression, plays are counted separately and replays are excluded.",
    ),
    th.Property(
        "video_watched_2s",
        th.StringType,
        description="Number of times your video was played for at least 2 seconds. For each video impression, plays are counted separately and replays are excluded.",
    ),
    th.Property(
        "video_watched_6s",
        th.StringType,
        description="Number of times your video was played for at least 6 seconds. For each video impression, plays are counted separately and replays are excluded.",
    ),
    th.Property(
        "engaged_view",
        th.StringType,
        description="Number of times your video was played for at least 6 seconds, played in full if it is less than 6 seconds, or received at least 1 engagement within the first 6 seconds.",
    ),
    th.Property(
        "engaged_view_15s",
        th.StringType,
        description="Number of times your video was played for at least 15 seconds, played in full if it is less than 15 seconds, or received at least 1 engagement within the first 15 seconds.",
    ),
    th.Property(
        "video_views_p25",
        th.StringType,
        description="Number of times your video was played at least 25% of its length. For each impression, views are counted separately and replays are excluded.",
    ),
    th.Property(
        "video_views_p50",
        th.StringType,
        description="Number of times your video was played at least 50% of its length. For each impression, views are counted separately and replays are excluded.",
    ),
    th.Property(
        "video_views_p75",
        th.StringType,
        description="Number of times your video was played at least 75% of its length. For each impression, views are counted separately and replays are excluded.",
    ),
    th.Property(
        "video_views_p100",
        th.StringType,
        description="Number of times your video was played 100% of its length. For each impression, views are counted separately and replays are excluded.",
    ),
    th.Property(
        "average_video_play",
        th.StringType,
        description="Average time your video was played per single video view, including any time spent replaying the video.",
    ),
    th.Property(
        "average_video_play_per_user",
        th.StringType,
        description="Average time your video was played per user, including any time spent replaying the video.",
    ),
)

SKAN_METRICS_PROPERTIES_LIST = th.PropertiesList(
    th.Property(
        "skan_result",
        th.StringType,
        description="Number of times your ad resulted in an intended outcome based on your campaign objective and optimization goal.",
    ),
    th.Property(
        "skan_cost_per_result",
        th.StringType,
        description="Average cost per each result from your ads.",
    ),
    th.Property(
        "skan_result_rate",
        th.StringType,
        description="Percentage of results that happened out of all impressions on your ads.",
    ),
    th.Property(
        "skan_conversion",
        th.StringType,
        description="Number of times your ad resulted in an intended outcome based on your optimization event.",
    ),
    th.Property(
        "skan_cost_per_conversion",
        th.StringType,
        description="Average amount you spent on a conversion.",
    ),
    th.Property(
        "skan_conversion_rate",
        th.StringType,
        description="Percentage of conversions you received out of all destination clicks on your ads.",
    ),
    th.Property(
        "skan_conversion_rate_v2",
        th.StringType,
        description="Percentage of conversions you received out of all impressions on your ads.",
    ),
    th.Property(
        "skan_click_time_conversion",
        th.StringType,
        description="The number of times your ad achieved an outcome based on the objective and settings you selected. Data may be partial due to SKAdNetwork(SKAN) limitations.",
    ),
)


class TikTokDailyBasicReportStream(TikTokReportStream, metaclass=abc.ABCMeta):

    report_type = "BASIC"

    pagination_class = DailyReportPaginator

    metrics_properties = th.PropertiesList(
        *BASE_METRICS_PROPERTIES_LIST,
        *SKAN_METRICS_PROPERTIES_LIST,
    )

    @cached_property
    def schema(self) -> dict:
        return th.PropertiesList(
            *self.metrics_properties,
            *self.report_specific_properties,
        ).to_dict()


class TikTokHourlyBasicReportStream(TikTokReportStream, metaclass=abc.ABCMeta):

    report_type = "BASIC"

    pagination_class = HourlyReportPaginator

    metrics_properties = th.PropertiesList(
        *BASE_METRICS_PROPERTIES_LIST,
    )

    @cached_property
    def schema(self) -> dict:
        return th.PropertiesList(
            *self.metrics_properties,
            *self.report_specific_properties,
        ).to_dict()

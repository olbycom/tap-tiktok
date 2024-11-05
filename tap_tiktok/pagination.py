import pendulum
from requests import Response
from singer_sdk.pagination import BaseAPIPaginator

PAGE_SIZE = 1000
STEP_NUM_DAYS = 30


class DailyReportPaginator(BaseAPIPaginator):
    def __init__(self, start_date: pendulum.DateTime):
        yesterday = pendulum.now().subtract(days=1)
        end_date = min(start_date.add(days=STEP_NUM_DAYS), yesterday)

        start_value = {
            "page_size": PAGE_SIZE,
            "page": 1,
            "start_date": start_date.to_date_string(),
            "end_date": end_date.to_date_string(),
        }
        super().__init__(start_value)

    def has_more(self, response: Response) -> bool:
        page_info = response.json().get("data", {}).get("page_info", {})
        current_page = page_info.get("page", 0)
        total_pages = page_info.get("total_page", 0)
        if current_page < total_pages:
            return True
        start_date = pendulum.parse(self.current_value["start_date"])
        yesterday = pendulum.now().subtract(days=1)
        end_date = min(start_date.add(days=STEP_NUM_DAYS), yesterday)
        if end_date.date() < yesterday.date():
            return True
        return False

    def get_next(self, response) -> dict[str, str]:
        page_info = response.json().get("data", {}).get("page_info", {})
        current_page = page_info.get("page", 0)
        total_pages = page_info.get("total_page", 0)
        if current_page < total_pages:
            return {
                **self.current_value,
                "page": self.current_value["page"] + 1,
            }
        start_date = pendulum.parse(self.current_value["end_date"]).add(days=1)
        yesterday = pendulum.now().subtract(days=1)
        start_date = min(start_date, yesterday)
        end_date = min(start_date.add(days=STEP_NUM_DAYS), yesterday)
        return {
            "page_size": PAGE_SIZE,
            "page": 1,
            "start_date": start_date.to_date_string(),
            "end_date": end_date.to_date_string(),
        }

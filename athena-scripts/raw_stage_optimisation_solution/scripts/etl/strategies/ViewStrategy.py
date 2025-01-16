from ..util.processing_utilities import extract_element_by_name
from .Strategy import Strategy


class ViewStrategy(Strategy):
    def extract(self):
        raw_database = self.args["raw_database"]
        event_processing_view_criteria_view = extract_element_by_name(self.config_data, "view_name", "event_processing_view_criteria")
        if event_processing_view_criteria_view is None:
            raise ValueError("filter value for event_processing_view_criteria is not found within config rules")
        print(f"config rule: event_processing_view_criteria | view: {event_processing_view_criteria_view}")

        sql_query = f'select * from "{raw_database}"."{event_processing_view_criteria_view}"'

        return self.get_raw_data(sql_query)

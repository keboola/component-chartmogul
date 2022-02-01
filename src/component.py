'''
Keboola ChartMogul Extractor
'''

import logging
import dateparser
import csv
import asyncio
from os import path, mkdir

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

from flatten_json_parser import FlattenJsonParser
import chartmogul
from chartmogul_client.client import ChartMogul_client
from chartmogul_client_v2.client import ChartMogulClient

# configuration variables
KEY_API_TOKEN = '#api_token'
KEY_INCREMENTAL_LOAD = 'incrementalLoad'
# TODO : incrementalLoad should be "incremental" to keep things consistent with other components
# Also use snake_case instead of camelCase

KEY_ENDPOINTS = 'endpoints'
# TODO : I would name it endpoint as it is a single endpoint and not a list of endpoints

# list of mandatory parameters => if some is missing,
# component will fail with readable message on initialization.
REQUIRED_PARAMETERS = [
    KEY_API_TOKEN,
    KEY_INCREMENTAL_LOAD,
    KEY_ENDPOINTS
]
REQUIRED_IMAGE_PARS = []


class Component(ComponentBase):
    """
        Extends base class for general Python components. Initializes the CommonInterface
        and performs configuration validation.

        For easier debugging the data folder is picked up by default from `../data` path,
        relative to working directory.

        If `debug` parameter is present in the `config.json`, the default logger is set to verbose DEBUG mode.
    """

    def __init__(self):
        super().__init__()

    def validate_params(self, params):
        '''
        Validating user input configuration values
        '''

        # 1 - ensure if there are any inputs
        if params == {}:
            raise UserException('Please input configuration.')

        # TODO : I would be more specific in the UserException, "Please fill in an endpoint in the configuration"
        # 2 - ensure at least one endpoint is selected
        endpoints = params.get(KEY_ENDPOINTS)
        if not endpoints:
            raise UserException('Please select an endpoint.')

        # TODO : This should be in a seperate function and just call that :
        """
        def test_connection (api_token):
            config = chartmogul.Config(
                api_token)
            try:
                chartmogul.Ping.ping(config).get()
            except Exception as err:
                raise UserException(f'API Token error: {err}')
        """

        # 3 - test if API token works
        config = chartmogul.Config(
            params[KEY_API_TOKEN], params[KEY_API_TOKEN])
        try:
            chartmogul.Ping.ping(config).get()
        except Exception as err:
            raise UserException(f'API Token error: {err}')

        # Fetching values for additional_params
        if endpoints == 'key_metrics':
            additional_params = params.get('additional_params_key_metrics')
        elif endpoints == 'activities':
            additional_params = params.get('additional_params_activities')
        else:
            additional_params = {}

        start_date = additional_params.get('start-date')
        end_date = additional_params.get('end-date')

        # 4 - endpoint key_metrics requires start-date and end-date
        if endpoints == 'key_metrics':
            if not start_date or not end_date:
                raise UserException(
                    '[Start date] and [End Date] are required.')

            else:
                start_date_form = dateparser.parse(start_date)
                end_date_form = dateparser.parse(end_date)
                day_diff = (end_date_form - start_date_form).days

                if day_diff < 0:
                    raise UserException(
                        '[Start Date] cannot exceed [End Date]')

        # 5 - validate start-date and end-date on activities endpoint
        if endpoints == 'activities':
            # TODO : I would put this in a second function validate activities, so that there isnt a nested IF

            if end_date and not start_date:
                raise UserException(
                    'Please specify [Start Date] when [End Date] is specified.')

            # TODO maybe add a function validate_date with a try except block to see if the user input date is valid
            elif start_date and end_date:
                start_date_form = dateparser.parse(start_date)
                end_date_form = dateparser.parse(end_date)
                day_diff = (end_date_form - start_date_form).days

                if day_diff < 0:
                    raise UserException(
                        '[Start Date] cannot exceed [End Date]')

    def run(self):
        '''
        Main execution code
        '''

        params = self.configuration.parameters

        # Setting up additional params
        # TODO : this will be done in the seperate endpoints
        if params.get(KEY_ENDPOINTS) == 'activities':
            additional_params = params.get('additional_params_activities')
        elif params.get(KEY_ENDPOINTS) == 'key_metrics':
            additional_params = params.get('additional_params_key_metrics')
        else:
            additional_params = {}

        # Validating user inputs
        self.validate_params(params)

        # Previous state
        previous_state = self.get_state_file()
        #
        # # TODO : this will be done in the seperate endpoints
        # # Parse date into the required format
        # if additional_params.get('start-date'):
        #     additional_params['start-date'] = dateparser.parse(
        #         additional_params['start-date']).strftime("%Y-%m-%d")
        # if additional_params.get('end-date'):
        #     additional_params['end-date'] = dateparser.parse(
        #         additional_params['end-date']).strftime("%Y-%m-%d")
        #
        # # Custom ChartMogul client
        # cm_client = ChartMogul_client(
        #     api_token=params.get(KEY_API_TOKEN),
        #     incremental=params.get(KEY_INCREMENTAL_LOAD),
        #     state=previous_state,
        #     destination=self.tables_out_path)
        #
        # # Process endpoint
        # cm_client.fetch(endpoint=params.get(KEY_ENDPOINTS),
        #                 additional_params=additional_params)
        #
        # # Updating state
        # self.write_state_file(cm_client.state)

        # TODO make fetch data, parsing, and saving seperate parts of the component
        cm_client_v2 = ChartMogulClient(api_token=params.get(KEY_API_TOKEN))

        endpoint = params.get(KEY_ENDPOINTS)
        incremental = params.get(KEY_INCREMENTAL_LOAD, False)

        if endpoint == 'activities':
            self.process_activities_endpoint(cm_client_v2, incremental)

        elif endpoint == 'key_metrics':
            self.process_key_metrics_endpoint(cm_client_v2, incremental)

        elif endpoint == 'customers':
            self.process_key_customers_endpoint(cm_client_v2, incremental)

        elif endpoint == 'customer_subscriptions':
            self.process_key_customer_subscriptions_endpoint(cm_client_v2, incremental)

        else:
            raise UserException(f"{endpoint}")

    def process_key_customers_endpoint(self, cm_client, incremental):
        table = self.create_out_table_definition("customer_subscriptions.csv",
                                                 incremental=incremental,
                                                 is_sliced=True,
                                                 primary_key=["id", "uuid"])
        self.create_sliced_directory(table.full_path)
        flatten_parser = FlattenJsonParser()
        for i, customer_data in enumerate(cm_client.get_customers()):
            parsed_data = flatten_parser.parse_data(customer_data)
            self.write_slice(table, i, parsed_data)

        self.write_manifest(table)

    def process_key_customer_subscriptions_endpoint(self, cm_client, incremental):
        table = self.create_out_table_definition("customers.csv",
                                                 incremental=incremental,
                                                 is_sliced=True,
                                                 primary_key=["id", "uuid"])
        self.create_sliced_directory(table.full_path)
        customer_uuids = cm_client.get_customer_uuids()

        loop = asyncio.get_event_loop()
        tasks = []

        for i, customer_uuid in enumerate(customer_uuids):
            tasks.append(self.process_key_customer_subscription(cm_client, customer_uuid, table, i))
        loop.run_until_complete(asyncio.wait(tasks))
        loop.close()
        self.write_manifest(table)

    async def process_key_customer_subscription(self, cm_client, customer_uuid, table, index):
        customer_sub_data = cm_client.get_customer_subscriptions(customer_uuid)
        flatten_parser = FlattenJsonParser()
        parsed_data = flatten_parser.parse_data(customer_sub_data)
        print(index)
        self.write_slice(table, index, parsed_data)

    def process_key_metrics_endpoint(self, cm_client, incremental):
        additional_params = self.get_endpoint_additional_params("additional_params_key_metrics")
        key_metrics = cm_client.get_key_metrics(additional_params)
        table = self.create_out_table_definition("key_metrics.csv",
                                                 incremental=incremental,
                                                 primary_key=["date"])
        self.write_to_table(table, key_metrics)
        self.write_manifest(table)

    def process_activities_endpoint(self, cm_client, incremental):
        additional_params = self.get_endpoint_additional_params("additional_params_activities")
        table = self.create_out_table_definition("activities.csv",
                                                 incremental=incremental,
                                                 is_sliced=True,
                                                 primary_key=["uuid"])
        self.create_sliced_directory(table.full_path)
        flatten_parser = FlattenJsonParser()
        for i, activity_data in enumerate(cm_client.get_activities(additional_params)):
            parsed_data = flatten_parser.parse_data(activity_data)
            self.write_slice(table, i, parsed_data)

        self.write_manifest(table)

    @staticmethod
    def create_sliced_directory(table_path: str) -> None:
        logging.info("Creating sliced file")
        if not path.isdir(table_path):
            mkdir(table_path)

    @staticmethod
    def write_to_table(table, data):
        if not table.columns and data:
            table.columns = list(data[0].keys())
        with open(table.full_path, 'w+', newline='') as out:
            writer = csv.DictWriter(out, fieldnames=table.columns, lineterminator='\n', delimiter=',')
            writer.writerows(data)

    @staticmethod
    def write_slice(table, index, data):
        slice_path = path.join(table.full_path, str(index))
        if not table.columns and data:
            table.columns = list(data[0].keys())
        with open(slice_path, 'w+', newline='') as out:
            writer = csv.DictWriter(out, fieldnames=table.columns, lineterminator='\n', delimiter=',')
            writer.writerows(data)

    def get_endpoint_additional_params(self, key_additional_params):
        additional_params = self.configuration.parameters.get(key_additional_params)
        if additional_params.get('start-date'):
            additional_params['start-date'] = dateparser.parse(
                additional_params['start-date']).strftime("%Y-%m-%d")
        if additional_params.get('end-date'):
            additional_params['end-date'] = dateparser.parse(
                additional_params['end-date']).strftime("%Y-%m-%d")
        return additional_params


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        # this triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)

import asyncio
import dateparser
import json
import logging
import os
import shutil
import time

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException
from keboola.csvwriter import ElasticDictWriter

import chartmogul
from chartmogul_client.client import ChartMogulClient, ChartMogulClientException
from chartmogul_client.mapping import pkeys_mapping

# configuration variables
KEY_API_TOKEN = '#api_token'
KEY_INCREMENTAL_LOAD = 'incrementalLoad'
KEY_ENDPOINTS = 'endpoints'

# list of mandatory parameters => if some is missing,
# component will fail with readable message on initialization.
REQUIRED_PARAMETERS = [
    KEY_API_TOKEN,
    KEY_INCREMENTAL_LOAD,
    KEY_ENDPOINTS
]
REQUIRED_IMAGE_PARS = []


class Component(ComponentBase):

    def __init__(self):
        super().__init__()
        self.state_columns = {}
        self.start_time = time.perf_counter()

    def run(self):
        '''
        Main execution code
        '''

        params = self.configuration.parameters

        # Setting up additional params
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
        self.state_columns = previous_state.get("columns", {})

        # Parse date into the required format
        if additional_params.get('start-date'):
            additional_params['start-date'] = dateparser.parse(
                additional_params['start-date']).strftime("%Y-%m-%d")
        if additional_params.get('end-date'):
            additional_params['end-date'] = dateparser.parse(
                additional_params['end-date']).strftime("%Y-%m-%d")

        temp_path = os.path.join(self.data_folder_path, "temp")

        # Custom ChartMogul client
        cm_client = ChartMogulClient(
            api_token=params.get(KEY_API_TOKEN),
            incremental=params.get(KEY_INCREMENTAL_LOAD),
            state=previous_state,
            destination=temp_path)

        # Process endpoint
        endpoint = params.get(KEY_ENDPOINTS)
        try:
            asyncio.run(cm_client.fetch(endpoint=endpoint, additional_params=additional_params))
        except ChartMogulClientException as e:
            raise UserException(f"Failed to fetch data from endpoint {endpoint}, exception: {e}")

        if os.path.isdir(temp_path):
            for subfolder in os.listdir(temp_path):
                self.process_subfolder(temp_path, subfolder, self.tables_out_path)

        # Updating state
        new_statefile = cm_client.state  # load state related data from current run

        if "columns" not in new_statefile:
            new_statefile["columns"] = {}

        for table in self.state_columns:
            new_statefile["columns"][table] = self.state_columns.get(table)

        self.write_state_file(new_statefile)

        # Clean temp folder (primarily for local runs)
        # shutil.rmtree(temp_path)

        end_time = time.perf_counter()
        runtime = end_time - self.start_time
        logging.info(f"Runtime: {runtime} seconds")

    def process_subfolder(self, temp_path: str, subfolder: str, tables_out_path: str):
        """
        Process a subfolder containing JSON files, write valid rows to an output table, and update state information.

        Args:
            temp_path (str): The path to the temporary directory containing the subfolder.
            subfolder (str): The name of the subfolder to process.
            tables_out_path (str): The path to the directory where output tables will be saved.

        Returns:
            None

        Note:
            If no valid rows are found, the output table file is deleted.
        """
        valid_rows = False
        subfolder_path = os.path.join(temp_path, subfolder)
        if not os.path.isdir(subfolder_path):
            return

        if self.are_files_in_directory(subfolder_path):

            out_table_path = os.path.join(tables_out_path, subfolder)
            fieldnames = self.state_columns.get(subfolder, [])

            with ElasticDictWriter(out_table_path, fieldnames) as wr:
                wr.writeheader()

                for json_file in os.listdir(subfolder_path):
                    json_file_path = os.path.join(subfolder_path, json_file)

                    with open(json_file_path, 'r') as file:
                        content = json.load(file)
                        for row in content:
                            if row:
                                wr.writerow(row)
                                valid_rows = True

            if valid_rows:
                pk = pkeys_mapping.get(subfolder, [])
                table = self.create_out_table_definition(subfolder, is_sliced=True, primary_key=pk)
                self.state_columns[subfolder] = wr.fieldnames
                self.write_manifest(table)
            else:
                if os.path.exists(out_table_path):
                    os.remove(out_table_path)

    @staticmethod
    def are_files_in_directory(path):
        if os.path.exists(path) and os.path.isdir(path):
            files = [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]
            return len(files) > 0
        else:
            return False

    @staticmethod
    def validate_params(params):
        '''
        Validating user input configuration values
        '''

        # 1 - ensure if there are any inputs
        if params == {}:
            raise UserException('Please input configuration.')

        # 2 - ensure at least one endpoint is selected
        endpoints = params.get(KEY_ENDPOINTS)
        if not endpoints:
            raise UserException('Please select an endpoint.')

        # 3 - test if API token works
        config = chartmogul.Config(params[KEY_API_TOKEN])
        try:
            chartmogul.Ping.ping(config).get()
        except Exception as err:
            raise UserException(f'API Token error: {err}')

        # Fetching values for additional_params
        if endpoints == 'key_metrics':
            additional_params = params.get('additional_params_key_metrics')
        elif endpoints == 'activities':
            additional_params = params.get('additional_params_activities', {})
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

            if end_date and not start_date:
                raise UserException(
                    'Please specify [Start Date] when [End Date] is specified.')

            elif start_date and end_date:
                start_date_form = dateparser.parse(start_date)
                end_date_form = dateparser.parse(end_date)
                day_diff = (end_date_form - start_date_form).days

                if day_diff < 0:
                    raise UserException(
                        '[Start Date] cannot exceed [End Date]')


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

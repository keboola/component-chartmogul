import asyncio
import dateparser
import json
import logging
import os
import shutil

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException
from keboola.csvwriter import ElasticDictWriter

import chartmogul

from chartmogul_client.client import ChartMogulClient, ChartMogulClientException

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

    def run(self):
        params = self.configuration.parameters

        # Setting up additional params
        if params.get(KEY_ENDPOINTS) == 'activities':
            additional_params = params.get('additional_params_activities', {})
        elif params.get(KEY_ENDPOINTS) == 'key_metrics':
            additional_params = params.get('additional_params_key_metrics', {})
        else:
            additional_params = {}

        # Validating user inputs
        self.validate_params(params)

        # Previous state
        previous_state = self.get_state_file()
        self.state_columns = previous_state.get("columns", {})

        # Parse date into the required format
        if additional_params.get('start-date'):
            additional_params['start-date'] = dateparser.parse(additional_params['start-date']).strftime("%Y-%m-%d")
        if additional_params.get('end-date'):
            additional_params['end-date'] = dateparser.parse(additional_params['end-date']).strftime("%Y-%m-%d")

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
            result_mapping = asyncio.run(cm_client.fetch(endpoint=endpoint, additional_params=additional_params))
        except ChartMogulClientException as e:
            raise UserException(f"Failed to fetch data from endpoint {endpoint}, exception: {e}")

        if os.path.isdir(temp_path):
            for subfolder in os.listdir(temp_path):
                self.process_subfolder(temp_path, subfolder, self.tables_out_path, result_mapping)

        # Updating state
        new_statefile = cm_client.state  # load state related data from current run

        if "columns" not in new_statefile:
            new_statefile["columns"] = {}

        for table in self.state_columns:
            new_statefile["columns"][table] = self.state_columns.get(table)

        self.write_state_file(new_statefile)

        # Clean temp folder (primarily for local runs)
        shutil.rmtree(temp_path)

    def process_subfolder(self, temp_path: str, subfolder: str, tables_out_path: str, result_mapping: dict):
        """
        Process a subfolder containing JSON files, write valid rows to an output table, and update state information.

        Args:
            temp_path (str): The path to the temporary directory containing the subfolder.
            subfolder (str): The name of the subfolder to process.
            tables_out_path (str): The path to the directory where output tables will be saved.
            result_mapping (dict): TableMapping dict returned by fetch method of ChartMogul client.

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
            # TODO: pick tableMapping from the childtable tree according to table name and set result_mapping

            fieldnames = self.state_columns.get(subfolder, list(result_mapping['column_mappings'].values()))
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
                pk = result_mapping.get("primary_keys", [])
                table = self.create_out_table_definition(subfolder, is_sliced=True, primary_key=pk)
                self.state_columns[subfolder] = wr.fieldnames
                self.write_manifest(table)
            else:
                # do not store empty tables, this leads to output mapping fail
                if os.path.exists(out_table_path):
                    os.remove(out_table_path)

    @staticmethod
    def are_files_in_directory(path):
        if os.path.exists(path) and os.path.isdir(path):
            files = [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]
            return len(files) > 0
        else:
            return False

    def validate_params(self, params):
        """
        Validating user input configuration values
        """
        self.ensure_non_empty_params(params)
        endpoint = self.get_validated_endpoints(params)
        self.validate_api_token(params[KEY_API_TOKEN])
        additional_params = self.get_additional_params(params, endpoint)
        start_date, end_date = self.get_dates(additional_params)

        if endpoint == 'key_metrics':
            self.validate_key_metrics_dates(start_date, end_date)
        elif endpoint == 'activities':
            self.validate_activities_dates(start_date, end_date)

    @staticmethod
    def ensure_non_empty_params(params):
        if not params:
            raise UserException('Please input configuration.')

    @staticmethod
    def get_validated_endpoints(params):
        endpoint = params.get(KEY_ENDPOINTS)
        if not endpoint:
            raise UserException('Please select an endpoint.')
        return endpoint

    @staticmethod
    def validate_api_token(api_token):
        config = chartmogul.Config(api_token)
        try:
            chartmogul.Ping.ping(config).get()
        except Exception as err:
            raise UserException(f'API Token error: {err}')

    @staticmethod
    def get_additional_params(params, endpoints):
        additional_params_key = f'additional_params_{endpoints}'
        return params.get(additional_params_key, {})

    @staticmethod
    def get_dates(additional_params):
        return additional_params.get('start-date'), additional_params.get('end-date')

    def validate_key_metrics_dates(self, start_date, end_date):
        if not start_date or not end_date:
            raise UserException('[Start date] and [End Date] are required.')
        self.validate_date_order(start_date, end_date)

    def validate_activities_dates(self, start_date, end_date):
        if end_date and not start_date:
            raise UserException('Please specify [Start Date] when [End Date] is specified.')
        elif start_date and end_date:
            self.validate_date_order(start_date, end_date)

    @staticmethod
    def validate_date_order(start_date, end_date):
        start_date_form = dateparser.parse(start_date)
        end_date_form = dateparser.parse(end_date)
        day_diff = (end_date_form - start_date_form).days

        if day_diff < 0:
            raise UserException('[Start Date] cannot exceed [End Date]')


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

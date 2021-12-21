'''
Keboola ChartMogul Extractor
'''

import logging
import dateparser

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException

import chartmogul
from chartmogul_client.client import ChartMogul_client

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

        # 2 - ensure at least one endpoint is selected
        endpoints = params.get(KEY_ENDPOINTS)
        if not endpoints:
            raise UserException('Please select an endpoint.')

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
                day_diff = (end_date_form-start_date_form).days

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
                day_diff = (end_date_form-start_date_form).days

                if day_diff < 0:
                    raise UserException(
                        '[Start Date] cannot exceed [End Date]')

    def run(self):
        '''
        Main execution code
        '''

        params = self.configuration.parameters

        # Setting up additiona params
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

        # Parse date into the required format
        if additional_params.get('start-date'):
            additional_params['start-date'] = dateparser.parse(
                additional_params['start-date']).strftime("%Y-%m-%d")
        if additional_params.get('end-date'):
            additional_params['end-date'] = dateparser.parse(
                additional_params['end-date']).strftime("%Y-%m-%d")

        # Custom ChartMogul client
        cm_client = ChartMogul_client(
            api_token=params.get(KEY_API_TOKEN),
            incremental=params.get(KEY_INCREMENTAL_LOAD),
            state=previous_state,
            destination=self.tables_out_path)

        # Process endpoint
        cm_client.fetch(endpoint=params.get(KEY_ENDPOINTS),
                        additional_params=additional_params)

        # Updating state
        self.write_state_file(cm_client.STATE)


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

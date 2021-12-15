'''
Keboola ChartMogul Extractor
'''
import os
import sys
import json
import logging
import requests
from datetime import datetime  # noqa
from urllib.parse import urljoin

from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException
from mapping_parser import MappingParser

import chartmogul

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

CHARTMOGUL_BASEURL = 'https://api.chartmogul.com/v1/'

try:
    # with open('/code/src/mapping.json') as f:
    with open('mapping.json') as f:
        CHARTMOGUL_MAPPING = json.load(f)
except Exception:
    logging.error('Error in loading mapping. Please contact support.')
    sys.exit(1)

CHARTMOGUL_ENDPOINT_CONFIGS = {
    'activities': {
        'endpoint': 'activities',
        'dataType': 'entries',
        'pagination': 'additional_params_activities'
    },
    'customers': {
        'endpoint': 'customers',
        'dataType': 'entries',
        'pagination': 'page'
    },
    'customers_subscriptions': {
        'endpoint': 'customers/{{customers_uuid}}/subscriptions',
        'dataType': 'entries',
        'required': 'customers',
        'pagination': 'page'
    },
    'key_metrics': {
        'endpoint': 'metrics/all',
        'dataType': 'entries',
        'pagination': 'additional_params_key_metrics'
    },
    'invoices': {
        'endpoint': 'invoices',
        'dataType': 'invoices',
        'pagination': 'page'
    }
}

# Storing UUIDs in case of child requests
UUIDS = {}


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
            logging.error('Please input configuration.')
            sys.exit(1)

        # 2 - ensure at least one endpoint is selected
        endpoints = params.get(KEY_ENDPOINTS)
        if not endpoints:
            logging.error('Please select an endpoint.')
            sys.exit(1)

        # 3 - test if API token works
        config = chartmogul.Config(
            params[KEY_API_TOKEN], params[KEY_API_TOKEN])
        try:
            chartmogul.Ping.ping(config).get()
        except Exception as err:
            logging.error(f'API Token error: {err}')
            sys.exit(1)

        # 4 - endpoint key_metrics requires start-date and end-date
        additional_params = params.get('additional_params_key_metrics')
        if endpoints == 'key_metrics':
            if not additional_params.get('start-date') or not additional_params.get('end-date'):
                logging.error('[Start date] and [End Date] are required.')
                sys.exit(1)

    @staticmethod
    def get_request(url, params, token):

        try:
            response = requests.get(url, params=params, auth=(token, ''))
        except Exception as err:
            logging.error(f'Request error against {url} - {err}')
            sys.exit(1)

        return response.json()

    def _fetch_page(self, endpoint, endpoint_url, endpoint_config, endpoint_mapping, parentKey=None):

        pagination_loop = True
        endpoint_params = {
            'page': 1,
            'per_page': 200
        }
        UUIDS[endpoint] = []

        while pagination_loop:

            logging.info(
                f'Extracting [{endpoint}] - Page {endpoint_params["page"]}')

            data_in = self.get_request(
                endpoint_url, endpoint_params, self.API_TOKEN)

            MappingParser(
                destination=os.path.join(self.tables_out_path),
                endpoint=endpoint,
                endpoint_data=data_in[endpoint_config['dataType']],
                mapping=endpoint_mapping,
                incremental=self.incremental,
                parent_key=parentKey
            )

            # Storing all UUIDS incases of child requests
            UUIDS[endpoint] = UUIDS[endpoint] + [i['uuid']
                                                 for i in data_in[endpoint_config['dataType']]]

            if data_in['current_page'] == data_in['total_pages']:
                pagination_loop = False
            else:
                endpoint_params['page'] += 1

        return {}

    def _fetch_activities(self, endpoint, endpoint_url, endpoint_config, endpoint_mapping, previous_state,
                          additional_params):

        pagination_loop = True
        endpoint_params = {'per_page': 200}
        LAST_UUID = ''
        if previous_state and self.incremental:
            endpoint_params['start-after'] = previous_state['start-after']
        else:
            for p in additional_params:
                if additional_params[p]:
                    endpoint_params[p] = additional_params[p]

        while pagination_loop:

            logging.info(f'Extracting [{endpoint}] - {LAST_UUID}')

            data_in = self.get_request(
                endpoint_url, endpoint_params, self.API_TOKEN)

            MappingParser(
                destination=os.path.join(self.tables_out_path),
                endpoint=endpoint,
                endpoint_data=data_in[endpoint_config['dataType']],
                mapping=endpoint_mapping,
                incremental=self.incremental,
            )

            LAST_UUID = data_in[endpoint_config['dataType']][-1]['uuid']
            endpoint_params = {'start-after': LAST_UUID, 'per_page': 200}

            if not data_in['has_more']:
                pagination_loop = False

        return {endpoint: endpoint_params}

    def _fetch_key_metrics(self, endpoint, endpoint_url, endpoint_config, endpoint_mapping, previous_state,
                           additional_params):

        endpoint_params = {}
        for p in additional_params:
            if additional_params[p]:
                endpoint_params[p] = additional_params[p]

        logging.info(f'Extracting[{endpoint}]')

        data_in = self.get_request(
            endpoint_url, endpoint_params, self.API_TOKEN)

        MappingParser(
            destination=os.path.join(self.tables_out_path),
            endpoint=endpoint,
            endpoint_data=data_in[endpoint_config['dataType']],
            mapping=endpoint_mapping,
            incremental=self.incremental,
        )

        return {}

    def fetch(self, endpoint, previous_state=None, additional_params=None):

        endpoint_mapping = CHARTMOGUL_MAPPING[endpoint]
        endpoint_config = CHARTMOGUL_ENDPOINT_CONFIGS[endpoint]
        endpoint_url = urljoin(
            CHARTMOGUL_BASEURL, endpoint_config.get('endpoint'))
        pagination_method = endpoint_config.get('pagination')

        # Child endpoints
        if endpoint_config.get('required'):
            self.fetch(endpoint_config.get('required')) if endpoint_config.get(
                'required') not in UUIDS else ''

            for i in UUIDS[endpoint_config.get('required')]:
                wildcard = '{{'+endpoint_config.get('required')+'_uuid}}'
                endpoint_url_i = endpoint_url.replace(wildcard, i)

                self._fetch_page(endpoint, endpoint_url_i,
                                 endpoint_config, endpoint_mapping, parentKey=i)

        # Parent endpoints with page pagination
        elif pagination_method == 'page':
            state = self._fetch_page(endpoint, endpoint_url,
                                     endpoint_config, endpoint_mapping)

        # Pagination method for activities
        elif pagination_method == 'additional_params_activities':
            state = self._fetch_activities(endpoint, endpoint_url, endpoint_config,
                                           endpoint_mapping, previous_state, additional_params)

        # Pagination method for key metrics
        elif pagination_method == 'additional_params_key_metrics':
            state = self._fetch_key_metrics(endpoint, endpoint_url, endpoint_config,
                                            endpoint_mapping, previous_state, additional_params)

        return state

    def run(self):
        '''
        Main execution code
        '''

        params = self.configuration.parameters
        self.API_TOKEN = params.get(KEY_API_TOKEN)
        self.incremental = params.get(KEY_INCREMENTAL_LOAD)

        if params.get(KEY_ENDPOINTS) == 'activities':
            additional_params = params.get('additional_params_activities')
        elif params.get(KEY_ENDPOINTS) == 'key_metrics':
            additional_params = params.get('additional_params_key_metrics')
        else:
            additional_params = None

        # Validating user inputs
        self.validate_params(params)

        # Previous state
        previous_state = self.get_state_file()

        # Fetch
        state = self.fetch(params.get(KEY_ENDPOINTS),
                           previous_state.get(params.get(KEY_ENDPOINTS)), additional_params)

        # Update state
        self.write_state_file(state)


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

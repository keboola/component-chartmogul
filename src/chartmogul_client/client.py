import requests
import logging
from urllib.parse import urljoin
from mapping_parser.parser import MappingParser

CHARTMOGUL_BASEURL = 'https://api.chartmogul.com/v1/'

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


class ChartMogul_client():
    def __init__(self, destination, api_token, incremental=False, state=None):

        # Storing UUIDs in case of child requests
        self.UUIDS = {}

        # Request paramters
        self.DESTINATION = destination
        self.API_TOKEN = api_token
        self.INCREMENTAL = incremental
        self.STATE = state

    @staticmethod
    def get_request(url, params, token):

        response = requests.get(url, params=params, auth=(token, ''))

        return response.json()

    def fetch(self, endpoint, additional_params=None):

        endpoint_config = CHARTMOGUL_ENDPOINT_CONFIGS[endpoint]
        endpoint_url = urljoin(
            CHARTMOGUL_BASEURL, endpoint_config.get('endpoint'))
        pagination_method = endpoint_config.get('pagination')

        # Child endpoints
        if endpoint_config.get('required'):
            self.fetch(endpoint_config.get('required')) if endpoint_config.get(
                'required') not in self.UUIDS else ''

            for i in self.UUIDS[endpoint_config.get('required')]:

                logging.info(f'{endpoint}: {i}')
                wildcard = '{{'+endpoint_config.get('required')+'_uuid}}'
                endpoint_url_i = endpoint_url.replace(wildcard, i)

                self._fetch_page(endpoint, endpoint_url_i,
                                 endpoint_config, parentKey=i)

        # Parent endpoints with page pagination
        elif pagination_method == 'page':
            self._fetch_page(endpoint, endpoint_url,
                             endpoint_config)

        # Pagination method for activities
        elif pagination_method == 'additional_params_activities':
            self._fetch_activities(
                endpoint, endpoint_url, endpoint_config, additional_params)

        # Pagination method for key metrics
        elif pagination_method == 'additional_params_key_metrics':
            self._fetch_key_metrics(
                endpoint, endpoint_url, endpoint_config, additional_params)

    def _fetch_page(self, endpoint, endpoint_url, endpoint_config, parentKey=None):

        pagination_loop = True
        endpoint_params = {
            'page': 1,
            'per_page': 200
        }
        self.UUIDS[endpoint] = []

        while pagination_loop:

            logging.info(
                f'Extracting [{endpoint}] - Page {endpoint_params["page"]}')

            data_in = self.get_request(
                endpoint_url, endpoint_params, self.API_TOKEN)

            MappingParser(
                destination=self.DESTINATION,
                endpoint=endpoint,
                endpoint_data=data_in[endpoint_config['dataType']],
                incremental=self.INCREMENTAL,
                parent_key=parentKey
            )

            endpoint_id = 'id' if endpoint == 'customers_subscriptions' else 'uuid'
            # Storing all UUIDS incases of child requests
            self.UUIDS[endpoint] = self.UUIDS[endpoint] + [i[endpoint_id]
                                                           for i in data_in[endpoint_config['dataType']]]

            if (not data_in.get('has_more') and data_in.get('has_more') is not None) \
                    or (data_in.get('current_page') is not None
                        and data_in.get('current_page') == data_in.get('total_pages')):
                pagination_loop = False
                break
            else:
                endpoint_params['page'] += 1

        self.STATE = {}

    def _fetch_activities(self, endpoint, endpoint_url, endpoint_config, additional_params):

        pagination_loop = True
        endpoint_params = {'per_page': 200}
        LAST_UUID = ''
        if self.STATE and self.INCREMENTAL:
            endpoint_params['start-after'] = self.state['start-after']
        else:
            for p in additional_params:
                if additional_params[p]:
                    endpoint_params[p] = additional_params[p]

        while pagination_loop:

            logging.info(f'Extracting [{endpoint}] - {LAST_UUID}')

            data_in = self.get_request(
                endpoint_url, endpoint_params, self.API_TOKEN)

            MappingParser(
                destination=self.DESTINATION,
                endpoint=endpoint,
                endpoint_data=data_in[endpoint_config['dataType']],
                incremental=self.INCREMENTAL,
            )

            if data_in[endpoint_config['dataType']]:
                LAST_UUID = data_in[endpoint_config['dataType']][-1]['uuid']
            endpoint_params = {'start-after': LAST_UUID, 'per_page': 200}

            if not data_in['has_more']:
                pagination_loop = False

        self.STATE = {endpoint: endpoint_params}

    def _fetch_key_metrics(self, endpoint, endpoint_url, endpoint_config, additional_params):

        endpoint_params = {}
        for p in additional_params:
            if additional_params[p]:
                endpoint_params[p] = additional_params[p]

        logging.info(f'Extracting[{endpoint}]')

        data_in = self.get_request(
            endpoint_url, endpoint_params, self.API_TOKEN)

        MappingParser(
            destination=self.DESTINATION,
            endpoint=endpoint,
            endpoint_data=data_in[endpoint_config['dataType']],
            incremental=self.INCREMENTAL,
        )

        self.STATE = {}

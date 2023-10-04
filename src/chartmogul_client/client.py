import json
import logging
import os
import uuid
from urllib.parse import urljoin

from keboola.http_client.async_client import AsyncHttpClient
from keboola.json_to_csv import Parser


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


class RetryableHttpException(Exception):
    pass


class ChartMogulClient(AsyncHttpClient):
    def __init__(self, destination, api_token, incremental=False, state=None):
        super().__init__(base_url=CHARTMOGUL_BASEURL,
                         auth=(api_token, ''),
                         retries=10,
                         retry_status_codes=[500, 502, 504])

        # Request parameters
        self.destination = destination
        self.INCREMENTAL = incremental
        self.STATE = state

    async def fetch_customers(self) -> list:
        endpoint_url = urljoin(CHARTMOGUL_BASEURL, "customers")

        endpoint_params = {
            'page': 1,
            'per_page': 200
        }

        results = []
        while True:
            logging.info(f'Extracting [customers] - Page {endpoint_params["page"]}')

            r_raw = await self.client.get(endpoint_url)
            r = r_raw.json()
            if r.get("entries"):
                for entry in r.get("entries"):
                    await self.save_result(r.get("entries"), "customers")
                    results.append(entry.get("uuid"))

            if not r.get('has_more'):
                return results
            else:
                endpoint_params['page'] += 1

    async def fetch(self, endpoint, additional_params=None):

        endpoint_config = CHARTMOGUL_ENDPOINT_CONFIGS[endpoint]

        if endpoint == 'customers_subscriptions':
            customer_uuids = await self.fetch_customers()
            async for results in self._fetch_customers_subscriptions(customer_uuids):
                for result in results:
                    await self.save_result(result, endpoint)

        elif endpoint == 'activities':
            async for results in self._fetch_activities(endpoint, endpoint_config, additional_params):
                for result in results:
                    await self.save_result(result, endpoint)

        elif endpoint == 'key_metrics':
            async for results in self._fetch_key_metrics(endpoint, additional_params):
                for result in results:
                    await self.save_result(result, endpoint)

        elif endpoint == 'invoices':
            async for results in self._fetch_invoices(endpoint):
                parser = Parser(main_table_name=endpoint, analyze_further=True)
                parsed = parser.parse_data(results)
                await self.save_result(parsed)

    async def save_result(self, results: dict):
        for result in results:
            path = os.path.join(self.destination, result)
            os.makedirs(path, exist_ok=True)

            full_path = os.path.join(path, f"{uuid.uuid4()}.json")
            with open(full_path, "w") as json_file:
                json.dump(results.get(result), json_file, indent=4)

    async def _fetch_customers_subscriptions(self, customer_uuids):
        endpoint_params = {
            'page': 1,
            'per_page': 200
        }

        for customer_uuid in customer_uuids:
            while True:
                endpoint = f'customers/{customer_uuid}/subscriptions'
                endpoint = urljoin(CHARTMOGUL_BASEURL, endpoint)

                # logging.info(f'Extracting [{endpoint}] - Page {endpoint_params["page"]}')

                r = await self.client.get(endpoint, params=endpoint_params)
                r = r.json()
                yield r.get(CHARTMOGUL_ENDPOINT_CONFIGS[endpoint]["dataType"], {})

                if not r.get('has_more'):
                    break
                else:
                    endpoint_params['page'] += 1

    async def _fetch_activities(self, endpoint, endpoint_config, additional_params):
        endpoint_url = urljoin(CHARTMOGUL_BASEURL, endpoint)

        endpoint_params = {'per_page': 200}
        LAST_UUID = ''
        if self.STATE and self.INCREMENTAL:
            endpoint_params['start-after'] = self.STATE.get('start-after')
        else:
            for p in additional_params:
                if additional_params[p]:
                    endpoint_params[p] = additional_params[p]

        while True:
            r = await self.client.get(endpoint_url, params=endpoint_params)
            r = r.json()
            yield r.get(CHARTMOGUL_ENDPOINT_CONFIGS[endpoint]["dataType"], {})

            if r[endpoint_config['dataType']]:
                LAST_UUID = r[endpoint_config['dataType']][-1]['uuid']
            endpoint_params = {'start-after': LAST_UUID, 'per_page': 200}

            if not r.get('has_more'):
                break

        self.STATE = {endpoint: endpoint_params}

    async def _fetch_key_metrics(self, endpoint, additional_params):
        endpoint_url = urljoin(CHARTMOGUL_BASEURL, CHARTMOGUL_ENDPOINT_CONFIGS[endpoint]["endpoint"])

        endpoint_params = {}
        for p in additional_params:
            if additional_params[p]:
                endpoint_params[p] = additional_params[p]

        r = await self.client.get(endpoint_url, params=endpoint_params)
        r = r.json()
        yield r.get(CHARTMOGUL_ENDPOINT_CONFIGS[endpoint]["dataType"], {})

        self.STATE = {}

    async def _fetch_invoices(self, endpoint):
        endpoint_url = urljoin(CHARTMOGUL_BASEURL, CHARTMOGUL_ENDPOINT_CONFIGS[endpoint]["endpoint"])

        r = await self.client.get(endpoint_url)
        r = r.json()
        yield r.get(CHARTMOGUL_ENDPOINT_CONFIGS[endpoint]["dataType"], {})

        self.STATE = {}

import asyncio
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
        'dataType': 'entries'
    },
    'customers': {
        'endpoint': 'customers',
        'dataType': 'entries'
    },
    'customers_subscriptions': {
        'endpoint': 'customers/{{customers_uuid}}/subscriptions',
        'dataType': 'entries'
    },
    'key_metrics': {
        'endpoint': 'metrics/all',
        'dataType': 'entries'
    },
    'invoices': {
        'endpoint': 'invoices',
        'dataType': 'invoices'
    }
}


class RetryableHttpException(Exception):
    pass


class ChartMogulClient(AsyncHttpClient):
    def __init__(self, destination, api_token, incremental=False, state=None, batch_size: int = 10):
        super().__init__(base_url=CHARTMOGUL_BASEURL,
                         auth=(api_token, ''),
                         retries=10,
                         retry_status_codes=[500, 502, 504],
                         max_requests_per_second=40)

        # Request parameters
        self.destination = destination
        self.incremental = incremental
        self.state = state
        self.batch_size = batch_size

    async def fetch(self, endpoint, additional_params=None):

        endpoint_config = CHARTMOGUL_ENDPOINT_CONFIGS[endpoint]

        if endpoint == 'customers':
            return await self._fetch_customers()

        if endpoint == 'customers_subscriptions':
            customer_uuids = await self.fetch(endpoint="customers")
            async for results in self._fetch_customers_subscriptions(customer_uuids):
                parser = Parser(main_table_name=endpoint, analyze_further=True)
                parsed = parser.parse_data(results)
                await self.save_result(parsed)

        elif endpoint == 'activities':
            async for results in self._fetch_activities(endpoint, endpoint_config, additional_params):
                parser = Parser(main_table_name=endpoint, analyze_further=True)
                parsed = parser.parse_data(results)
                await self.save_result(parsed)

        elif endpoint == 'key_metrics':
            async for results in self._fetch_key_metrics(endpoint, additional_params):
                parser = Parser(main_table_name=endpoint, analyze_further=True)
                parsed = parser.parse_data(results)
                await self.save_result(parsed)

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
        tasks = []
        for i, customer_uuid in enumerate(customer_uuids):
            tasks.append(self._fetch_customer_subscriptions(customer_uuid))

            # Check if the batch size is reached or if we are at the last customer
            if len(tasks) == self.batch_size or i == len(customer_uuids) - 1:
                results = await asyncio.gather(*tasks)
                for result in results:
                    yield result

                tasks.clear()

    async def _fetch_customer_subscriptions(self, customer_uuid):
        endpoint_params = {
            'page': 1,
            'per_page': 200
        }

        all_entries = []
        while True:
            endpoint = f'customers/{customer_uuid}/subscriptions'
            endpoint = urljoin(CHARTMOGUL_BASEURL, endpoint)

            r = await self._get(endpoint, params=endpoint_params)
            entries = r.get(CHARTMOGUL_ENDPOINT_CONFIGS["customers_subscriptions"]["dataType"])
            for entry in entries:
                entry["customer_uuid"] = customer_uuid

            all_entries.extend(entries)

            if not r.get('has_more'):
                break
            else:
                endpoint_params['page'] += 1

        return all_entries

    async def _fetch_activities(self, endpoint, endpoint_config, additional_params):
        endpoint_url = urljoin(CHARTMOGUL_BASEURL, endpoint)

        endpoint_params = {'per_page': 200}
        last_uuid = ''
        if self.state and self.incremental:
            endpoint_params['start-after'] = self.state.get('start-after')
        else:
            for p in additional_params:
                if additional_params[p]:
                    endpoint_params[p] = additional_params[p]

        while True:
            r = await self.client.get(endpoint_url, params=endpoint_params)
            r = r.json()
            yield r.get(CHARTMOGUL_ENDPOINT_CONFIGS[endpoint]["dataType"], {})

            if r[endpoint_config['dataType']]:
                last_uuid = r[endpoint_config['dataType']][-1]['uuid']
            endpoint_params = {'start-after': last_uuid, 'per_page': 200}

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

    async def _fetch_invoices(self, endpoint):
        endpoint_url = urljoin(CHARTMOGUL_BASEURL, CHARTMOGUL_ENDPOINT_CONFIGS[endpoint]["endpoint"])

        r = await self.client.get(endpoint_url)
        r = r.json()
        yield r.get(CHARTMOGUL_ENDPOINT_CONFIGS[endpoint]["dataType"], {})

    async def _fetch_customers(self) -> list:
        endpoint_url = urljoin(CHARTMOGUL_BASEURL, "customers")

        params = {
            'page': 1,
            'per_page': 200
        }

        results = []
        while True:
            logging.info(f'Extracting [customers] - Page {params["page"]}')

            r = await self.client.get(endpoint_url, params=params)
            r = r.json()
            data = r.get(CHARTMOGUL_ENDPOINT_CONFIGS["customers"]["dataType"], {})

            if r:
                parser = Parser(main_table_name="customers", analyze_further=True)
                parsed = parser.parse_data(data)
                await self.save_result(parsed)

                for customer in parsed.get("customers", []):
                    results.append(customer.get("uuid"))

            if not r.get('has_more'):
                return results
            else:
                params['page'] += 1

    async def _get(self, endpoint: str, params=None) -> dict:
        if params is None:
            params = {}

        try:
            return await self.get(endpoint, params=params)
        except json.decoder.JSONDecodeError as e:
            raise Exception(e) from e

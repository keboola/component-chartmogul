import asyncio
import json
import logging
import os
import uuid
from pathlib import Path
from urllib.parse import urljoin
from typing import AsyncIterable

from httpx import HTTPStatusError
from keboola.http_client.async_client import AsyncHttpClient
from keboola.json_to_csv import Parser, TableMapping

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

MAX_REQUESTS_PER_SECOND = BATCH_SIZE = 40


class ChartMogulClientException(Exception):
    pass


class ChartMogulClient(AsyncHttpClient):
    def __init__(self, destination, api_token, incremental=False, state=None, batch_size: int = BATCH_SIZE,
                 debug: bool = False):
        super().__init__(base_url=CHARTMOGUL_BASEURL,
                         auth=(api_token, ''),
                         retries=5,
                         retry_status_codes=[402, 429, 500, 502, 503, 504],
                         max_requests_per_second=MAX_REQUESTS_PER_SECOND,
                         timeout=10,
                         debug=debug)

        # Request parameters
        self.processed_records = 0
        self.parser = None
        self.destination = destination
        self.incremental = incremental
        self.batch_size = batch_size
        mappings = Path(os.path.abspath(__file__)).parent.joinpath('mappings.json').as_posix()
        self._table_mappings = json.load(open(mappings))

    async def fetch(self, endpoint, additional_params=None) -> dict:

        table_mapping = TableMapping.build_from_legacy_mapping({endpoint: self._table_mappings[endpoint]})
        self.parser = Parser(main_table_name=endpoint, table_mapping=table_mapping, analyze_further=True)

        if endpoint == 'customers':
            await self._fetch_customers()

        elif endpoint == 'customers_subscriptions':
            customer_uuids = await self._fetch_customers(save_results=False)
            if customer_uuids:
                async for results in self._fetch_customers_subscriptions(customer_uuids):
                    parsed = self.parser.parse_data(results)
                    await self.save_result(parsed)
            else:
                raise ChartMogulClientException("Cannot fetch customer subscriptions, reason: No customers found.")

        elif endpoint == 'activities':
            async for results in self._fetch_activities(endpoint, additional_params):
                parsed = self.parser.parse_data(results)
                await self.save_result(parsed)

        elif endpoint == 'key_metrics':
            async for results in self._fetch_key_metrics(endpoint, additional_params):
                parsed = self.parser.parse_data(results)
                await self.save_result(parsed)

        elif endpoint == 'invoices':
            async for results in self._fetch_invoices(endpoint):
                parsed = self.parser.parse_data(results)
                await self.save_result(parsed)

        else:
            raise ChartMogulClientException(f"Unsupported endpoint: {endpoint}")

        return self.parser.table_mapping.as_dict()

    async def save_result(self, results: dict) -> None:
        for result in results:
            path = os.path.join(self.destination, result)
            os.makedirs(path, exist_ok=True)

            full_path = os.path.join(path, f"{uuid.uuid4()}.json")
            with open(full_path, "w") as json_file:
                json.dump(results.get(result), json_file, indent=4)
        self.parser._csv_file_results = {}

    async def _fetch_customers_subscriptions(self, customer_uuids) -> AsyncIterable:
        tasks = []
        for i, customer_uuid in enumerate(customer_uuids):
            tasks.append(self._fetch_customer_subscriptions(customer_uuid))

            # Check if the batch size is reached or if we are at the last customer
            if len(tasks) == self.batch_size or i == len(customer_uuids) - 1:
                results = await asyncio.gather(*tasks)
                for result in results:
                    yield result

                tasks.clear()

    async def _fetch_customer_subscriptions(self, customer_uuid) -> list:
        endpoint_params = {}

        all_entries = []
        while True:
            endpoint = f'customers/{customer_uuid}/subscriptions'
            endpoint = urljoin(CHARTMOGUL_BASEURL, endpoint)

            r = await self._get(endpoint, params=endpoint_params)
            entries = r.get(CHARTMOGUL_ENDPOINT_CONFIGS["customers_subscriptions"]["dataType"])
            for entry in entries:
                entry["customers_uuid"] = customer_uuid

            all_entries.extend(entries)

            if not r.get('has_more'):
                break
            else:
                endpoint_params['cursor'] = r.get('cursor')

        self.processed_records += 1

        if self.processed_records % 1000 == 0:
            logging.info(f"Fetched {self.processed_records} customer subscriptions.")

        return all_entries

    async def _fetch_activities(self, endpoint, additional_params) -> AsyncIterable:
        endpoint_url = urljoin(CHARTMOGUL_BASEURL, endpoint)

        endpoint_params = {'per_page': 200}
        for p in additional_params:
            if additional_params[p]:
                endpoint_params[p] = additional_params[p]

        while True:
            r = await self._get(endpoint_url, params=endpoint_params)
            yield r.get(CHARTMOGUL_ENDPOINT_CONFIGS[endpoint]["dataType"], {})

            if not r.get('has_more'):
                break
            else:
                endpoint_params['cursor'] = r.get('cursor')

    async def _fetch_key_metrics(self, endpoint, additional_params) -> AsyncIterable:
        endpoint_url = urljoin(CHARTMOGUL_BASEURL, CHARTMOGUL_ENDPOINT_CONFIGS[endpoint]["endpoint"])
        endpoint_params = {}

        for p in additional_params:
            if additional_params[p]:
                endpoint_params[p] = additional_params[p]

        r = await self._get(endpoint_url, params=endpoint_params)
        yield r.get(CHARTMOGUL_ENDPOINT_CONFIGS[endpoint]["dataType"], {})

    async def _fetch_invoices(self, endpoint) -> AsyncIterable:
        endpoint_url = urljoin(CHARTMOGUL_BASEURL, CHARTMOGUL_ENDPOINT_CONFIGS[endpoint]["endpoint"])
        endpoint_params = {}

        while True:
            r = await self._get(endpoint_url, params=endpoint_params)
            yield r.get(CHARTMOGUL_ENDPOINT_CONFIGS[endpoint]["dataType"], {})

            if not r.get('has_more'):
                break
            else:
                endpoint_params['cursor'] = r.get('cursor')

    async def _fetch_customers(self, save_results: bool = True) -> list:
        customer_uuids = []
        done = False
        i = 1

        while not done:
            tasks = [self._fetch_customers_page(i, save_results) for i in range(i, i + self.batch_size)]
            i += self.batch_size

            batch_results = await asyncio.gather(*tasks)

            for result in batch_results:
                customer_uuids.extend(result)
                if not result:
                    done = True

        return customer_uuids

    async def _fetch_customers_page(self, page: int, save_results: bool = True) -> list:
        endpoint_url = urljoin(CHARTMOGUL_BASEURL, "customers")
        parser = self.parser if save_results else Parser(main_table_name="customers", analyze_further=True)

        params = {
            'page': page,
            'per_page': 200
        }

        results = []
        r = await self._get(endpoint_url, params=params)
        data = r.get(CHARTMOGUL_ENDPOINT_CONFIGS["customers"]["dataType"], {})
        if data:
            parsed = parser.parse_data(data)
            if save_results:
                await self.save_result(parsed)

            for customer in parsed.get("customers", []):
                results.append(customer.get("uuid"))
        return results

    async def _get(self, endpoint: str, params=None) -> dict:
        if params is None:
            params = {}

        r = await self.get_raw(endpoint, params=params)

        try:
            r.raise_for_status()
        except HTTPStatusError:
            raise ChartMogulClientException(f"Cannot fetch resource: {endpoint}")

        try:
            return r.json()
        except json.decoder.JSONDecodeError as e:
            raise ChartMogulClientException(f"Cannot parse response for {endpoint}, exception: {e}") from e

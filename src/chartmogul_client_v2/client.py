import logging
from requests.exceptions import HTTPError
from keboola.http_client import HttpClient

CHARTMOGUL_BASEURL = 'https://api.chartmogul.com/v1/'

KEY_METRICS_ENDPOINT = 'metrics/all'
KEY_ACTIVITIES_ENDPOINT = 'activities'
KEY_CUSTOMERS_ENDPOINT = "customers"
KEY_SUBSCRIPTIONS_ENDPOINT = "subscriptions"

START_PAGE = 1
MAX_PAGE_SIZE = 200

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


class ChartMogulClientException(Exception):
    pass


class ChartMogulClient(HttpClient):
    def __init__(self, api_token):
        super().__init__(CHARTMOGUL_BASEURL, auth=(api_token, ''))

    def get_activities(self, additional_params):
        return self.get_paged_generator(endpoint_name=KEY_ACTIVITIES_ENDPOINT,
                                        endpoint_url=KEY_ACTIVITIES_ENDPOINT,
                                        use_pages=False,
                                        additional_params=additional_params)

    def get_customers(self):
        return self.get_paged_generator(endpoint_name=KEY_CUSTOMERS_ENDPOINT,
                                        endpoint_url=KEY_CUSTOMERS_ENDPOINT,
                                        use_pages=True,
                                        log=False)

    def get_customer_uuids(self):
        all_customer_uuids = []
        for customers in self.get_customers():
            for customer in customers:
                all_customer_uuids.append(customer.get("uuid"))
        return all_customer_uuids

    def get_customer_subscriptions(self, customer_uuid):
        customer_sub_endpoint = "/".join([KEY_CUSTOMERS_ENDPOINT, customer_uuid, KEY_SUBSCRIPTIONS_ENDPOINT])
        customer_subscriptions = self.get_paged_results(endpoint_name="Customer Subscriptions",
                                                        endpoint_url=customer_sub_endpoint,
                                                        data_type="entries",
                                                        parent_key=customer_uuid)
        return customer_subscriptions

    def get_key_metrics(self, additional_params):
        endpoint_params = {}
        for p in additional_params:
            if additional_params[p]:
                endpoint_params[p] = additional_params[p]

        logging.info('Extracting Key Metrics')

        try:
            key_metric_data = self.get(endpoint_path=KEY_METRICS_ENDPOINT, params=endpoint_params)
        except HTTPError as http_error:
            raise ChartMogulClientException(http_error) from http_error

        return key_metric_data["entries"]

    def get_invoices(self):
        pass

    def get_paged_generator(self,
                            endpoint_name="",
                            endpoint_url="",
                            use_pages=True,
                            additional_params=None,
                            data_type="entries",
                            log=True):

        if log:
            logging.info(f'Extracting {endpoint_name}')

        if not additional_params:
            additional_params = {}

        additional_params['per_page'] = MAX_PAGE_SIZE

        if use_pages:
            additional_params['page'] = START_PAGE

        pagination_loop = True
        while pagination_loop:

            if use_pages and log:
                logging.info(f'Extracting {endpoint_name} - Page {additional_params["page"]}')

            page_data = self.get(endpoint_path=endpoint_url, params=additional_params)
            yield page_data[data_type]

            if not use_pages and page_data["entries"]:
                last_id = page_data[data_type][-1]['uuid']
                additional_params['start-after'] = last_id

            if self.pagination_loop_ended(dict(page_data), data_type):
                pagination_loop = False
            elif use_pages:
                additional_params['page'] += 1

    def get_paged_results(self, endpoint_name="", endpoint_url="", data_type="", parent_key=""):
        paged_results = []
        logging.info(f'Extracting {endpoint_name} for {parent_key}')
        for results in self.get_paged_generator(endpoint_name=endpoint_name,
                                                endpoint_url=endpoint_url,
                                                use_pages=True,
                                                data_type=data_type,
                                                log=False):
            for result in results:
                result["uuid"] = parent_key
                paged_results.append(result)
        return paged_results

    @staticmethod
    def pagination_loop_ended(page_data, data_type):
        if not page_data or page_data[data_type]:
            return True
        elif "has_more" in page_data and not page_data.get("has_more", False):
            return True
        elif "current_page" in page_data and page_data.get('current_page') == page_data.get('total_pages'):
            return True
        else:
            return False

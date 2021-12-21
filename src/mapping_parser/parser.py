import os
import json
import pandas as pd


with open('/code/src/mapping_parser/mapping.json') as f:
    # with open('mapping_parser/mapping.json') as f:
    CHARTMOGUL_MAPPING = json.load(f)


class MappingParser():
    def __init__(self, destination, endpoint, endpoint_data, parent_key=None, incremental=False, mapping=None):

        self.destination = destination
        self.endpoint = endpoint
        self.endpoint_data = endpoint_data
        self.mapping = mapping if mapping else CHARTMOGUL_MAPPING[endpoint]
        self.parent_key = parent_key
        self.output = []
        self.primary_key = []
        self.incremental = incremental

        # Countermeasures for response coming in as DICT
        if type(self.endpoint_data) == dict:
            self.endpoint_data = []
            self.endpoint_data.append(endpoint_data)

        # Parsing
        self.parse()
        if self.output:
            self._output(df_json=self.output, filename=self.endpoint)
            self._produce_manifest(
                filename=self.endpoint, incremental=self.incremental, primary_key=self.primary_key)

    def parse(self):
        for row in self.endpoint_data:
            row_json = {}

            for m in self.mapping:
                col_type = self.mapping[m].get('type') if type(
                    self.mapping[m]) != str else 'string'

                if col_type == 'string':
                    key = self.mapping[m]
                    value = self._fetch_value(row=row, key=m)
                    row_json[key] = value

                elif col_type == 'column' or not col_type:
                    key = self.mapping[m]['mapping']['destination']
                    # value = row[m]
                    value = self._fetch_value(row=row, key=m)
                    row_json[key] = value

                    # Primary key for incremental load
                    if "primaryKey" in self.mapping[m]['mapping'] and key not in self.primary_key:
                        self.primary_key.append(key)

                elif col_type == 'user':
                    key = self.mapping[m]['mapping']['destination']
                    value = self.parent_key
                    row_json[key] = value

                    # Primary key for incremental load
                    self.primary_key.append(
                        key) if key not in self.primary_key else ''

                elif col_type == 'table':
                    endpoint = self.mapping[m]['destination']
                    mapping = self.mapping[m]['tableMapping']
                    parent_key = row['uuid']
                    data = self._fetch_value(row=row, key=m)

                    MappingParser(
                        destination=self.destination,
                        endpoint=endpoint,
                        endpoint_data=data,
                        mapping=mapping,
                        parent_key=parent_key,
                        incremental=self.incremental
                    )

            self.output.append(row_json)

    @staticmethod
    def _fetch_value(row, key):
        '''
        Fetching value from a nested object
        '''
        key_list = key.split('.')
        value = row

        try:
            for k in key_list:
                value = value[k]

        except Exception:
            value = ''

        return value

    def _output(self, df_json, filename):
        output_filename = f'{self.destination}/{filename}.csv'
        if df_json:
            data_output = pd.DataFrame(df_json, dtype=str)
            if not os.path.isfile(output_filename):
                with open(output_filename, 'w') as b:
                    data_output.to_csv(b, index=False)
                b.close()
            else:
                with open(output_filename, 'a') as b:
                    data_output.to_csv(b, index=False, header=False)
                b.close()

    def _produce_manifest(self, filename, incremental, primary_key):
        manifest_filename = f'{self.destination}/{filename}.csv.manifest'
        manifest = {
            'incremental': incremental,
            'primary_key': primary_key,
        }

        with open(manifest_filename, 'w') as file_out:
            json.dump(manifest, file_out)

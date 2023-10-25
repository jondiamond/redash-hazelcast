try:
    import logging
    import random
    import os
    import hazelcast
    import requests
    import sys

    from hazelcast.core import HazelcastJsonValue
    from hazelcast.discovery import HazelcastCloudDiscovery
    from tempfile import NamedTemporaryFile
    from base64 import b64decode
    enabled = True
except ImportError:
    enabled = False


from redash.query_runner import BaseQueryRunner, register
from redash.query_runner import (
    TYPE_STRING,
    TYPE_DATE,
    TYPE_DATETIME,
    TYPE_INTEGER,
    TYPE_FLOAT,
    TYPE_BOOLEAN,
)
from redash.utils import json_dumps, json_loads

TYPES_MAP = {
    0: "TYPE_STRING",
    1: "TYPE_BOOLEAN",
    2: "TYPE_INTEGER",
    3: "TYPE_INTEGER",
    4: "TYPE_INTEGER",
    5: "TYPE_INTEGER",
    6: "TYPE_STRING",
    7: "TYPE_FLOAT",
    8: "TYPE_FLOAT",
    9: "TYPE_DATETIME",
    10: "TYPE_DATETIME",
    11: "TYPE_INTEGER",
    12: "TYPE_INTEGER",
    13: "TYPE_STRING",
    14: "TYPE_STRING"
}

def parse_sql_statements(query):
    statements = query.strip().split(";")
    return [stmt.strip() for stmt in statements if stmt.strip()]


def _create_cert_file(configuration, key, ssl_config):
    file_key = key + "File"
    if file_key in configuration:
        with NamedTemporaryFile(mode="w", delete=False) as cert_file:
            cert_bytes = b64decode(configuration[file_key])
            cert_file.write(cert_bytes.decode("utf-8"))

        ssl_config[key] = cert_file.name


def _cleanup_ssl_certs(ssl_config):
    for k, v in ssl_config.items():
        if k != "sslmode":
            os.remove(v)


def _get_ssl_config(configuration):
    ssl_config = {"sslmode": configuration.get("ssl", "enabled")}
    _create_cert_file(configuration, "sslrootcert", ssl_config)
    _create_cert_file(configuration, "sslcert", ssl_config)
    _create_cert_file(configuration, "sslkey", ssl_config)

    return ssl_config


class Hazelcast(BaseQueryRunner):
    should_annotate_query = False
    noop_query = "SELECT 1"

    @classmethod
    def configuration_schema(cls):
        return {
            "type": "object",
            "properties": {
                "clusterMembers": {"type": "string", "title": "Cluster Members"},
                "clusterName": {"type": "string", "title": "Cluster Name/ID"},
                "cloudUrl": {"type": "string", "title": "Cloud URL", "default": "api.viridian.hazelcast.com"},
                "discoveryToken": {"type": "string", "title": "Discovery Token"},
                "keystoreTruststorePassword": {"type": "string", "title": "Keystore Password"},
                "sslrootcertFile": {"type": "string", "title": "SSL Root Certificate"},
                "sslcertFile": {"type": "string", "title": "SSL Client Certificate"},
                "sslkeyFile": {"type": "string", "title": "SSL Client Key"},
            },
            "order": ["clusterMembers", "clusterName", "discoveryToken", "keystoreTruststorePassword", "sslrootcertFile", "sslcertFile", "sslkeyFile"],
            "required": ["clusterName"],
            "secret": ["discoveryToken", "keystoreTruststorePassword"],
        }

    @classmethod
    def type(cls):
        return "hazelcast"

    @classmethod
    def name(cls):
        return "Hazelcast Platform"

    @classmethod
    def enabled(cls):
        return enabled

    @classmethod
    def determine_type(self, data_type):
        t = TYPES_MAP.get(data_type, None)

        return t

    def _get_connection(self):

        max_connection_timeout=60

        if (self.configuration.get("discoveryToken") is not None and len(self.configuration.get("discoveryToken")) > 0):
            HazelcastCloudDiscovery._CLOUD_URL_BASE = self.configuration.get("cloudUrl")

            self.ssl_config = _get_ssl_config(self.configuration)

            client = hazelcast.HazelcastClient(
                cluster_name=self.configuration["clusterName"],
                cloud_discovery_token=self.configuration["discoveryToken"],
                statistics_enabled=True,
                ssl_enabled=True,
                ssl_cafile=self.ssl_config.get("sslrootcert"),
                ssl_certfile=self.ssl_config.get("sslcert"),
                ssl_keyfile=self.ssl_config.get("sslkey"),
                ssl_password=self.configuration["keystoreTruststorePassword"],
                cluster_connect_timeout=max_connection_timeout,
            )
        else:
            if (self.configuration.get("ssl", "enabled")):
                client = hazelcast.HazelcastClient(
                    cluster_members=self.configuration.get("clusterMembers").split(','),
                    cluster_name=self.configuration.get("clusterName"),
                    smart_routing=True,
                    redo_operation=False,
                    statistics_enabled=True,
                    connection_timeout=30.0,
                    cluster_connect_timeout=max_connection_timeout,
                )
            else:
                client = hazelcast.HazelcastClient(
                    cluster_members=self.configuration.get("clusterMembers").split(','),
                    cluster_name=self.configuration.get("clusterName"),
                    smart_routing=True,
                    redo_operation=False,
                    statistics_enabled=True,
                    ssl_cafile=self.ssl_config.get("sslrootcert"),
                    ssl_certfile=self.ssl_config.get("sslcert"),
                    ssl_keyfile=self.ssl_config.get("sslkey"),
                    ssl_password=self.configuration["keystoreTruststorePassword"],
                    connection_timeout=30.0,
                    cluster_connect_timeout=max_connection_timeout,
                )


        return client

    def _column_name(self, column_name):
        if self.configuration.get("lower_case_columns", False):
            return column_name.lower()

        return column_name


    def fetch_columns(self, columns):
        column_names = []
        duplicates_counter = 1
        new_columns = []

        for col in columns:
            column_name = col[0]
            if column_name in column_names:
                column_name = "{}{}".format(column_name, duplicates_counter)
                duplicates_counter += 1

            column_names.append(column_name)
            new_columns.append(
                {"name": column_name, "friendly_name": column_name, "type": col[1]}
            )

        return new_columns

    def _parse_results(self, result):
        rows = []
        columns = []
        if result.is_row_set():
            for row in result:
                columns = self.fetch_columns(
                    [
                        (self._column_name(i.name), self.determine_type(i.type))
                        for i in row.metadata.columns
                    ]
                )


                currentRow = dict(zip((column["name"] for column in columns), row))

                rows.append(currentRow)

            data = {"columns": columns, "rows": rows}
        else:
            columns = [{"name": "count", "friendly_name": "count", "type": 2}]
            row = [result.update_count()]
            currentRow = dict(zip((column["name"] for column in columns), row))
            rows.append(currentRow)

            data = {"columns": columns, "rows": rows}

        return data


    def lookup_query_id_from_result(self, result):
        return 0

    def get_query_id_from_hash(self, query_hash, client):

        query_id_hash_map = client.get_map("query_id_hash_map")

        query_id = query_id_hash_map.get(query_hash)

        if query_id is None:
            print("No matching query_hash: " + query_hash + " found in map")
            return 0
        else:
            print("Found query_id: " + query_id)
            return query_id

    def run_query(self, query, user, metadata=None):

        query_id = 0
        query_hash = ""

        print("HZ debug [query]:" + query, file=sys.stderr )

        if metadata is not None:
            print("HZ debug [metadata]:", file=sys.stderr )
            print(metadata, file=sys.stderr )

        self.ssl_config = _get_ssl_config(self.configuration)
        client = self._get_connection()

        sql_statements = parse_sql_statements(query)

        results = []

        try:

            for statement in sql_statements:
                result = client.sql.execute(statement).result()
                results.append(self._parse_results(result))

            # Combine the results
            combined_columns = []
            combined_rows = []
            error = None

            for result in results:
                combined_columns.extend(result["columns"])
                combined_rows.extend(result["rows"])

            data = {"columns": combined_columns, "rows": combined_rows}
            json_data = json_dumps(data)

        finally:
            client.shutdown()
            _cleanup_ssl_certs(self.ssl_config)

        return json_data, error

    def query_update_listener(object):
        def __init__(self, query_result):
             self.query_result = query_result

    def on_event(self, event):
        # Update query result with new data
        self.query_result.update(event.resultset)


register(Hazelcast)


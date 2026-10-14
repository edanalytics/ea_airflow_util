import os
from contextlib import contextmanager

import requests
from airflow_client.client import ApiClient, Configuration, ConnectionApi
from airflow_client.client.exceptions import ApiException
from airflow_client.client.models.connection_body import ConnectionBody

_PAGE_SIZE = 100


# TODO: Probably never localhost, and we should probably just settle to use
# AIRFLOW__WEBSERVER__BASE_URL, unless there's a new env var for free in v3.
def _get_api_host() -> str:
    host = os.environ.get("AIRFLOW__API__BASE_URL")
    if not host:
        raise ValueError("bla bla bla")
    return host
    # if not host:
    #     from airflow.configuration import conf

    #     host = conf.get("api", "base_url", fallback=None)
    # return (host or "http://localhost:8080").rstrip("/")


# TODO: Work w/ CE on auth flow.
# Consider caching. Might not be necessary.
def _get_access_token(host: str) -> str:
    token = os.environ.get("AIRFLOW_API_ACCESS_TOKEN")
    if token:
        return token

    username = os.environ.get("AIRFLOW_API_USERNAME")
    password = os.environ.get("AIRFLOW_API_PASSWORD")
    if username and password:
        response = requests.post(
            f"{host}/auth/token",
            json={"username": username, "password": password},
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
        response.raise_for_status()
        return response.json()["access_token"]

    raise RuntimeError(
        "Airflow API credentials are required. Set AIRFLOW_API_ACCESS_TOKEN or "
        "AIRFLOW_API_USERNAME and AIRFLOW_API_PASSWORD."
    )


@contextmanager
def _connection_api():
    host = _get_api_host()
    configuration = Configuration(host=host, access_token=_get_access_token(host))
    with ApiClient(configuration) as api_client:
        yield ConnectionApi(api_client)


def _connection_body(
    conn_id,
    conn_type=None,
    description=None,
    host=None,
    login=None,
    password=None,
    schema=None,
    port=None,
    extra=None,
):
    body_kwargs = {"connection_id": conn_id}
    if conn_type is not None:
        body_kwargs["conn_type"] = conn_type
    if description is not None:
        body_kwargs["description"] = description
    if host is not None:
        body_kwargs["host"] = host
    if login is not None:
        body_kwargs["login"] = login
    if password is not None:
        body_kwargs["password"] = password
    if port is not None:
        body_kwargs["port"] = port
    if extra is not None:
        body_kwargs["extra"] = extra
    if schema is not None:
        body_kwargs["var_schema"] = schema
    return ConnectionBody(**body_kwargs)


def _connection_exists(api: ConnectionApi, conn_id: str) -> bool:
    try:
        api.get_connection(conn_id)
    except ApiException as exc:
        if exc.status == 404:
            return False
        raise
    return True


def create_conn(conn_id, login, password, conn_type="http", host=None):
    body = _connection_body(
        conn_id=conn_id,
        conn_type=conn_type,
        login=login,
        password=password,
        host=host,
    )
    with _connection_api() as api:
        api.post_connection(body)


def upsert_conn(conn_id, conn_type, login, password, host=None):
    body = _connection_body(
        conn_id=conn_id,
        conn_type=conn_type,
        login=login,
        password=password,
        host=host,
    )
    update_mask = ["conn_type", "host", "login", "password"]

    with _connection_api() as api:
        if _connection_exists(api, conn_id):
            api.patch_connection(conn_id, body, update_mask=update_mask)
        else:
            api.post_connection(body)


def list_conn(pattern="%"):
    list_kwargs = {}
    if pattern != "%":
        if pattern.endswith("_"):
            list_kwargs["connection_id_prefix_pattern"] = pattern
        else:
            list_kwargs["connection_id_pattern"] = pattern

    conn_ids = []
    offset = 0

    with _connection_api() as api:
        while True:
            response = api.get_connections(limit=_PAGE_SIZE, offset=offset, **list_kwargs)
            conn_ids.extend(conn.connection_id for conn in response.connections)
            offset += _PAGE_SIZE
            if offset >= response.total_entries:
                break

    return conn_ids


def update_conn(
    conn_id,
    conn_type=None,
    description=None,
    host=None,
    login=None,
    password=None,
    schema=None,
    port=None,
    extra=None,
    uri=None,
):
    if uri is not None:
        raise NotImplementedError(
            "Updating connections via uri is not supported by the Airflow REST API."
        )

    body = _connection_body(
        conn_id=conn_id,
        conn_type=conn_type,
        description=description,
        host=host,
        login=login,
        password=password,
        schema=schema,
        port=port,
        extra=extra,
    )
    update_mask = [
        field
        for field, value in (
            ("conn_type", conn_type),
            ("description", description),
            ("host", host),
            ("login", login),
            ("password", password),
            ("var_schema", schema),
            ("port", port),
            ("extra", extra),
        )
        if value is not None
    ]

    with _connection_api() as api:
        if not update_mask:
            return
        if _connection_exists(api, conn_id):
            api.patch_connection(conn_id, body, update_mask=update_mask)


def delete_conn(conn_id):
    with _connection_api() as api:
        try:
            api.delete_connection(conn_id)
        except ApiException as exc:
            if exc.status != 404:
                raise

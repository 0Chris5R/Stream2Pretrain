"""Apply the least catalog grants required by the Iceberg writer and reader."""

from __future__ import annotations

import json
import os
from typing import Any

import requests


def required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"missing environment variable: {name}")
    return value


def response_json(response: requests.Response) -> dict[str, Any]:
    response.raise_for_status()
    value = response.json()
    if not isinstance(value, dict):
        raise RuntimeError(f"unexpected Polaris response from {response.url}")
    return value


def access_headers(base_url: str, client_id: str, client_secret: str) -> dict[str, str]:
    token_response = requests.post(
        base_url + "/api/catalog/v1/oauth/tokens",
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
            "scope": os.environ.get("POLARIS_SCOPE", "PRINCIPAL_ROLE:ALL"),
        },
        timeout=15,
    )
    token = response_json(token_response).get("access_token")
    if not isinstance(token, str) or not token:
        raise RuntimeError("Polaris token response did not contain an access token")
    return {"Authorization": f"Bearer {token}"}


def main() -> None:
    catalog = required_env("POLARIS_WAREHOUSE")
    namespace = required_env("ICEBERG_NAMESPACE")
    role = os.environ.get("POLARIS_CATALOG_ROLE", "catalog_admin")
    principal_role = os.environ.get("POLARIS_PRINCIPAL_ROLE", "service_admin")
    base_url = required_env("POLARIS_URI").removesuffix("/api/catalog")
    client_id, client_secret = required_env("POLARIS_CREDENTIAL").split(":", 1)
    headers = access_headers(base_url, client_id, client_secret)
    management_url = base_url + "/api/management/v1"

    catalogs = response_json(
        requests.get(management_url + "/catalogs", headers=headers, timeout=15)
    ).get("catalogs", [])
    catalog_created = not any(
        isinstance(item, dict) and item.get("name") == catalog for item in catalogs
    )
    if catalog_created:
        bucket = required_env("MINIO_GOLD_BUCKET")
        location = f"s3://{bucket}/warehouse"
        endpoint = required_env("MINIO_ENDPOINT")
        response = requests.post(
            management_url + "/catalogs",
            headers=headers,
            json={
                "catalog": {
                    "type": "INTERNAL",
                    "name": catalog,
                    "properties": {"default-base-location": location},
                    "storageConfigInfo": {
                        "storageType": "S3",
                        "allowedLocations": [location],
                        "region": os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
                        "endpoint": endpoint,
                        "endpointInternal": endpoint,
                        "pathStyleAccess": True,
                        "stsUnavailable": True,
                    },
                }
            },
            timeout=15,
        )
        response.raise_for_status()

    roles_url = f"{management_url}/catalogs/{catalog}/catalog-roles"
    roles = response_json(requests.get(roles_url, headers=headers, timeout=15)).get("roles", [])
    if not any(isinstance(item, dict) and item.get("name") == role for item in roles):
        response = requests.post(
            roles_url,
            headers=headers,
            json={"catalogRole": {"name": role}},
            timeout=15,
        )
        response.raise_for_status()

    grants_url = f"{roles_url}/{role}/grants"
    current = response_json(requests.get(grants_url, headers=headers, timeout=15)).get("grants", [])
    existing = {
        (grant.get("type"), tuple(grant.get("namespace", [])), grant.get("privilege"))
        for grant in current
        if isinstance(grant, dict)
    }
    added: list[str] = []
    for privilege in ("CATALOG_MANAGE_ACCESS", "CATALOG_MANAGE_METADATA"):
        key = ("catalog", (), privilege)
        if key in existing:
            continue
        response = requests.put(
            grants_url,
            headers=headers,
            json={"grant": {"type": "catalog", "privilege": privilege}},
            timeout=15,
        )
        response.raise_for_status()
        added.append(privilege)

    assigned_url = f"{management_url}/principal-roles/{principal_role}/catalog-roles/{catalog}"
    assigned = response_json(requests.get(assigned_url, headers=headers, timeout=15)).get(
        "roles", []
    )
    if not any(isinstance(item, dict) and item.get("name") == role for item in assigned):
        response = requests.put(
            assigned_url,
            headers=headers,
            json={"catalogRole": {"name": role}},
            timeout=15,
        )
        response.raise_for_status()

    headers = access_headers(base_url, client_id, client_secret)
    namespaces_url = f"{base_url}/api/catalog/v1/{catalog}/namespaces"
    namespaces = response_json(requests.get(namespaces_url, headers=headers, timeout=15)).get(
        "namespaces", []
    )
    if [namespace] not in namespaces:
        response = requests.post(
            namespaces_url,
            headers=headers,
            json={"namespace": [namespace], "properties": {}},
            timeout=15,
        )
        response.raise_for_status()

    current = response_json(requests.get(grants_url, headers=headers, timeout=15)).get("grants", [])
    existing = {
        (grant.get("type"), tuple(grant.get("namespace", [])), grant.get("privilege"))
        for grant in current
        if isinstance(grant, dict)
    }
    for privilege in ("TABLE_READ_DATA", "TABLE_WRITE_DATA"):
        key = ("namespace", (namespace,), privilege)
        if key in existing:
            continue
        response = requests.put(
            grants_url,
            headers=headers,
            json={
                "grant": {
                    "type": "namespace",
                    "namespace": [namespace],
                    "privilege": privilege,
                }
            },
            timeout=15,
        )
        response.raise_for_status()
        added.append(privilege)

    print(
        json.dumps(
            {
                "catalog": catalog,
                "namespace": namespace,
                "catalog_role": role,
                "catalog_created": catalog_created,
                "added_grants": added,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()

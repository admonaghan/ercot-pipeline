from typing import Any
import requests

import dlt
from rest_api import (
    RESTAPIConfig,
    check_connection,
    rest_api_source,
    rest_api_resources,
)


def return_bearer_token(
    username: str = dlt.secrets.value,
    password: str = dlt.secrets.value,
    authentication_url: str = dlt.secrets.value,
):
    auth_response = requests.post(
        authentication_url.format(username=username, password=password)
    )
    return auth_response.json().get()


@dlt.source
def ercot_source(bearer_token: str, subscription_key: str = dlt.secrets.value) -> Any:
    # Create a REST API configuration
    # Use RESTAPIConfig to get autocompletion and type checking
    config: RESTAPIConfig = {
        "client": {
            "base_url": "https://api.ercot.com/api/public-reports/",
            "headers": {
                "Ocp-Apim-Subscription-Key": subscription_key,
            },
            "auth": {
                "type": "bearer",
                "token": bearer_token,
            },
        },
        # The default configuration for all resources and their endpoints
        "resource_defaults": {
            "primary_key": "id",
            "write_disposition": "replace",
            "endpoint": {
                "params": {
                    "per_page": 100,
                },
            },
        },
        "resources": [
            # This is a simple resource definition,
            # that uses the endpoint path as a resource name:
            # "pulls",
            # Alternatively, you can define the endpoint as a dictionary
            # {
            #     "name": "pulls", # <- Name of the resource
            #     "endpoint": "pulls",  # <- This is the endpoint path
            # }
            # Or use a more detailed configuration:
            {
                "name": "solar_production_5_min_avg",
                "endpoint": {
                    "path": "np4-746-cd/spp_actual_5min_avg_values_geo",
                    # Query parameters for the endpoint
                    "params": {
                        "intervalEndingFrom": "2024-08-01T00:00:00",
                        "intervalEndingTo": "2024-09-01T00:00:00",
                    },
                },
            },
            # The following is an example of a resource that uses
            # a parent resource (`issues`) to get the `issue_number`
            # and include it in the endpoint path:
            {
                "name": "issue_comments",
                "endpoint": {
                    # The placeholder {issue_number} will be resolved
                    # from the parent resource
                    "path": "issues/{issue_number}/comments",
                    "params": {
                        # The value of `issue_number` will be taken
                        # from the `number` field in the `issues` resource
                        "issue_number": {
                            "type": "resolve",
                            "resource": "issues",
                            "field": "number",
                        }
                    },
                },
                # Include data from `id` field of the parent resource
                # in the child data. The field name in the child data
                # will be called `_issues_id` (_{resource_name}_{field_name})
                "include_from_parent": ["id"],
            },
        ],
    }

    yield from rest_api_resources(config)


def load_github() -> None:
    bearer_token = return_bearer_token()

    pipeline = dlt.pipeline(
        pipeline_name="rest_api_github",
        destination="duckdb",
        dataset_name="rest_api_data",
    )
    load_info = pipeline.run(ercot_source(bearer_token))

    print(load_info)


def load_pokemon() -> None:
    pipeline = dlt.pipeline(
        pipeline_name="rest_api_pokemon",
        destination="duckdb",
        dataset_name="rest_api_data",
    )

    pokemon_source = rest_api_source(
        {
            "client": {
                "base_url": "https://pokeapi.co/api/v2/",
                # If you leave out the paginator, it will be inferred from the API:
                # paginator: "json_link",
            },
            "resource_defaults": {
                "endpoint": {
                    "params": {
                        "limit": 1000,
                    },
                },
            },
            "resources": [
                "pokemon",
                "berry",
                "location",
            ],
        }
    )

    def check_network_and_authentication() -> None:
        (can_connect, error_msg) = check_connection(
            pokemon_source,
            "not_existing_endpoint",
        )
        if not can_connect:
            pass  # do something with the error message

    check_network_and_authentication()

    load_info = pipeline.run(pokemon_source)
    print(load_info)


if __name__ == "__main__":
    load_github()
    load_pokemon()

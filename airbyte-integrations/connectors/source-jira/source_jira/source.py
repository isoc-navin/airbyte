#
# Copyright (c) 2024 Airbyte, Inc., all rights reserved.
#
from datetime import datetime, timedelta
from logging import Logger
from typing import Any, List, Mapping, Optional, Tuple

import pendulum
import pytz
from pydantic import ValidationError
from requests.exceptions import InvalidURL

from airbyte_cdk.models import ConfiguredAirbyteCatalog, FailureType
from airbyte_cdk.sources.declarative.exceptions import ReadException
from airbyte_cdk.sources.declarative.yaml_declarative_source import YamlDeclarativeSource
from airbyte_cdk.sources.source import TState
from airbyte_cdk.sources.streams.core import Stream
from airbyte_cdk.sources.streams.http.requests_native_auth import BasicHttpAuthenticator
from airbyte_cdk.utils.traced_exception import AirbyteTracedException
from datetime import datetime, timedelta
from .streams import IssueFields, Issues, PullRequests
from .utils import read_full_refresh
import psycopg2

class SourceJira(YamlDeclarativeSource):
    def __init__(self, catalog: Optional[ConfiguredAirbyteCatalog], config: Optional[Mapping[str, Any]], state: TState, **kwargs):
        super().__init__(catalog=catalog, config=config, state=state, **{"path_to_yaml": "manifest.yaml"})

        self.config = config
        est_tz = pytz.timezone('US/Eastern')
        backfill_clearance = config.get("backfill_clearance", "None")

        # Handle start_date logic only for specific backfill_clearance values
        if backfill_clearance in ["last_7_days_refresh", "last_15_days_refresh", "last_30_days_refresh"]:
            middle_part = backfill_clearance.split("_")[1]
            numeric_part = ''.join(char for char in middle_part if char.isdigit())  # Explicit generator expression
            days = int(numeric_part)
            today = datetime.now(est_tz)
            self.date_start = (today - timedelta(days=days)).strftime("%Y-%m-%d")
            self.date_stop = today.strftime("%Y-%m-%d")
        else:
            # Default fallback if backfill_clearance does not match
            self.date_start = self.config["start_date"]
            self.date_stop = self.config["end_date"]

        self.backfill_date_start = self.config['backfill_date_start']
        self.backfill_date_stop = self.config['backfill_date_stop']
    def check_connection(self, logger: Logger, config: Mapping[str, Any]) -> Tuple[bool, any]:
        try:
            backfill_clearance = config.get("backfill_clearance", "None")
            database = config.get("database")
            username = config.get("username")
            password = config.get("password")
            host = config.get("host")
            port = config.get("port")
            schema = config.get("schema")
            table_name = config.get("table_name")

            if backfill_clearance in ["last_7_days_refresh", "last_15_days_refresh", "last_30_days_refresh"]:
                # Calculate the date range for backfill clearance
                middle_part = backfill_clearance.split("_")[1]
                numeric_part = ''.join(char for char in middle_part if char.isdigit())  # Explicit generator expression
                days = int(numeric_part)
                est_tz = pytz.timezone('US/Eastern')
                today = datetime.now(est_tz)
                clear_start_date = (today - timedelta(days=days)).strftime("%Y-%m-%d")
                clear_end_date = today.strftime("%Y-%m-%d")

                try:
                    # Connect to Redshift
                    connection = psycopg2.connect(
                        dbname=database,
                        user=username,
                        password=password,
                        host=host,
                        port=port
                    )
                    cursor = connection.cursor()

                    # Build and execute the delete query
                    delete_query = f"""
                        DELETE FROM {schema}.{table_name}
                        WHERE date_start BETWEEN '{clear_start_date}' AND '{clear_end_date}';
                    """
                    cursor.execute(delete_query)
                    connection.commit()
                    print(f"Data cleared in Redshift with query: {delete_query}")

                except Exception as e:
                    print(f"Error during Redshift backfill operation: {e}")
                    raise
                finally:
                    if cursor:
                        cursor.close()
                    if connection:
                        connection.close()

            # Proceed with the remaining connection checks
            streams = self.streams(config)
            stream_name_to_stream = {s.name: s for s in streams}

            # check projects
            if config.get("projects"):
                projects_stream = stream_name_to_stream["projects"]
                actual_projects = {project["key"] for project in read_full_refresh(projects_stream)}
                unknown_projects = set(config["projects"]) - actual_projects
                if unknown_projects:
                    return False, "unknown project(s): " + ", ".join(unknown_projects)

            # Get streams to check access to any of them
            for stream_name in self._source_config["check"]["stream_names"]:
                try:
                    next(read_full_refresh(stream_name_to_stream[stream_name]), None)
                except:
                    logger.warning(f"No access to stream: {stream_name}")
                else:
                    logger.info(f"API Token have access to stream: {stream_name}, so check is successful.")
                    return True, None
            return False, "This API Token does not have permission to read any of the resources."
        except ValidationError as e:
            return False, e
        except (AirbyteTracedException, ReadException, InvalidURL) as e:
            if isinstance(e, InvalidURL) or "404" in str(e) or (isinstance(e, AirbyteTracedException) and "Not found" in e.message):
                raise AirbyteTracedException(
                    message="Config validation error: please check that your domain is valid and does not include protocol (e.g: https://).",
                    internal_message=str(e),
                    failure_type=FailureType.config_error,
                ) from None
            raise e

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        streams = super().streams(config)
        return streams + self.get_non_portable_streams(config=config)

    def _validate_and_transform_config(self, config: Mapping[str, Any]):
        start_date = config.get("start_date")
        if start_date:
            config["start_date"] = pendulum.parse(start_date)
        config["lookback_window_minutes"] = pendulum.duration(minutes=config.get("lookback_window_minutes", 0))
        config["projects"] = config.get("projects", [])
        return config

    @staticmethod
    def get_authenticator(config: Mapping[str, Any]):
        return BasicHttpAuthenticator(config.get("email"), config["api_token"])

    def get_non_portable_streams(self, config: Mapping[str, Any]) -> List[Stream]:
        config = self._validate_and_transform_config(config.copy())
        authenticator = self.get_authenticator(config)
        args = {"authenticator": authenticator, "domain": config.get("domain"), "projects": config["projects"]}
        incremental_args = {
            **args,
            "start_date": config.get("start_date"),
            "lookback_window_minutes": config.get("lookback_window_minutes"),
        }
        issues_stream = Issues(**incremental_args)
        issue_fields_stream = IssueFields(**args)

        experimental_streams = []
        if config.get("enable_experimental_streams", False):
            experimental_streams.append(
                PullRequests(issues_stream=issues_stream, issue_fields_stream=issue_fields_stream, **incremental_args)
            )
        return experimental_streams

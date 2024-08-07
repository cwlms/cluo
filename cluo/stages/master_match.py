import json
from datetime import datetime
from textwrap import dedent
from typing import Any, List

import psycopg2
import psycopg2.extras
from dateparser import parse
from psycopg2 import sql
from psycopg2._psycopg import connection

from cluo.connections import PostgresConnectionPool
from cluo.core import ErrorHandlingMethod, Record, RecordData, Stage


class MasterMatchStage(Stage):
    """Looks up existing master table records for a given Record."""

    def __init__(
        self,
        master_table: str,
        connection_pool: PostgresConnectionPool,
        record_data_source_key_field: str = "data_source_key",
        match_ratio: float = 0.1,
        timestamp_fields: List[str] | None = ["ts_updated"],
        date_fields: List[str] | None = None,
        sort_fields: List[str] | None = ["ts_updated"],
        sort_reverse: bool = True,
        name: str | None = None,
        processes: int = 1,
        error_handling_method: ErrorHandlingMethod = ErrorHandlingMethod.DEFAULT,
        expose_metrics: bool = False,
    ) -> None:
        """Initialize the `MasterMatchStage`.

        Args:
            master_table (str): The name of the master table on which to perform match.
            connection_pool (PostgresConnectionPool): Connection pool to use connections from for matching.
            record_data_source_key_field (str, optional): The name of the field in the record data that contains the data source key. Defaults to `"data_source_key"`.
            match_ratio (float): The ratio of hash matches required to consider a record a match. 1 is all hashes match, or record already in master, 0.5 would be half the hashes match, etc.
            timestamp_fields (List[str], optional): list of fields in timestamp format.  parsed from strings to python datetimes.  Defaults to `["ts_updated"]`
            date_fields (List[str], optional): list of fields in date format.  parsed from strings to python dates.  Defaults to `None`
            sort_fields (List[str], optional): list of fields to use in sorting match results.  sorts the returned matches by values in these fields. Sorting is not applied when empty or None. Defaults to `["ts_updated"]`
            sort_reverse (bool): sort matches in reverse order.  Applies to all sort keys. Only used when sort_fields also present. Defaults to `True`.
            name (str, optional): Stage name. Defaults to class name if name = None.
            processes (int, optional): Number of CPUs to use. Can only be used if .process_record method is implemented. Defaults to 1.
            error_handling_method (ErrorHandlingMethod, optional): Enum that represents how the stage would like the pipeline to handle errors which occur when running this stage. By default, errors will be raised.
            expose_metrics (bool, optional): Whether or not to expose metrics for this stage. Defaults to False.
        """
        Stage.__init__(
            self,
            name=name,
            processes=processes,
            error_handling_method=error_handling_method,
            expose_metrics=expose_metrics,
        )

        self.master_table = master_table
        self.connection_pool = connection_pool
        self.match_ratio = match_ratio
        self.record_data_source_key_field = record_data_source_key_field
        self.timestamp_fields = timestamp_fields
        self.date_fields = date_fields
        self.sort_fields = sort_fields
        self.sort_reverse = sort_reverse

    def _validate_lookup_results(
        self, results: list[psycopg2.extras.RealDictRow]
    ) -> None:
        if len(results) != 1:
            raise Exception(
                f"Lookup query must return exactly one row. Got {len(results)} rows."
            )

    def _date_from_postgres_str(self, ts_str: Any) -> Any:
        retval = ts_str
        if isinstance(retval, str):
            retval = parse(retval)
            if isinstance(retval, datetime):
                retval = retval.date()
        return retval

    def _timestamp_from_postgres_str(self, ts_str: Any) -> Any:
        return parse(ts_str) if isinstance(ts_str, str) else ts_str

    def _post_process_matches(self, matches: List[dict]) -> List[dict]:
        retval = []
        for match in matches:
            for key, value in match.items():
                if self.date_fields and key in self.date_fields:
                    match[key] = self._date_from_postgres_str(value)
                elif self.timestamp_fields and key in self.timestamp_fields:
                    match[key] = self._timestamp_from_postgres_str(value)
                else:
                    match[key] = value
            retval.append(match)
        if self.sort_fields:
            # when sort_reverse = True, descending on all keys, nulls last - default config
            # when sort_reverse = False, ascending on all keys, nulls last
            # generalized null handling for sorting: https://stackoverflow.com/a/72138073
            def _sort_func(x):
                return (
                    [(x.get(key) is not None, x.get(key)) for key in self.sort_fields]
                    if self.sort_reverse
                    else [(x.get(key) is None, x.get(key)) for key in self.sort_fields]
                )

            retval = sorted(retval, key=_sort_func, reverse=self.sort_reverse)
        return retval

    def _process_lookup_results(
        self, results: list[psycopg2.extras.RealDictRow]
    ) -> dict[str, Any]:
        self._validate_lookup_results(results)
        return dict(results[0])

    def _do_lookup(
        self, conn: connection, query: str, record_data: RecordData
    ) -> dict[str, Any]:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cursor.execute(query, record_data)
        results = cursor.fetchall()
        return self._process_lookup_results(results)

    def process_record(self, record: Record) -> Record:
        conn = self.connection_pool.getconn()
        try:
            lookup_results = self._do_lookup(
                conn, self._construct_query(record), record.data
            )
        finally:
            self.connection_pool.putconn(conn)
        matches = lookup_results["matches"]
        record.data["matches"] = json.loads(matches) if matches is not None else []
        record.data["matches"] = self._post_process_matches(record.data["matches"])
        return record

    def _construct_query(self, record: Record) -> sql.SQL:
        return sql.SQL(
            dedent(
                """
                WITH all_matches AS (
                    WITH master_matches AS (
                        WITH master_record AS (
                            SELECT
                                *
                            FROM
                                master.{master_table_name}
                            WHERE
                                array[{record_data_source_key}::text] && data_source_keys
                        ) -- end master_record
                        SELECT
                            cl.*, 1 AS MATCH
                        FROM
                            master_record mr
                        JOIN clean.{master_table_name} cl ON
                            cl.data_source_key = ANY (data_source_keys)
                    ) -- end master_matches
                    ,hash_matches AS (
                        WITH incoming AS (
                            SELECT
                                array_hash
                            FROM
                                clean.{master_table_name}
                            WHERE
                                data_source_key = {record_data_source_key}
                        )
                        SELECT
                            id, CARDINALITY( ARRAY ( SELECT UNNEST(searching.array_hash) INTERSECT SELECT UNNEST(incoming.array_hash) ))::decimal / CARDINALITY(incoming.array_hash) AS MATCH
                        FROM
                            clean.{master_table_name} searching, incoming
                        WHERE
                            searching.array_hash && incoming.array_hash
                    ) -- end hash_matches
                    SELECT
                        mm.id
                    FROM
                        master_matches mm
                    UNION ALL
                    SELECT
                        hm.id
                    FROM
                        hash_matches hm
                    WHERE
                        MATCH >= {match_ratio}
                        -- MATCH RATIO (1 is already in master, or all hashes match, 0.5 would be half the hashes match, etc)
                    UNION ALL
                    SELECT
                        self_m.id
                    FROM
                        clean.{master_table_name} self_m
                    WHERE
                        data_source_key = {record_data_source_key}
                )  -- end all_matches
                SELECT
                    JSON_AGG(DISTINCT(cl.*))::TEXT AS matches
                FROM
                    all_matches am
                JOIN clean.{master_table_name} cl ON
                    am.id = cl.id
                WHERE cl.ts_status IS NULL OR cl.ts_status != 'DELETED'
        """
            )
        ).format(
            record_data_source_key=sql.Literal(
                record.data[self.record_data_source_key_field]
            ),
            master_table_name=sql.Identifier(self.master_table),
            match_ratio=sql.Literal(self.match_ratio),
        )

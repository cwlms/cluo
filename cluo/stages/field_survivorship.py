import datetime
from collections import Counter
from typing import Any, Literal, Tuple, Union

from cluo.core import ErrorHandlingMethod, Record, Stage
from cluo.utils.hashing import hash_nested_primitive


class FieldSurvivorshipStage(Stage):
    """Survive data by field according to survivorship rules."""

    # TODO: Remove
    default_order = [
        "data_steward",
        "salesforce",
        "aim",
        "pitchbook",
        "crunchbase",
        "affinity",
        "f6s",
        "airtable_investors",
        "paperstreet",
        "ltse",
    ]

    def __init__(
        self,
        config: dict[
            Union[str, Tuple[str]],
            dict[Literal["rule", "order"], Union[str, list[str]]],
        ],
        name: str | None = None,
        processes: int = 1,
        error_handling_method: ErrorHandlingMethod = ErrorHandlingMethod.DEFAULT,
        expose_metrics: bool = False,
    ) -> None:
        """Initialize the `FieldSurvivorshipStage`.

        Args:
            config: (dict): Configuration for the stage. Maps field names or tuples of field names to survivorship rules (and source order for `"source_priority"` rule.)  Tuple-keyed rules survive all the fields in the tuple as a group.  All fields are survived from the same match record.  Match records qualify if at least one of the record's tuple field values is non-empty.  They are not required to have non-empty values for all tuple fields.
            name (str, optional): Stage name. Defaults to class name if name = None.
            processes (int, optional): Number of CPUs to use. Can only be used if .process_record method is implemented. Defaults to 1.
            error_handling_method (ErrorHandlingMethod, optional): Enum that represents how the stage would like the pipeline to handle errors which occur when running this stage. By default, errors will be raised.
            expose_metrics (bool, optional): Whether or not to expose metrics for this stage. Defaults to False.

        example config
                ```py
        {
            "company_name": {
                "rule": "source_priority",
                "order": ["aim", "salesforce", "crunchbase"],
            },
            "data_source_key": {
                "rule": "source_priority",
                # sources can be grouped together as tuples
                # records from grouped sources are ranked at the same level
                # and resolve to the most recent record from any of the grouped sources
                "order": ["affinity", ("aim", "salesforce", "bee"), "crunchbase"],
            },
            "investor_type": {"rule": "aggregate"},
            "all_categories": {"rule": "aggregate"},
            "state_province": {"rule": "recency", "include_nulls": False},
            "point_of_contact": {"rule": "frequency"},
            # survives all four fields as a group
            ("city", "state_province", "postal_code", "country_of_origin"): {
                "rule": "source_priority",
                "order": ["affinity", "aim", "salesforce", "crunchbase"],
            },
        }
                ```

        """
        Stage.__init__(
            self,
            name=name,
            processes=processes,
            error_handling_method=error_handling_method,
            expose_metrics=expose_metrics,
        )
        self.match_field = "matches"
        self.config = config
        # set after first call to process_record
        self.validated: bool = False
        self.accepted_rules: list[str] = [
            "source_priority",
            "recency",
            "frequency",
            "aggregate",
        ]

    def _validate_config(self) -> None:
        # config must have at least one rule
        def _check_nonempty_config() -> None:
            if not self.config:
                raise ValueError("survivorship config must have at least one rule")

        # config cannot have more than one rule per field
        def _flatten_list(list_of_lists: list[str | list[str]]) -> list[str]:
            return [item for sublist in list_of_lists for item in sublist]

        def _get_duplicates(key_list: list[str]) -> list[str]:
            return [item for item, count in Counter(key_list).items() if count > 1]

        def _invalid_order(order_list: Union[str, list[str], list[Any]]) -> bool:
            # invalid order if empty list or any key is not a string or a tuple of strings
            order_list = list(order_list) if isinstance(order_list, str) else order_list
            return (not order_list) or len(
                [key for key in order_list if not isinstance(key, (tuple, str))]
            ) != 0

        def _check_duplicate_keys() -> None:
            if duplicate_keys := _get_duplicates(
                _flatten_list(
                    [
                        [key] if isinstance(key, str) else list(key)
                        for key in self.config
                    ]
                )
            ):
                raise ValueError(
                    f"survivorship config must have no more than one rule per field.  multiple rules specified for {list(duplicate_keys)}"
                )

        def _check_aggregate_rules() -> None:
            # aggregate survivorship rule must not target more than one field
            if list_sourced_aggregates := [
                key
                for key, value in self.config.items()
                if value.get("rule") == "aggregate" and isinstance(key, tuple)
            ]:
                raise ValueError(
                    f"aggregate survivorship rules cannot be configured for field groups: {list_sourced_aggregates}"
                )

        def _check_source_priority_rules() -> None:
            if bad_source_priority_keys := [
                key
                for key, value in self.config.items()
                if value.get("rule") == "source_priority"
                and _invalid_order(value.get("order", []))
            ]:
                raise ValueError(
                    f'source_priority survivorship rules must include a list of data sources in "order" key: {bad_source_priority_keys}'
                )

        def _check_recency_rules() -> None:
            if bad_recency_keys := [
                key
                for key, value in self.config.items()
                if value.get("rule") == "recency"
                and not isinstance(value.get("include_nulls", False), bool)  # type: ignore
            ]:
                raise ValueError(
                    f'recency survivorship key "include_nulls" must supply a boolean value: {bad_recency_keys}'
                )

        def _check_valid_rules() -> None:
            if invalid_rule_keys := [
                (key, value.get("rule"))
                for key, value in self.config.items()
                if value.get("rule") not in self.accepted_rules
            ]:
                raise ValueError(
                    f"survivorship config includes invalid rule type(s): {invalid_rule_keys}"
                )

        _check_nonempty_config()
        _check_duplicate_keys()
        _check_aggregate_rules()
        _check_source_priority_rules()
        _check_recency_rules()
        _check_valid_rules()
        self.validated = True

    def _collect_fields_from_match_and_records(
        self,
        record: Record,
        any_fields: list[str],
        required_fields: list[str] = [],
        include_nulls: bool = False,
    ):
        """
        to qualify:
            - a record must include keys for all values in any_fields + required_fields
            - a record must have at least one non-empty value from any_fields
            - a record must have non-empty values for all fields in required_fields
        """

        def _values_are_populated(
            match: dict[str, Any],
            fields: list[str],
            required_fields: list[str],
            include_nulls: bool,
        ) -> bool:
            return any(
                (value is not None or include_nulls)
                for value in (match[field] for field in fields)
            ) and (
                not required_fields
                or all(
                    value is not None
                    for value in (match[field] for field in required_fields)
                )
            )

        # matches qualify if any of the fields in the list are populated
        all_data = record.data["matches"]
        options = []
        all_fields = set(any_fields + required_fields)
        for match in all_data:
            if all_fields.issubset(set(match.keys())) and _values_are_populated(
                match, any_fields, required_fields, include_nulls
            ):
                options.append({field: match[field] for field in all_fields})
        return options

    def _get_most_frequent_value(self, value_list: list[Any]) -> Any:
        if not value_list:
            return []
        hashed_pairs = [(hash_nested_primitive(value), value) for value in value_list]
        hash_lookup = dict(hashed_pairs)
        hashed_list = [item[0] for item in hashed_pairs]
        return hash_lookup.get(Counter(hashed_list).most_common(1)[0][0])

    def _get_most_recent_value(self, value_list: list[Any]) -> Any:
        if not value_list:
            return None
        return value_list[0]

    def _flatten_source_order(self, source_order: list[Union[str, tuple]]) -> list[str]:
        retval: list[str] = []
        for source in source_order:
            if isinstance(source, tuple):
                retval = retval + list(source)
            else:
                retval.append(source)
        return retval

    def _rank_source_order(
        self, source_order: list[Union[str, tuple]]
    ) -> dict[str, int]:
        # build the source order rank dictionary: keys = data_source, values = relative rank
        retval: dict[str, int] = {}
        for source_rank, source in enumerate(source_order):
            if isinstance(source, tuple):
                for sub_source in source:
                    retval[sub_source] = source_rank
            else:
                retval[source] = source_rank
        return retval

    def _get_value_by_source_priority(
        self,
        record: Record,
        field_list: list[str],
        source_order: list[Union[str, tuple]],
    ) -> dict[str, Any]:
        options = self._collect_fields_from_match_and_records(
            record,
            field_list,
            required_fields=["data_source"],
        )
        # flattened source order list for filtering
        flat_source_order = self._flatten_source_order(source_order)
        filtered = [
            option for option in options if option["data_source"] in flat_source_order
        ]
        # source order rank by data source for sorting
        rank_by_source = self._rank_source_order(source_order)
        sorted_options = sorted(
            filtered, key=lambda x: rank_by_source[x["data_source"]]
        )
        # if no registered sources have data null out the field
        if len(sorted_options) == 0:
            return {key: None for key in field_list}
        best_match_data_source = sorted_options[0]["data_source"]
        most_recent_value_list = self._get_most_recent_value(
            [
                [option[field_name] for field_name in field_list]
                for option in options
                if option["data_source"] == best_match_data_source
            ]
        )
        return dict(zip(field_list, most_recent_value_list))

    def _get_value_by_recency(
        self, record: Record, field_list: list[str], include_nulls: bool = False
    ) -> dict[str, Any]:
        STRPTIME_FORMAT = "%Y-%m-%dT%H:%M:%S.%f"
        options = self._collect_fields_from_match_and_records(
            record,
            field_list,
            # TODO: Genericize
            required_fields=["ts_updated"],
            include_nulls=include_nulls,
        )
        for option in options:
            option.update(
                {
                    "ts_updated": datetime.datetime.strptime(
                        option["ts_updated"], STRPTIME_FORMAT
                    )
                    if isinstance(option["ts_updated"], str)
                    else option["ts_updated"]
                }
            )
        sorted_options = sorted(options, key=lambda x: x["ts_updated"], reverse=True)
        # if no sources have data null out the field
        if len(sorted_options) == 0:
            return {key: None for key in field_list}
        return {key: sorted_options[0].get(key) for key in field_list}

    def _get_value_by_frequency(
        self, record: Record, field_list: list[str]
    ) -> dict[str, Any]:
        options = self._collect_fields_from_match_and_records(record, field_list)
        most_frequent_value_list = self._get_most_frequent_value(
            [[option[field_name] for field_name in field_list] for option in options]
        )
        return dict(zip(field_list, most_frequent_value_list))

    def _get_value_by_aggregation(
        self, record: Record, field_list: list[str]
    ) -> dict[str, Any]:
        collected = []
        options = self._collect_fields_from_match_and_records(record, field_list)
        for option in options:
            value = option[field_list[0]]
            if isinstance(value, list):
                collected.extend(value)
            elif isinstance(value, tuple):
                collected.extend(list(value))
            else:
                collected.append(value)
        return {field_list[0]: sorted(list(set(collected)))}

    def _process_single_field(
        self,
        record: Record,
        field_list: list[str],
        rule: Union[str, list[str]],
        order: list[str] | None = default_order,
        include_nulls: bool = False,
    ) -> Any:
        if rule == "source_priority":
            return self._get_value_by_source_priority(record, field_list, order)  # type: ignore
        elif rule == "recency":
            return self._get_value_by_recency(
                record, field_list, include_nulls=include_nulls
            )
        elif rule == "frequency":
            return self._get_value_by_frequency(record, field_list)
        elif rule == "aggregate":
            return self._get_value_by_aggregation(record, field_list)
        else:
            raise ValueError(f"Unknown rule: {rule}")

    def process_record(self, record: Record) -> Record:
        if not self.validated:
            self._validate_config()

        for field_list_raw, sub_config in self.config.items():
            field_list = (
                [field_list_raw]
                if isinstance(field_list_raw, str)
                else list(field_list_raw)
            )
            record.data = record.data | self._process_single_field(
                record=record,
                field_list=field_list,
                rule=sub_config["rule"],
                order=sub_config["order"]  # type: ignore
                if sub_config["rule"] == "source_priority"
                else None,
                include_nulls=sub_config.get("include_nulls", False),  # type: ignore
            )
        return record

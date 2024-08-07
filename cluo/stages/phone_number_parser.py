from typing import Any, Iterable, Literal

import phonenumbers

from cluo.core import ErrorHandlingMethod
from cluo.extensions.field_processor_stage import FieldProcessorStage


class PhoneNumberParserStage(FieldProcessorStage):
    """Parse and format phone numbers if possible, else raise exception or provide default. Tries to parse with E.164 format, if not, tries to parse with default country code of US."""

    def __init__(
        self,
        fields: Iterable[str] | None = None,
        allow_nulls: bool = True,
        on_error: Literal["raise", "default"] = "raise",
        name: str | None = None,
        processes: int = 1,
        error_handling_method: ErrorHandlingMethod = ErrorHandlingMethod.DEFAULT,
        expose_metrics: bool = False,
    ) -> None:
        """Initialize the `PhoneNumberParserStage`.

        Args:
            fields (Iterable[str], optional): Fields to operate on. Will operate on all fields if not set (None).
            allow_nulls (bool, optional): Allow null values/fields if True, otherwise use error_handling_method
            on_error ("raise" or "default", optional): What to do if a phone number cannot be parsed. Defaults to "raise".
            name (str, optional): Stage name. Defaults to class name if name = None.
            processes (int, optional): Number of CPUs to use. Can only be used if .process_record method is implemented. Defaults to 1.
            error_handling_method (ErrorHandlingMethod, optional): Enum that represents how the stage would like the pipeline to handle errors which occur when running this stage. By default, errors will be raised.
            expose_metrics (bool, optional): Whether or not to expose metrics for this stage. Defaults to False.
        """
        FieldProcessorStage.__init__(
            self,
            fields=fields,
            allow_nulls=allow_nulls,
            name=name,
            processes=processes,
            error_handling_method=error_handling_method,
            expose_metrics=expose_metrics,
        )
        self.on_error = on_error

    def _clean_phone_number(self, phone_number: str | int | None) -> str | None:
        if phone_number is None:
            return None
        elif isinstance(phone_number, int):
            return str(phone_number)
        elif isinstance(phone_number, str):
            return phone_number.strip()
        else:
            raise ValueError(
                "Phone number must be a string or integer. Unable to parse"
            )

    def _parse_single_phone_number(
        self, phone_number: str | int | None
    ) -> tuple[str | None, bool]:
        phone_number = self._clean_phone_number(phone_number)
        if phone_number is None:
            return None, True
        is_in_e164_format = phone_number[0] == "+"
        try:
            if is_in_e164_format:
                parsed = phonenumbers.parse(phone_number, None)
            else:
                parsed = phonenumbers.parse(phone_number, "US")
        except phonenumbers.phonenumberutil.NumberParseException:
            return None, True
        return (
            (self._convert_to_e164_format(parsed), False)
            if phonenumbers.is_possible_number(parsed)
            else (None, True)
        )

    def _convert_to_e164_format(
        self, parsed: phonenumbers.phonenumber.PhoneNumber
    ) -> str:
        return f"+{parsed.country_code}{parsed.national_number}"

    def process_field(self, field: Any, field_value: Any) -> Any:
        parsed, err = self._parse_single_phone_number(field_value)
        if err and self.on_error == "raise":
            raise ValueError(
                f"Unable to parse phone number in field `{field}`:" f" {field_value}"
            )
        return parsed

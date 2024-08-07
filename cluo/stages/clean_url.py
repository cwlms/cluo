import re
from typing import Any, Iterable
from urllib.parse import urlparse

from cluo.core import ErrorHandlingMethod
from cluo.extensions.field_processor_stage import FieldProcessorStage


class CleanUrlStage(FieldProcessorStage):
    """Parse and standardize URL format."""

    def __init__(
        self,
        fields: Iterable[str] | None = None,
        allow_nulls: bool = True,
        include_protocol: bool = True,
        include_subdomain: bool = True,
        include_path: bool = True,
        include_query_string: bool = True,
        null_if_invalid: bool = False,
        name: str | None = None,
        processes: int = 1,
        error_handling_method: ErrorHandlingMethod = ErrorHandlingMethod.DEFAULT,
        expose_metrics: bool = False,
    ) -> None:
        """Initialize `CleanUrlStage`.

        Args:
            fields (Iterable[str], optional): Fields to operate on. Will operate on all fields if not set (None).
            allow_nulls (bool, optional): Allow null values/fields if True, otherwise use error_handling_method
            include_protocol (bool, optional): Specifies whether or not to include 'http(s)://'. Defaults to True.
            include_subdomain (bool, optional): Specifies whether or not to include subdomains 'subdomain.domain.com'. Defaults to True.
            include_path (bool, optional): Specifies whether or not to include URL path (including any query string). Defaults to True.
            include_query_string (bool, optional): Specifies whether or not to include any query string following the '?' character. Defaults to True. Ignored if path is excluded.
            null_if_invalid (bool, optional): If True, replaces invalid URLs with nulls instead of raising errors. Defaults to False.
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
        self.include_protocol = include_protocol
        self.include_subdomain = include_subdomain
        self.include_path = include_path
        self.include_query_string = include_query_string
        self.null_if_invalid = null_if_invalid

    def process_field(self, field: Any, field_value: Any) -> Any:
        return self.clean_url(field_value)

    def _get_protocol(self, parsed_url) -> str:
        if parsed_url.scheme:
            return f"{parsed_url.scheme}://"
        else:
            raise Exception("URL contains no protocol")

    def _get_domain(self, parsed_url) -> str:
        full_domain = parsed_url.netloc
        if re.compile(".+\\..+").match(full_domain):
            if self.include_subdomain:
                return full_domain
            else:
                return full_domain[
                    full_domain.rfind(".", 0, full_domain.rfind(".")) + 1 :
                ]
        else:
            raise Exception("URL contains no valid domain")

    def _get_path(self, parsed_url) -> str:
        path = parsed_url.path
        if path and path[-1] == "/":
            # Trim trailing slash
            path = path[:-1]
        return path

    def _get_query_string(self, parsed_url) -> str:
        return f"?{parsed_url.query}" if parsed_url.query else ""

    def clean_url(
        self,
        url: str,
    ) -> str | None:
        """Clean a URL string

        Args:
            url (str): The raw URL string.

        Raises:
            Exception: Raised if `include_protocol` is True but no protocol is found in `url`.
            Exception: Raised if `url` contains no domain (e.g. 'example.com').

        Returns:
            str: Cleaned URL string
        """
        parsed_url = (
            urlparse(url.strip()) if "//" in url else urlparse(f"//{url.strip()}")
        )  # If no protocol, adding '//' parses into netloc instead of path
        cleaned_url = ""

        try:
            if self.include_protocol:
                cleaned_url += self._get_protocol(parsed_url)

            cleaned_url += self._get_domain(parsed_url)
        except Exception as e:
            if self.null_if_invalid:
                return None
            else:
                raise Exception(e)

        if self.include_path:
            path = self._get_path(parsed_url)
            cleaned_url += path
            if self.include_query_string:
                cleaned_url += self._get_query_string(parsed_url)

        return cleaned_url

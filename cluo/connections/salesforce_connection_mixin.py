from functools import cached_property

from simple_salesforce import Salesforce


class SalesforceConnectionMixin:
    """Mixin class for providing access to a Salesforce instance."""

    def __init__(
        self,
        username: str | None = None,
        password: str | None = None,
        security_token: str | None = None,
        domain: str | None = None,
        version: str | None = None,
    ) -> None:
        """
        Initialize the `SalesforceConnectionMixin`. **This is a private method for subclass use. Subclasses should copy-paste this `__init__` documentation.**

        Args:
            username (str): Username.
            password (str): Password.
            security_token (str): Security token.
            domain (str): Domain.
            version (str): Salesforce API version.
        """
        self.username = username
        self.password = password
        self.security_token = security_token
        self.domain = domain
        self.version = version

    @cached_property
    def sf(self) -> Salesforce:
        """Salesforce connection object. Generated on first access, then cached."""
        return (
            Salesforce(
                username=self.username,
                password=self.password,
                security_token=self.security_token,
                domain=self.domain,
                version=self.version,
            )
            if self.version
            else Salesforce(
                username=self.username,
                password=self.password,
                security_token=self.security_token,
                domain=self.domain,
            )
        )

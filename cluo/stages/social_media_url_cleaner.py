import re
from typing import Any, Callable, Literal

from cluo.core import ErrorHandlingMethod, Record, SocialMediaUrlError, Stage


class SocialMediaUrlCleanerStage(Stage):
    """Cleans Social media urls to the desired format"""

    def __init__(
        self,
        fields: dict[
            str,
            Literal[
                "angel",
                "facebook",
                "linkedin",
                "twitter",
                "pitchbook",
                "crunchbase",
                "paperstreet",
            ],
        ],
        null_on_error: bool = False,
        name: str | None = None,
        processes: int = 1,
        error_handling_method: ErrorHandlingMethod = ErrorHandlingMethod.DEFAULT,
        expose_metrics: bool = False,
    ) -> None:
        """Initialize the `SocialMediaUrlCleanerStage`.
        Args:
            fields (dict[str,Literal["angel", "facebook", "linkedin", "twitter", "pitchbook", "crunchbase", "paperstreet"]]): Social Media URL fields to clean and their corresponding cleaning method.
            null_on_error bool: DEFAULTs to False. Raise error
            name (str, optional): Stage name. Defaults to class name if name = None.
            processes (int, optional): Number of CPUs to use. Can only be used if process_record method is implemented. Defaults to 1.
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
        if any(val not in self.mappings.keys() for val in fields.values()):
            raise ValueError(
                "Cleaning method values must be in the following list: ['linkedin','angel','facebook','twitter','pitchbook','crunchbase','paperstreet']"
            )
        self.fields = fields
        self.null_on_error = null_on_error

    mappings: dict[str, dict[str, Any]] = {
        "angel": {
            "regex": "(?i)(?:https?:\\/\\/)?(?:(?:www|\\w\\w)\\.)?angel\\.co\\/([^?^#]*)",
            "callback": lambda match: f"angel.co/{match[1]}",
        },
        "facebook": {
            "regex": "(?:https?:\\/\\/)?(?:(?:www|\\w\\w)\\.)?(?:facebook\\.com|fb\\.me)\\/([^?^#]*)",
            "callback": lambda match: f"facebook.com/{match[1]}",
        },
        "linkedin": {
            "regex": "(?:https?:\\/\\/)?(?:(?:www|\\w\\w)\\.)?linkedin\\.(?:com|cn)\\/(?:(?:organization-guest|mwlite)\\/)?(company|companies|in|showcase|pub|groups|organization|school|edu|company-beta)\\/(?:([^?]*?)(?:\\/)|(.*))",
            "callback": lambda match: "linkedin.com/{}/{}".format(
                "company" if match[1] == "company-beta" else match[1],
                match[2] or match[3],
            ),
        },
        "twitter": {
            "regex": "(?:https?:\\/\\/)?(?:(?:www|\\w\\w)\\.)?twitter\\.com\\/([^?^#]*)",
            "callback": lambda match: f"twitter.com/{match[1]}",
        },
        "pitchbook": {
            "regex": "(?:https?:\\/\\/)?(?:(?:www|\\w\\w)\\.)?my\\.pitchbook\\.com\\/profile\\/([^?^#]*)",
            "callback": lambda match: f"my.pitchbook.com/profile/{match[1]}",
        },
        "crunchbase": {
            "regex": "(?:https?:\\/\\/)?(?:(?:www|\\w\\w)\\.)?crunchbase\\.com\\/(company|organization|financial-organization)\\/(?:([^?^#]*?)(?:\\/)|([^?]*))",
            "callback": lambda match: "crunchbase.com/organization/{}".format(
                match[2] or match[3]
            ),
        },
        "paperstreet": {
            "regex": "(?:https?:\\/\\/)?(?:(?:www|\\w\\w)\\.)?paperstreet\\.vc\\/([^?^#]*)",
            "callback": lambda match: f"paperstreet.vc/{match[1]}",
        },
    }

    def _get_social_media_url(
        self,
        og_url: str,
        search_val: str,
        match_regex: str,
        callback: Callable,
    ) -> str | None:
        if (
            og_url
            and search_val.upper() not in og_url.upper()
            and self.null_on_error
            or not og_url
        ):
            return None
        elif search_val.upper() not in og_url.upper():
            raise SocialMediaUrlError(
                stage_name="SocialMediaUrlCleanerStage",
                message=f"{og_url} is not a {search_val} URL, string:{search_val} not found in URL",
                handling_method=ErrorHandlingMethod.DEFAULT,
            )
        match = re.search(match_regex, og_url, flags=re.IGNORECASE)
        if match is not None:
            return callback(match)
        if self.null_on_error:
            return None
        else:
            raise SocialMediaUrlError(
                stage_name="SocialMediaUrlCleanerStage",
                message=f"No regex matches found in the provided URL:{og_url}, Check if valid {search_val} URL is passed",
                handling_method=ErrorHandlingMethod.DEFAULT,
            )

    def process_record(self, record: Record) -> Record:
        for key, val in self.fields.items():
            regex = self.mappings[val]["regex"]
            callback = self.mappings[val]["callback"]
            record.data[key] = self._get_social_media_url(
                record.data[key], val, regex, callback
            )
        return record

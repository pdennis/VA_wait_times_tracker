import json
from urllib.parse import urljoin

import requests

from ..config.settings import (
    API_FORMAT_CONGRESS_GOV,
    API_KEY_CONGRESS_GOV,
    API_URL_CONGRESS_GOV,
    API_VERSION_CONGRESS_GOV,
)


class _MethodWrapper:
    """Wrap request method to facilitate queries.  Supports requests signature."""

    # noinspection PyProtectedMember
    def __init__(self, parent, http_method):
        self._parent = parent
        self._method = getattr(parent._session, http_method)

    def __call__(self, endpoint, *args, **kwargs):  # full signature passed here
        response = self._method(urljoin(self._parent.base_url, endpoint), *args, **kwargs)
        # unpack
        if response.headers.get("content-type", "").startswith("application/json"):
            return response.json(), response.status_code
        else:
            return response.content, response.status_code


class CDGClient:
    """A sample client to interface with Congress.gov."""

    def __init__(
        self,
        api_key=API_KEY_CONGRESS_GOV,
        api_url=API_URL_CONGRESS_GOV,
        api_version=API_VERSION_CONGRESS_GOV,
        response_format=API_FORMAT_CONGRESS_GOV,
        raise_on_error=True,
    ):
        print(f"Using Congress.gov API key: {api_key}")
        self.base_url = urljoin(api_url, api_version) + "/"
        self._session = requests.Session()

        # do not use url parameters, even if offered, use headers
        self._session.params = {
            "api_key": api_key,
            "format": response_format,
        }
        # self._session.headers.update({"x-api-key": api_key})

        if raise_on_error:
            self._session.hooks = {"response": lambda r, *args, **kwargs: r.raise_for_status()}

    def __getattr__(self, method_name):
        """Find the session method dynamically and cache for later."""
        method = _MethodWrapper(self, method_name)
        self.__dict__[method_name] = method
        return method


class CongressGovApi:
    def __init__(
        self,
        api_key: str = API_KEY_CONGRESS_GOV,
        api_url: str = API_URL_CONGRESS_GOV,
        api_version=API_VERSION_CONGRESS_GOV,
        fmt: str = API_FORMAT_CONGRESS_GOV,
    ):
        super().__init__()
        self.api_key = api_key
        self.api_url = api_url
        self.api_version = api_version
        self.fmt = fmt

        self.client = CDGClient(api_key, api_url, api_version, fmt)

    def get_house_member(self, state: str, district: int) -> json:
        return self._get_house_member(state, district)

    def _get_house_member(self, state: str, district: int) -> json:
        endpoint = f"member/{state}/{district}?currentMember=True"
        data, status_code = self.client.get(endpoint)
        print(f"Status code: {status_code}; Data: {data}")
        return data

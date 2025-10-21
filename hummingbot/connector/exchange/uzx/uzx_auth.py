import base64
import hashlib
import hmac
import json
from collections import OrderedDict
from typing import Any, Dict
from urllib.parse import urlencode, urlparse

from hummingbot.connector.time_synchronizer import TimeSynchronizer
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTMethod, RESTRequest, WSRequest


class UzxAuth(AuthBase):
    def __init__(self, api_key: str, secret_key: str, api_passphrase: str, time_provider: TimeSynchronizer):
        self.api_key = api_key
        self.secret_key = secret_key
        self.api_passphrase = api_passphrase
        self.time_provider = time_provider

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        """
        Adds the server time and the signature to the request, required for authenticated interactions. It also adds
        the required parameter in the request header.
        :param request: the request to be configured for authenticated interaction
        """
        headers = {}
        if request.headers is not None:
            headers.update(request.headers)
        headers.update(self.header_for_authentication(request))
        request.headers = headers

        return request

    def get_ws_authenticate_payload(self,
                                    ws_url,
                                    ) -> Dict[str, any]:

        timestamp = int(self.time_provider.time())
        parsedurl = urlparse(ws_url)
        url_path = parsedurl.path
        pre_hash = f"{timestamp}GET{url_path}"

        signature = self._generate_signature(pre_hash)

        payload = {
           "event": "login",
           "params": {
              "type": "api",
              "api_key": self.api_key,
              "api_timestamp": str(timestamp),
              "api_sign": signature,
              "api_passphrase": self.api_passphrase
           }
        }

        return payload

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        """
        This method is intended to configure a websocket request to be authenticated. Uzx does not use this
        functionality
        """
        return request  # pass-through

    def header_for_authentication(self, request) -> Dict[str, str]:

        timestamp = int(self.time_provider.time())
        parsedurl = urlparse(request.url)

        path_with_query = parsedurl.path
        if request.params:
            path_with_query += "?" + urlencode(request.params)
        body_str = ""

        if request.data:
          json_data = json.loads(request.data)
          body_str = json.dumps(json_data, separators=(",", ":"))
          request.data = body_str

        pre_hash = f"{timestamp}{str(request.method).upper()}{path_with_query}{body_str}"

        signature = self._generate_signature(pre_hash)

        return {"UZX-ACCESS-KEY": self.api_key,
                "UZX-ACCESS-SIGN" : str(signature),
                "UZX-ACCESS-TIMESTAMP" : str(timestamp),
                "UZX-ACCESS-PASSPHRASE": self.api_passphrase}

    def _generate_signature(self, pre_hash: str) -> str:
     digest = hmac.new(
        self.secret_key.encode("utf-8"),
        pre_hash.encode("utf-8"),
        hashlib.sha256).digest()
     return base64.b64encode(digest).decode("utf-8")

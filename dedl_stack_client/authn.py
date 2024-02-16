"""
Copyright 2024 EODC GmbH

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""

import getpass
from datetime import datetime

import requests
from dask_gateway.auth import GatewayAuth
from jwt import PyJWKClient, decode


class DaskOIDC(GatewayAuth):
    def __init__(self, username: str):
        self.username = username
        self.client_id = "dedl-stack-public-client"
        self.token_url = "https://identity.data.destination-earth.eu/auth/realms/dedl/protocol/openid-connect/token"
        self.cert_url = "https://identity.data.destination-earth.eu/auth/realms/dedl/protocol/openid-connect/certs"
        self.token = self.get_token()
        self.access_token_decoded = self.decode_access_token()

    def get_token(self):
        payload = {
            "grant_type": "password",
            "client_id": self.client_id,
            "username": self.username,
            "password": getpass.getpass(prompt="Enter your password:"),
        }
        return requests.post(self.token_url, data=payload).json()

    def decode_access_token(self):
        jwks_client = PyJWKClient(self.cert_url)
        signing_key = jwks_client.get_signing_key_from_jwt(self.token["access_token"])
        return decode(
            self.token["access_token"],
            signing_key.key,
            audience="account",
            algorithms=["RS256"],
            leeway=10,
        )

    def is_token_expired(self):
        return datetime.now() > datetime.fromtimestamp(self.access_token_decoded["exp"])

    def refresh_token_exchange(self):
        payload = {
            "grant_type": "refresh_token",
            "client_id": self.client_id,
            "refresh_token": self.token["refresh_token"],
        }
        return requests.post(self.token_url, data=payload).json()

    def refresh(self):
        self.token = self.refresh_token_exchange()
        self.access_token_decoded = self.decode_access_token()

    def pre_request(self, _):
        if self.is_token_expired():
            self.refresh()
        headers = {"Authorization": "Bearer " + self.token["access_token"]}
        return headers, None

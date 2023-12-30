import base64
import logging
from dataclasses import dataclass

import requests

_log = logging.getLogger(__name__)


@dataclass
class AuthConfig:
    keycloak_host: str
    keycloak_realm: str
    keycloak_ca: str
    client_id: str


def get_auth_token(config: AuthConfig,
                   client_secret: str) -> str:
    """Get the keycloak client credential token to authorize with services.
    Args:
      keycloak_host (str): Host string for keycloak.
      keycloak_realm (str): Keycloak realm to authorize in.
      keycloak_ca (str): CA cert file signing keycloak server auth.
      client_id (str): Client id within keycloak.
      client_secret (str): Secret key for keycloak auth.
    """
    token_url = f"https://{config.keycloak_host}/realms/{config.keycloak_realm}/protocol/openid-connect/token"
    encoded = base64.b64encode(f"{config.client_id}:{config.client_secret}").decode('utf-8')
    headers = {
        "Authorization": f"Basic {encoded}"
    }
    data = {
        "grant_type": "client_credentials"
    }
    response = requests.post(
        token_url, headers=headers, data=data, verify=config.keycloak_ca)
    response.raise_for_status()
    return response.json()["access_token"]

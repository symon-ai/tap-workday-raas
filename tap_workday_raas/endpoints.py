"""API routing shared by WSDL downloads, SOAP bindings and report requests."""

import ipaddress
import socket
from urllib.parse import urlsplit, urlunsplit

from requests.adapters import HTTPAdapter
from requests.exceptions import InvalidURL, ProxyError
from requests.utils import select_proxy
from urllib3.connection import HTTPSConnection
from urllib3.connectionpool import HTTPSConnectionPool
from urllib3.exceptions import NewConnectionError
from urllib3.util import Timeout


class PublicHTTPSConnection(HTTPSConnection):
    """Resolve and connect the same checked address, retaining host for TLS/SNI."""

    def _new_conn(self):
        try:
            addresses = socket.getaddrinfo(self._dns_host, self.port, type=socket.SOCK_STREAM)
            if not addresses or any(not ipaddress.ip_address(item[4][0]).is_global for item in addresses):
                raise OSError("HTTPS destination must resolve only to public addresses")
            error = None
            for family, kind, protocol, _, address in addresses:
                sock = socket.socket(family, kind, protocol)
                try:
                    sock.settimeout(Timeout.resolve_default_timeout(self.timeout))
                    for option in self.socket_options or ():
                        sock.setsockopt(*option)
                    if self.source_address:
                        sock.bind(self.source_address)
                    # sockaddr comes directly from the checked resolution. No second lookup.
                    sock.connect(address)
                    return sock
                except OSError as exc:
                    error = exc
                    sock.close()
            raise error
        except OSError as exc:
            raise NewConnectionError(self, "Unable to connect to a public HTTPS destination") from exc


class PublicHTTPSConnectionPool(HTTPSConnectionPool):
    ConnectionCls = PublicHTTPSConnection


class PublicHTTPSAdapter(HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        super().init_poolmanager(*args, **kwargs)
        # urllib3's default mapping is global; replace only this manager's copy.
        self.poolmanager.pool_classes_by_scheme = dict(self.poolmanager.pool_classes_by_scheme)
        self.poolmanager.pool_classes_by_scheme["https"] = PublicHTTPSConnectionPool

    def send(self, request, **kwargs):
        if urlsplit(request.url).scheme != "https":
            raise InvalidURL("OAuth and custom Workday endpoints require HTTPS")
        if select_proxy(request.url, kwargs.get("proxies") or {}):
            # A proxy performs its own target DNS lookup outside our checked socket.
            raise ProxyError("Proxies are not supported for public-only Workday endpoints")
        kwargs["verify"] = True
        return super().send(request, **kwargs)


def configure_token_session(session):
    adapter = PublicHTTPSAdapter()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    request = session.request

    def token_request(method, url, **kwargs):
        validate_base_url(url)
        kwargs["verify"] = True
        kwargs["allow_redirects"] = False
        return request(method, url, **kwargs)

    session.request = token_request

def validate_base_url(value):
    if not isinstance(value, str) or any(c.isspace() or ord(c) < 32 for c in value) or "\\" in value:
        raise ValueError("base_url must be an absolute public HTTPS URL")
    parts = urlsplit(value)
    if (parts.scheme != "https" or not parts.hostname or parts.username is not None
            or parts.password is not None or parts.query or parts.fragment):
        raise ValueError("base_url must be HTTPS without credentials, query or fragment")
    try:
        port = parts.port or 443
        try:
            addresses = [ipaddress.ip_address(parts.hostname)]
        except ValueError:
            addresses = [ipaddress.ip_address(item[4][0]) for item in
                         socket.getaddrinfo(parts.hostname, port, type=socket.SOCK_STREAM)]
        if not addresses or any(not address.is_global for address in addresses):
            raise ValueError("base_url must resolve only to public addresses")
    except OSError as exc:
        raise ValueError("base_url hostname could not be resolved") from exc
    return value.rstrip("/")


def route_api_url(url, base_url):
    if not base_url:
        return url
    base = urlsplit(base_url)
    original = urlsplit(url)
    prefix = base.path.rstrip("/").split("/")[1:]
    path = original.path.lstrip("/").split("/")
    # A returned service URL can already include /ccx/service/{tenant}.
    overlap = 0
    for count in range(min(len(prefix), len(path)), 0, -1):
        if prefix[-count:] == path[:count]:
            overlap = count
            break
    joined = "/" + "/".join(prefix + path[overlap:])
    return urlunsplit((base.scheme, base.netloc, joined, original.query, original.fragment))


def configure_api_session(session, config, oauth_provider=None):
    base_url = validate_base_url(config["base_url"]) if config.get("base_url") else None
    request = session.request
    adapter = PublicHTTPSAdapter()
    secured = False

    def routed_request(method, url, **kwargs):
        nonlocal secured
        endpoint = base_url
        if oauth_provider is not None:
            # Refresh before resolving the endpoint, not after preparing the request.
            oauth_provider.get_access_token()
            endpoint = oauth_provider.api_base_url or endpoint
        if endpoint:
            validate_base_url(endpoint)
            url = route_api_url(url, endpoint)
            if not secured:
                session.mount("https://", adapter)
                session.mount("http://", adapter)
                secured = True
            kwargs["verify"] = True
            # Never follow an unvalidated redirect carrying SOAP or report credentials.
            kwargs["allow_redirects"] = False
        return request(method, url, **kwargs)

    # Zeep uses this same session for WSDL imports and service POSTs, including
    # vendor addresses embedded in the WSDL. Token HTTP uses a separate session.
    session.request = routed_request

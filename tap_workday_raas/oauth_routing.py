"""URL contract shared by the standalone Workday OAuth connectors."""

import ipaddress
import re
import socket
from urllib.parse import quote, urlsplit, urlunsplit

import requests


_DNS_LABEL = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\Z", re.IGNORECASE)


def _is_public(ip):
    address = ipaddress.ip_address(ip)
    if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped:
        address = address.ipv4_mapped
    return (address.is_global and not address.is_multicast and not address.is_reserved
            and not getattr(address, "is_site_local", False))


def https_url(value):
    """Reject ambiguous URLs and non-public literals before using credentials."""
    error = "Workday OAuth endpoints must be public HTTPS URLs without user information or fragments"
    if not isinstance(value, str) or re.search(r"[\x00-\x20\x7f\\#]", value):
        raise ValueError(error)
    try:
        url = urlsplit(value)
        host = url.hostname
        port = url.port
    except ValueError:
        raise ValueError(error) from None
    if url.scheme != "https" or not host or url.username is not None or "%" in url.netloc or port == 0:
        raise ValueError(error)
    try:
        ipaddress.ip_address(host)
    except ValueError:
        labels = host.rstrip(".").split(".")
        if not all(_DNS_LABEL.fullmatch(label) for label in labels) or labels[-1].isdigit() or labels[-1] in {
            "localhost", "local", "internal", "localdomain", "home", "lan", "corp", "onion",
        }:
            raise ValueError(error) from None
    else:
        if not _is_public(host):
            raise ValueError(error)
    return url


def _public_ip(url):
    """Reject mixed public/private DNS answers and return a vetted connection address."""
    try:
        addresses = socket.getaddrinfo(url.hostname, url.port or 443, type=socket.SOCK_STREAM)
    except OSError:
        raise ValueError("Workday OAuth endpoint did not resolve") from None
    if not addresses or any(not _is_public(address[4][0]) for address in addresses):
        raise ValueError("Workday OAuth endpoint must resolve only to public addresses")
    return addresses[0][4][0]


class _PublicHttpsAdapter(requests.adapters.HTTPAdapter):
    def build_connection_pool_key_attributes(self, request, verify, cert=None):
        host_params, pool_kwargs = super().build_connection_pool_key_attributes(request, verify, cert)
        url = https_url(request.url)
        # Dial the checked IP instead of resolving the hostname again, including through CONNECT.
        # Preserve the requested hostname for HTTP Host, TLS SNI and certificate verification.
        host_params["host"] = _public_ip(url)
        pool_kwargs["server_hostname"] = url.hostname
        pool_kwargs["assert_hostname"] = url.hostname
        request.headers["Host"] = url.netloc
        return host_params, pool_kwargs


def oauth_session():
    session = requests.Session()
    session.mount("https://", _PublicHttpsAdapter())
    return session


def _origin(url):
    return url.scheme, url.hostname, url.port or 443


def same_origin(url, other):
    return _origin(https_url(url)) == _origin(https_url(other))


def configured_base_url(config):
    value = config.get("base_url")
    if value is None:
        return None
    url = https_url(value)
    if url.path not in ("", "/") or "?" in value:
        raise ValueError("Workday base_url must be an HTTPS origin")
    return urlunsplit((url.scheme, url.netloc, "", "", ""))


def token_endpoint(config):
    base = configured_base_url(config)
    explicit = config.get("token_url")
    if explicit:
        return routed_url(explicit, base)
    tenant = config.get("tenant")
    if base and isinstance(tenant, str) and tenant.strip() and tenant not in (".", ".."):
        return f"{base}/ccx/oauth2/{quote(tenant, safe='')}/token"
    raise ValueError("OAuth auth requires token_url or base_url and tenant in config")


def response_route(fields):
    for name in ("service_url", "instance_url"):
        value = fields.get(name)
        if value is not None and value != "":
            url = https_url(value)
            return urlunsplit((url.scheme, url.netloc, "", "", ""))
    return None


def routed_url(url, route):
    source = https_url(url)
    if route is None:
        return url
    destination = https_url(route)
    return urlunsplit((destination.scheme, destination.netloc, source.path, source.query, ""))


def reject_redirect(response):
    if 300 <= response.status_code < 400:
        response.close()
        raise requests.exceptions.HTTPError("Workday OAuth request refused an HTTP redirect", response=response)

"""IP address classification utilities."""

import ipaddress
import logging

logger = logging.getLogger(__name__)


def is_private_ip(ip_address: str) -> bool:
    """
    Check if an IP address is private (RFC 1918).

    Args:
        ip_address: IP address string (with or without CIDR notation)

    Returns:
        True if IP is private, False otherwise
    """
    try:
        ip_str = ip_address.split('/')[0] if '/' in ip_address else ip_address
        ip_obj = ipaddress.ip_address(ip_str)
        return ip_obj.is_private
    except (ValueError, AttributeError) as e:
        logger.debug(f"Could not parse IP {ip_address}: {e}")
        return False

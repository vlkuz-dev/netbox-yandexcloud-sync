"""IP address utility functions — CIDR helpers."""


def get_ip_without_cidr(ip_address: str) -> str:
    """
    Get IP address without CIDR notation.

    Args:
        ip_address: IP address string (with or without CIDR notation)

    Returns:
        IP address without CIDR notation
    """
    return ip_address.split('/')[0] if '/' in ip_address else ip_address


def ensure_cidr_notation(ip_address: str, default_prefix: str = "/32") -> str:
    """
    Ensure IP address has CIDR notation.

    Args:
        ip_address: IP address string
        default_prefix: Default prefix to add if not present

    Returns:
        IP address with CIDR notation
    """
    if '/' not in ip_address:
        return f"{ip_address}{default_prefix}"
    return ip_address

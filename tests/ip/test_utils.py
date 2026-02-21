"""Tests for netbox_sync.ip.utils module."""


from netbox_sync.ip.utils import get_ip_without_cidr, ensure_cidr_notation


class TestGetIPWithoutCIDR:
    def test_ip_with_cidr(self):
        assert get_ip_without_cidr("10.0.0.1/24") == "10.0.0.1"

    def test_ip_without_cidr(self):
        assert get_ip_without_cidr("10.0.0.1") == "10.0.0.1"

    def test_ip_with_32_prefix(self):
        assert get_ip_without_cidr("8.8.8.8/32") == "8.8.8.8"


class TestEnsureCIDRNotation:
    def test_ip_without_cidr_gets_default(self):
        assert ensure_cidr_notation("10.0.0.1") == "10.0.0.1/32"

    def test_ip_with_cidr_unchanged(self):
        assert ensure_cidr_notation("10.0.0.1/24") == "10.0.0.1/24"

    def test_custom_default_prefix(self):
        assert ensure_cidr_notation("10.0.0.1", "/24") == "10.0.0.1/24"

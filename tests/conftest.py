"""Shared fixtures and mocks for netbox-sync tests."""

from unittest.mock import MagicMock


class MockRecord:
    """Mock pynetbox Record with attribute access and mockable save/delete."""

    def __init__(self, **kwargs):
        self.save = MagicMock(return_value=True)
        self.delete = MagicMock(return_value=True)
        for key, value in kwargs.items():
            setattr(self, key, value)


class MockTag:
    """Mock NetBox tag."""

    def __init__(self, id=1, name="synced-from-yc", slug="synced-from-yc"):
        self.id = id
        self.name = name
        self.slug = slug


def make_mock_netbox_client(dry_run=False):
    """Create a mock NetBoxClient with pre-configured sub-objects."""
    client = MagicMock()
    client.dry_run = dry_run
    client.ensure_sync_tag.return_value = 1
    client.ensure_cluster_type.return_value = 1
    client.ensure_site.return_value = 10
    client.ensure_cluster.return_value = 20
    client.ensure_platform.return_value = 8
    client.ensure_prefix.return_value = MockRecord(id=30, prefix="10.0.0.0/24")
    client.fetch_vms.return_value = []
    client.create_vm.return_value = MockRecord(id=100, name="test-vm")
    client.create_disk.return_value = MockRecord(id=200, name="disk-0")
    client.create_interface.return_value = MockRecord(id=300, name="eth0")
    client.create_ip.return_value = MockRecord(id=400, address="10.0.0.1/32")
    client.update_vm.return_value = True
    client.set_vm_primary_ip.return_value = True

    # Setup nb sub-object with mock API endpoints
    client.nb = MagicMock()
    client.nb.dcim.sites.all.return_value = []
    client.nb.virtualization.clusters.all.return_value = []
    client.nb.ipam.prefixes.all.return_value = []
    client.nb.virtualization.virtual_machines.all.return_value = []
    client.nb.virtualization.virtual_machines.filter.return_value = []
    client.nb.virtualization.interfaces.filter.return_value = []
    client.nb.ipam.ip_addresses.filter.return_value = []
    client.nb.virtualization.virtual_disks.filter.return_value = []

    return client

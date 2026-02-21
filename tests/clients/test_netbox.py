"""Tests for NetBox client."""

import pytest
from unittest.mock import MagicMock, patch

from netbox_sync.clients.netbox import NetBoxClient


class MockRecord:
    """Mock pynetbox Record object."""

    def __init__(self, id, name=None, slug=None, **kwargs):
        self.id = id
        self.name = name
        self.slug = slug
        self.save = MagicMock(return_value=True)
        self.delete = MagicMock(return_value=True)
        for k, v in kwargs.items():
            setattr(self, k, v)

    def __str__(self):
        return self.name or str(self.id)


@pytest.fixture
def nb_client():
    """Create a NetBoxClient with mocked pynetbox API."""
    with patch('netbox_sync.clients.netbox.pynetbox') as mock_pynetbox:
        mock_api = MagicMock()
        mock_pynetbox.api.return_value = mock_api

        client = NetBoxClient("https://netbox.example.com", "test-token", dry_run=False)

        # Make the mock api accessible for test setup
        client._mock_api = mock_api
        return client


@pytest.fixture
def nb_client_dry_run():
    """Create a NetBoxClient in dry-run mode."""
    with patch('netbox_sync.clients.netbox.pynetbox') as mock_pynetbox:
        mock_api = MagicMock()
        mock_pynetbox.api.return_value = mock_api

        client = NetBoxClient("https://netbox.example.com", "test-token", dry_run=True)
        client._mock_api = mock_api
        return client


class TestEnsureSyncTag:
    def test_returns_cached_tag(self, nb_client):
        nb_client._sync_tag_id = 42
        result = nb_client.ensure_sync_tag()
        assert result == 42
        # Should not make any API calls
        nb_client.nb.extras.tags.get.assert_not_called()

    def test_finds_existing_tag_by_name(self, nb_client):
        tag = MockRecord(10, name="synced-from-yc", slug="synced-from-yc")
        nb_client.nb.extras.tags.get.return_value = tag

        result = nb_client.ensure_sync_tag()

        assert result == 10
        assert nb_client._sync_tag_id == 10

    def test_finds_existing_tag_by_slug_fallback(self, nb_client):
        # First call (by name) raises, second (by slug) succeeds
        tag = MockRecord(11, name="synced-from-yc", slug="synced-from-yc")

        def get_side_effect(**kwargs):
            if 'name' in kwargs:
                raise Exception("not found")
            return tag

        nb_client.nb.extras.tags.get.side_effect = get_side_effect

        result = nb_client.ensure_sync_tag()

        assert result == 11

    def test_creates_tag_when_not_found(self, nb_client):
        nb_client.nb.extras.tags.get.return_value = None
        new_tag = MockRecord(20, name="synced-from-yc")
        nb_client.nb.extras.tags.create.return_value = new_tag

        result = nb_client.ensure_sync_tag()

        assert result == 20
        nb_client.nb.extras.tags.create.assert_called_once()
        call_args = nb_client.nb.extras.tags.create.call_args[0][0]
        assert call_args["name"] == "synced-from-yc"
        assert call_args["slug"] == "synced-from-yc"

    def test_dry_run_returns_mock_id(self, nb_client_dry_run):
        nb_client_dry_run.nb.extras.tags.get.return_value = None

        result = nb_client_dry_run.ensure_sync_tag()

        assert result == 1
        nb_client_dry_run.nb.extras.tags.create.assert_not_called()

    def test_create_failure_returns_zero(self, nb_client):
        nb_client.nb.extras.tags.get.return_value = None
        nb_client.nb.extras.tags.create.side_effect = Exception("create failed")

        result = nb_client.ensure_sync_tag()

        assert result == 0


class TestEnsureSite:
    def test_finds_existing_site_by_name(self, nb_client):
        nb_client._sync_tag_id = 1  # Pre-set to avoid tag creation
        site = MockRecord(5, name="ru-central1-a", slug="ru-central1-a",
                          description="old", status="active", tags=[])
        nb_client.nb.dcim.sites.get.return_value = site

        result = nb_client.ensure_site("ru-central1-a")

        assert result == 5

    def test_creates_site_when_not_found(self, nb_client):
        nb_client._sync_tag_id = 1
        nb_client.nb.dcim.sites.get.return_value = None
        new_site = MockRecord(6, name="ru-central1-a")
        nb_client.nb.dcim.sites.create.return_value = new_site

        result = nb_client.ensure_site("ru-central1-a")

        assert result == 6
        call_args = nb_client.nb.dcim.sites.create.call_args[0][0]
        assert call_args["name"] == "ru-central1-a"
        assert call_args["slug"] == "ru-central1-a"
        assert call_args["status"] == "active"
        assert call_args["tags"] == [1]

    def test_uses_zone_name_when_provided(self, nb_client):
        nb_client._sync_tag_id = 1
        nb_client.nb.dcim.sites.get.return_value = None
        new_site = MockRecord(7, name="Zone A")
        nb_client.nb.dcim.sites.create.return_value = new_site

        result = nb_client.ensure_site("ru-central1-a", zone_name="Zone A")

        assert result == 7
        call_args = nb_client.nb.dcim.sites.create.call_args[0][0]
        assert call_args["name"] == "Zone A"
        # slug still derived from zone_id
        assert call_args["slug"] == "ru-central1-a"

    def test_dry_run_returns_mock_id(self, nb_client_dry_run):
        nb_client_dry_run.nb.dcim.sites.get.return_value = None

        result = nb_client_dry_run.ensure_site("ru-central1-a")

        assert result == 1
        nb_client_dry_run.nb.dcim.sites.create.assert_not_called()

    def test_handles_duplicate_slug_error(self, nb_client):
        nb_client._sync_tag_id = 1
        # First get returns None, create throws duplicate slug error
        nb_client.nb.dcim.sites.get.side_effect = [None, None, MockRecord(8, name="ru-central1-a")]
        nb_client.nb.dcim.sites.create.side_effect = Exception("400 slug already exists")

        result = nb_client.ensure_site("ru-central1-a")

        assert result == 8


class TestEnsureClusterType:
    def test_returns_cached_type(self, nb_client):
        nb_client._cluster_type_id = 99
        result = nb_client.ensure_cluster_type()
        assert result == 99

    def test_finds_existing_by_name(self, nb_client):
        nb_client._sync_tag_id = 1
        ct = MockRecord(3, name="yandex-cloud", slug="yandex-cloud",
                        description="Yandex Cloud Platform", tags=[])
        nb_client.nb.virtualization.cluster_types.get.return_value = ct

        result = nb_client.ensure_cluster_type()

        assert result == 3
        assert nb_client._cluster_type_id == 3

    def test_creates_when_not_found(self, nb_client):
        nb_client._sync_tag_id = 1
        nb_client.nb.virtualization.cluster_types.get.return_value = None
        new_ct = MockRecord(4, name="yandex-cloud")
        nb_client.nb.virtualization.cluster_types.create.return_value = new_ct

        result = nb_client.ensure_cluster_type()

        assert result == 4


class TestCreateVM:
    def test_create_vm_success(self, nb_client):
        nb_client._sync_tag_id = 1
        vm = MockRecord(100, name="web-1")
        nb_client.nb.virtualization.virtual_machines.create.return_value = vm

        result = nb_client.create_vm({
            "name": "web-1",
            "cluster": 1,
            "vcpus": 2,
            "memory": 4096,
            "status": "active",
        })

        assert result.id == 100
        assert result.name == "web-1"

    def test_create_vm_removes_disk_field(self, nb_client):
        nb_client._sync_tag_id = 1
        vm = MockRecord(101, name="web-2")
        nb_client.nb.virtualization.virtual_machines.create.return_value = vm

        nb_client.create_vm({
            "name": "web-2",
            "cluster": 1,
            "disk": 50,  # Should be removed
        })

        call_args = nb_client.nb.virtualization.virtual_machines.create.call_args[0][0]
        assert "disk" not in call_args

    def test_create_vm_adds_tag(self, nb_client):
        nb_client._sync_tag_id = 5
        vm = MockRecord(102, name="web-3")
        nb_client.nb.virtualization.virtual_machines.create.return_value = vm

        nb_client.create_vm({"name": "web-3", "cluster": 1})

        call_args = nb_client.nb.virtualization.virtual_machines.create.call_args[0][0]
        assert call_args["tags"] == [5]

    def test_create_vm_dry_run(self, nb_client_dry_run):
        result = nb_client_dry_run.create_vm({"name": "test-vm", "cluster": 1})

        assert result is not None
        assert result.id == 1
        assert result.name == "test-vm"
        nb_client_dry_run.nb.virtualization.virtual_machines.create.assert_not_called()

    def test_create_vm_failure_returns_none(self, nb_client):
        nb_client._sync_tag_id = 1
        nb_client.nb.virtualization.virtual_machines.create.side_effect = Exception("API error")

        result = nb_client.create_vm({"name": "fail-vm", "cluster": 1})

        assert result is None


class TestSetVmPrimaryIp:
    def test_set_primary_ip_success(self, nb_client):
        vm = MockRecord(100, name="web-1")
        ip = MockRecord(200, address="10.0.0.5/24", assigned_object_id=50)
        iface = MockRecord(50, name="eth0")

        nb_client.nb.virtualization.virtual_machines.get.return_value = vm
        nb_client.nb.ipam.ip_addresses.get.return_value = ip
        nb_client.nb.virtualization.interfaces.filter.return_value = [iface]

        result = nb_client.set_vm_primary_ip(100, 200, ip_version=4)

        assert result is True
        assert vm.primary_ip4 == 200

    def test_set_primary_ip_assigns_to_interface_if_needed(self, nb_client):
        vm = MockRecord(100, name="web-1")
        ip = MockRecord(200, address="10.0.0.5/24", assigned_object_id=999)  # Not on this VM
        iface = MockRecord(50, name="eth0")

        nb_client.nb.virtualization.virtual_machines.get.return_value = vm
        nb_client.nb.ipam.ip_addresses.get.return_value = ip
        nb_client.nb.virtualization.interfaces.filter.return_value = [iface]

        result = nb_client.set_vm_primary_ip(100, 200)

        assert result is True
        # Should have assigned IP to VM's first interface
        assert ip.assigned_object_id == 50

    def test_set_primary_ip_vm_not_found(self, nb_client):
        nb_client.nb.virtualization.virtual_machines.get.return_value = None

        result = nb_client.set_vm_primary_ip(999, 200)

        assert result is False

    def test_set_primary_ip_ip_not_found(self, nb_client):
        vm = MockRecord(100, name="web-1")
        nb_client.nb.virtualization.virtual_machines.get.return_value = vm
        nb_client.nb.ipam.ip_addresses.get.return_value = None

        result = nb_client.set_vm_primary_ip(100, 999)

        assert result is False

    def test_set_primary_ip_invalid_version(self, nb_client):
        vm = MockRecord(100, name="web-1")
        ip = MockRecord(200, address="10.0.0.5/24", assigned_object_id=50)
        iface = MockRecord(50, name="eth0")

        nb_client.nb.virtualization.virtual_machines.get.return_value = vm
        nb_client.nb.ipam.ip_addresses.get.return_value = ip
        nb_client.nb.virtualization.interfaces.filter.return_value = [iface]

        result = nb_client.set_vm_primary_ip(100, 200, ip_version=5)

        assert result is False

    def test_set_primary_ipv6(self, nb_client):
        vm = MockRecord(100, name="web-1")
        ip = MockRecord(200, address="::1/128", assigned_object_id=50)
        iface = MockRecord(50, name="eth0")

        nb_client.nb.virtualization.virtual_machines.get.return_value = vm
        nb_client.nb.ipam.ip_addresses.get.return_value = ip
        nb_client.nb.virtualization.interfaces.filter.return_value = [iface]

        result = nb_client.set_vm_primary_ip(100, 200, ip_version=6)

        assert result is True
        assert vm.primary_ip6 == 200

    def test_dry_run(self, nb_client_dry_run):
        result = nb_client_dry_run.set_vm_primary_ip(100, 200)

        assert result is True
        nb_client_dry_run.nb.virtualization.virtual_machines.get.assert_not_called()

    def test_no_interfaces_returns_false(self, nb_client):
        vm = MockRecord(100, name="web-1")
        ip = MockRecord(200, address="10.0.0.5/24", assigned_object_id=999)

        nb_client.nb.virtualization.virtual_machines.get.return_value = vm
        nb_client.nb.ipam.ip_addresses.get.return_value = ip
        nb_client.nb.virtualization.interfaces.filter.return_value = []  # No interfaces

        result = nb_client.set_vm_primary_ip(100, 200)

        assert result is False


class TestUpdateVM:
    def test_update_vm_success(self, nb_client):
        nb_client._sync_tag_id = 1
        vm = MockRecord(100, name="web-1", tags=[])
        nb_client.nb.virtualization.virtual_machines.get.return_value = vm

        result = nb_client.update_vm(100, {"vcpus": 4, "memory": 8192})

        assert result is True
        assert vm.vcpus == 4
        assert vm.memory == 8192

    def test_update_vm_removes_disk_field(self, nb_client):
        nb_client._sync_tag_id = 1
        vm = MockRecord(100, name="web-1", tags=[])
        nb_client.nb.virtualization.virtual_machines.get.return_value = vm

        result = nb_client.update_vm(100, {"vcpus": 4, "disk": 100})

        assert result is True
        assert vm.vcpus == 4
        assert not hasattr(vm, 'disk') or getattr(vm, 'disk', None) is None

    def test_update_vm_not_found(self, nb_client):
        nb_client.nb.virtualization.virtual_machines.get.return_value = None

        result = nb_client.update_vm(999, {"vcpus": 4})

        assert result is False

    def test_update_vm_dry_run(self, nb_client_dry_run):
        result = nb_client_dry_run.update_vm(100, {"vcpus": 4})

        assert result is True
        nb_client_dry_run.nb.virtualization.virtual_machines.get.assert_not_called()


class TestCreateInterface:
    def test_create_interface_success(self, nb_client):
        iface = MockRecord(50, name="eth0")
        nb_client.nb.virtualization.interfaces.create.return_value = iface

        result = nb_client.create_interface({
            "virtual_machine": 100,
            "name": "eth0",
        })

        assert result.id == 50
        # Should set default type
        call_args = nb_client.nb.virtualization.interfaces.create.call_args[0][0]
        assert call_args["type"] == "virtual"

    def test_create_interface_dry_run(self, nb_client_dry_run):
        result = nb_client_dry_run.create_interface({
            "virtual_machine": 100,
            "name": "eth0",
        })

        assert result is not None
        assert result.name == "eth0"


class TestCreateIP:
    def test_create_ip_adds_cidr(self, nb_client):
        nb_client.nb.ipam.ip_addresses.filter.return_value = []
        ip = MockRecord(300, address="10.0.0.5/32")
        nb_client.nb.ipam.ip_addresses.create.return_value = ip

        result = nb_client.create_ip({"address": "10.0.0.5", "interface": 50})

        assert result.id == 300
        call_args = nb_client.nb.ipam.ip_addresses.create.call_args[0][0]
        assert call_args["address"] == "10.0.0.5/32"
        assert call_args["assigned_object_type"] == "virtualization.vminterface"
        assert call_args["assigned_object_id"] == 50

    def test_create_ip_returns_existing(self, nb_client):
        existing = MockRecord(301, address="10.0.0.5/24", assigned_object_id=50)
        nb_client.nb.ipam.ip_addresses.filter.return_value = [existing]

        result = nb_client.create_ip({"address": "10.0.0.5/32", "interface": 50})

        assert result.id == 301
        nb_client.nb.ipam.ip_addresses.create.assert_not_called()

    def test_create_ip_updates_interface_on_existing(self, nb_client):
        existing = MockRecord(301, address="10.0.0.5/24", assigned_object_id=40)
        nb_client.nb.ipam.ip_addresses.filter.return_value = [existing]

        result = nb_client.create_ip({"address": "10.0.0.5/32", "interface": 50})

        assert result.id == 301
        assert existing.assigned_object_id == 50

    def test_create_ip_dry_run(self, nb_client_dry_run):
        result = nb_client_dry_run.create_ip({"address": "10.0.0.5"})
        assert result is None


class TestGetVmByName:
    def test_found(self, nb_client):
        vm = MockRecord(100, name="web-1")
        nb_client.nb.virtualization.virtual_machines.get.return_value = vm

        result = nb_client.get_vm_by_name("web-1")

        assert result.id == 100

    def test_not_found(self, nb_client):
        nb_client.nb.virtualization.virtual_machines.get.return_value = None

        result = nb_client.get_vm_by_name("nonexistent")

        assert result is None

    def test_error_returns_none(self, nb_client):
        nb_client.nb.virtualization.virtual_machines.get.side_effect = Exception("error")

        result = nb_client.get_vm_by_name("error-vm")

        assert result is None


class TestFetchVms:
    def test_fetch_vms_success(self, nb_client):
        vms = [MockRecord(1, name="vm1"), MockRecord(2, name="vm2")]
        nb_client.nb.virtualization.virtual_machines.all.return_value = vms

        result = nb_client.fetch_vms()

        assert len(result) == 2

    def test_fetch_vms_error_returns_empty(self, nb_client):
        nb_client.nb.virtualization.virtual_machines.all.side_effect = Exception("error")

        result = nb_client.fetch_vms()

        assert result == []


class TestAddTagToObject:
    def test_adds_tag(self, nb_client):
        obj = MockRecord(1, name="test", tags=[])
        result = nb_client._add_tag_to_object(obj, 5)
        assert result is True

    def test_skips_if_already_present(self, nb_client):
        tag = MockRecord(5, name="synced-from-yc")
        obj = MockRecord(1, name="test", tags=[tag])
        result = nb_client._add_tag_to_object(obj, 5)
        assert result is True

    def test_skips_in_dry_run(self, nb_client_dry_run):
        obj = MockRecord(1, name="test", tags=[])
        result = nb_client_dry_run._add_tag_to_object(obj, 5)
        assert result is False

    def test_skips_if_no_tag_id(self, nb_client):
        obj = MockRecord(1, name="test", tags=[])
        result = nb_client._add_tag_to_object(obj, 0)
        assert result is False


class TestSafeUpdateObject:
    def test_updates_changed_fields(self, nb_client):
        obj = MockRecord(1, name="old-name", status="inactive")
        result = nb_client._safe_update_object(obj, {"name": "new-name", "status": "active"})
        assert result is True
        assert obj.name == "new-name"
        obj.save.assert_called_once()

    def test_no_update_when_same(self, nb_client):
        obj = MockRecord(1, name="same")
        result = nb_client._safe_update_object(obj, {"name": "same"})
        assert result is False
        obj.save.assert_not_called()

    def test_empty_updates_returns_false(self, nb_client):
        obj = MockRecord(1, name="test")
        result = nb_client._safe_update_object(obj, {})
        assert result is False

    def test_dry_run_returns_false(self, nb_client_dry_run):
        obj = MockRecord(1, name="old")
        result = nb_client_dry_run._safe_update_object(obj, {"name": "new"})
        assert result is False

    def test_handles_choice_item_comparison(self, nb_client):
        """ChoiceItem objects (e.g., status) are compared by .value."""
        class MockChoiceItem:
            def __init__(self, value):
                self.value = value
        obj = MockRecord(1, name="test", status=MockChoiceItem("active"))
        result = nb_client._safe_update_object(obj, {"status": "active"})
        assert result is False
        obj.save.assert_not_called()

    def test_updates_choice_item_when_different(self, nb_client):
        """ChoiceItem objects trigger update when value differs."""
        class MockChoiceItem:
            def __init__(self, value):
                self.value = value
        obj = MockRecord(1, name="test", status=MockChoiceItem("planned"))
        result = nb_client._safe_update_object(obj, {"status": "active"})
        assert result is True
        obj.save.assert_called_once()


class TestEnsureCluster:
    def test_finds_existing_cluster(self, nb_client):
        nb_client._sync_tag_id = 1
        nb_client._cluster_type_id = 2
        cluster = MockRecord(10, name="my-cloud/prod", tags=[],
                             type=MockRecord(2), site=None, comments="")
        nb_client.nb.virtualization.clusters.get.return_value = cluster

        result = nb_client.ensure_cluster("prod", "folder1", "my-cloud")

        assert result == 10

    def test_creates_cluster_when_not_found(self, nb_client):
        nb_client._sync_tag_id = 1
        nb_client._cluster_type_id = 2
        nb_client.nb.virtualization.clusters.get.return_value = None
        nb_client.nb.virtualization.clusters.filter.return_value = []
        new_cluster = MockRecord(11, name="my-cloud/staging")
        nb_client.nb.virtualization.clusters.create.return_value = new_cluster

        result = nb_client.ensure_cluster("staging", "folder2", "my-cloud", site_id=5)

        assert result == 11
        call_args = nb_client.nb.virtualization.clusters.create.call_args[0][0]
        assert call_args["name"] == "my-cloud/staging"
        assert call_args["type"] == 2
        assert call_args["site"] == 5

    def test_creates_cluster_without_cloud_name(self, nb_client):
        """When cloud_name is empty, uses folder_name only."""
        nb_client._sync_tag_id = 1
        nb_client._cluster_type_id = 2
        nb_client.nb.virtualization.clusters.get.return_value = None
        nb_client.nb.virtualization.clusters.filter.return_value = []
        new_cluster = MockRecord(12, name="standalone")
        nb_client.nb.virtualization.clusters.create.return_value = new_cluster

        result = nb_client.ensure_cluster("standalone", "folder3", "")

        assert result == 12
        call_args = nb_client.nb.virtualization.clusters.create.call_args[0][0]
        assert call_args["name"] == "standalone"

    def test_dry_run(self, nb_client_dry_run):
        nb_client_dry_run.nb.virtualization.clusters.get.return_value = None
        nb_client_dry_run.nb.virtualization.clusters.filter.return_value = []

        result = nb_client_dry_run.ensure_cluster("prod", "f1", "cloud1")

        assert result == 1


class TestEnsurePlatform:
    def test_finds_existing_platform(self, nb_client):
        platform = MockRecord(5, name="Ubuntu 22.04", slug="ubuntu-22-04")
        nb_client.nb.dcim.platforms.get.return_value = platform

        result = nb_client.ensure_platform("ubuntu-22-04")

        assert result == 5
        nb_client.nb.dcim.platforms.get.assert_called_with(slug="ubuntu-22-04")

    def test_creates_platform_when_not_found(self, nb_client):
        nb_client.nb.dcim.platforms.get.return_value = None
        new_platform = MockRecord(6, name="windows-2022", slug="windows-2022")
        nb_client.nb.dcim.platforms.create.return_value = new_platform

        result = nb_client.ensure_platform("windows-2022", "Windows Server 2022")

        assert result == 6
        call_args = nb_client.nb.dcim.platforms.create.call_args[0][0]
        assert call_args["name"] == "Windows Server 2022"
        assert call_args["slug"] == "windows-2022"

    def test_dry_run(self, nb_client_dry_run):
        nb_client_dry_run.nb.dcim.platforms.get.return_value = None

        result = nb_client_dry_run.ensure_platform("linux")

        assert result == 1
        nb_client_dry_run.nb.dcim.platforms.create.assert_not_called()

    def test_slug_used_as_name_when_no_name(self, nb_client):
        nb_client.nb.dcim.platforms.get.return_value = None
        new_platform = MockRecord(7, name="linux", slug="linux")
        nb_client.nb.dcim.platforms.create.return_value = new_platform

        result = nb_client.ensure_platform("linux")

        assert result == 7
        call_args = nb_client.nb.dcim.platforms.create.call_args[0][0]
        assert call_args["name"] == "linux"


class TestCreateDisk:
    def test_create_disk_success(self, nb_client):
        disk = MockRecord(60, name="boot-disk")
        nb_client.nb.virtualization.virtual_disks = MagicMock()
        nb_client.nb.virtualization.virtual_disks.create.return_value = disk

        result = nb_client.create_disk({"virtual_machine": 100, "size": 50, "name": "boot-disk"})

        assert result.id == 60

    def test_create_disk_dry_run(self, nb_client_dry_run):
        result = nb_client_dry_run.create_disk({"virtual_machine": 100, "size": 50, "name": "boot"})
        assert result is None


class TestImports:
    def test_import_from_module(self):
        from netbox_sync.clients.netbox import NetBoxClient as NB
        assert NB is NetBoxClient


class TestGetVmByCustomField:
    def test_found(self, nb_client):
        vm = MockRecord(100, name="web-1")
        nb_client.nb.virtualization.virtual_machines.filter.return_value = [vm]

        result = nb_client.get_vm_by_custom_field("yc_id", "abc123")

        assert result.id == 100
        nb_client.nb.virtualization.virtual_machines.filter.assert_called_with(cf_yc_id="abc123")

    def test_not_found(self, nb_client):
        nb_client.nb.virtualization.virtual_machines.filter.return_value = []

        result = nb_client.get_vm_by_custom_field("yc_id", "nonexistent")

        assert result is None

    def test_error_returns_none(self, nb_client):
        nb_client.nb.virtualization.virtual_machines.filter.side_effect = Exception("error")

        result = nb_client.get_vm_by_custom_field("yc_id", "abc")

        assert result is None

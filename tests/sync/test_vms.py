"""Tests for netbox_sync.sync.vms module."""

from unittest.mock import patch

from netbox_sync.sync.vms import (
    prepare_vm_data,
    update_vm_parameters,
    sync_vm_disks,
    update_vm_primary_ip,
    sync_vm_interfaces,
    sync_vms,
    detect_platform_slug,
    DEFAULT_PLATFORM_SLUG,
)
from tests.conftest import MockRecord, MockTag, make_mock_netbox_client


class TestPrepareVmData:
    """Tests for prepare_vm_data."""

    def _base_yc_vm(self, **overrides):
        vm = {
            "id": "vm-123",
            "name": "test-vm",
            "folder_id": "f1",
            "zone_id": "ru-central1-a",
            "status": "RUNNING",
            "platform_id": "standard-v3",
            "os": "ubuntu-22.04",
            "created_at": "2024-01-01",
            "resources": {"memory": 4294967296, "cores": 2},
            "disks": [{"name": "boot", "size": 10737418240}],
        }
        vm.update(overrides)
        return vm

    def _base_id_mapping(self):
        return {
            "zones": {"ru-central1-a": 10},
            "folders": {"f1": 20},
        }

    def test_basic_vm_data(self):
        """Prepares basic VM data with name, vcpus, memory, status."""
        netbox = make_mock_netbox_client()
        yc_vm = self._base_yc_vm()

        result = prepare_vm_data(yc_vm, netbox, self._base_id_mapping())

        assert result["name"] == "test-vm"
        assert result["vcpus"] == 2
        assert result["memory"] == 4096  # 4GB in bytes -> 4096 MB
        assert result["status"] == "active"
        assert result["cluster"] == 20
        assert result["site"] == 10

    def test_status_offline_for_non_running(self):
        """Non-RUNNING VMs get status='offline'."""
        netbox = make_mock_netbox_client()
        yc_vm = self._base_yc_vm(status="STOPPED")

        result = prepare_vm_data(yc_vm, netbox, self._base_id_mapping())

        assert result["status"] == "offline"

    def test_memory_bytes_to_mb_conversion(self):
        """Large memory values (bytes) are converted to MB."""
        netbox = make_mock_netbox_client()
        yc_vm = self._base_yc_vm(resources={"memory": 8589934592, "cores": 4})  # 8 GB

        result = prepare_vm_data(yc_vm, netbox, self._base_id_mapping())

        assert result["memory"] == 8192
        assert result["vcpus"] == 4

    def test_memory_string_parsing(self):
        """Memory as string "8GB" should be parsed."""
        netbox = make_mock_netbox_client()
        yc_vm = self._base_yc_vm(resources={"memory": "8", "cores": 2})

        result = prepare_vm_data(yc_vm, netbox, self._base_id_mapping())

        # "8" -> 8 < 1000 -> treated as GB -> 8 * 1024 = 8192 MB
        assert result["memory"] == 8192

    def test_memory_zero_when_missing(self):
        """When memory is 0, memory_mb is 0."""
        netbox = make_mock_netbox_client()
        yc_vm = self._base_yc_vm(resources={"memory": 0, "cores": 1})

        result = prepare_vm_data(yc_vm, netbox, self._base_id_mapping())

        assert result["memory"] == 0

    def test_cores_string_parsing(self):
        """Cores as string are parsed."""
        netbox = make_mock_netbox_client()
        yc_vm = self._base_yc_vm(resources={"memory": 4294967296, "cores": "4"})

        result = prepare_vm_data(yc_vm, netbox, self._base_id_mapping())

        assert result["vcpus"] == 4

    def test_platform_ubuntu_2204(self):
        """Ubuntu 22.04 resolves to correct platform slug."""
        netbox = make_mock_netbox_client()
        yc_vm = self._base_yc_vm(os="ubuntu-22.04-lts")

        prepare_vm_data(yc_vm, netbox, self._base_id_mapping())

        netbox.ensure_platform.assert_called_with("ubuntu-22-04")

    def test_platform_ubuntu_2404(self):
        """Ubuntu 24.04 resolves to correct platform slug."""
        netbox = make_mock_netbox_client()
        yc_vm = self._base_yc_vm(os="Ubuntu 24.04 Noble")

        prepare_vm_data(yc_vm, netbox, self._base_id_mapping())

        netbox.ensure_platform.assert_called_with("ubuntu-24-04")

    def test_platform_windows_2022(self):
        """Windows Server 2022 resolves to correct platform slug."""
        netbox = make_mock_netbox_client()
        yc_vm = self._base_yc_vm(os="Windows Server 2022")

        prepare_vm_data(yc_vm, netbox, self._base_id_mapping())

        netbox.ensure_platform.assert_called_with("windows-2022")

    def test_platform_debian(self):
        """Debian 11 resolves to correct platform slug."""
        netbox = make_mock_netbox_client()
        yc_vm = self._base_yc_vm(os="debian-11-bullseye")

        prepare_vm_data(yc_vm, netbox, self._base_id_mapping())

        netbox.ensure_platform.assert_called_with("debian-11")

    def test_platform_centos(self):
        """CentOS 7 resolves to correct platform slug."""
        netbox = make_mock_netbox_client()
        yc_vm = self._base_yc_vm(os="centos-7")

        prepare_vm_data(yc_vm, netbox, self._base_id_mapping())

        netbox.ensure_platform.assert_called_with("centos-7")

    def test_platform_almalinux(self):
        """AlmaLinux 9 resolves to correct platform slug."""
        netbox = make_mock_netbox_client()
        yc_vm = self._base_yc_vm(os="almalinux-9")

        prepare_vm_data(yc_vm, netbox, self._base_id_mapping())

        netbox.ensure_platform.assert_called_with("almalinux-9")

    def test_platform_unknown_defaults_to_linux(self):
        """Unknown OS defaults to linux slug."""
        netbox = make_mock_netbox_client()
        yc_vm = self._base_yc_vm(os="")

        prepare_vm_data(yc_vm, netbox, self._base_id_mapping())

        netbox.ensure_platform.assert_called_with(DEFAULT_PLATFORM_SLUG)

    def test_detect_platform_slug_coverage(self):
        """Test slug detection for various OS names."""
        assert detect_platform_slug("windows-2019-dc") == "windows-2019"
        assert detect_platform_slug("Windows Server 2025") == "windows-2025"
        assert detect_platform_slug("Windows") == "windows"
        assert detect_platform_slug("oracle-linux-9") == "oracle-linux-9"
        assert detect_platform_slug("rocky linux 9") == DEFAULT_PLATFORM_SLUG
        assert detect_platform_slug("some-linux-distro") == DEFAULT_PLATFORM_SLUG
        assert detect_platform_slug("") == DEFAULT_PLATFORM_SLUG
        assert detect_platform_slug("FreeBSD") == DEFAULT_PLATFORM_SLUG

    def test_comments_include_metadata(self):
        """Comments include VM ID, zone, platform, OS, and creation date."""
        netbox = make_mock_netbox_client()
        yc_vm = self._base_yc_vm()

        result = prepare_vm_data(yc_vm, netbox, self._base_id_mapping())

        assert "YC VM ID: vm-123" in result["comments"]
        assert "Zone: ru-central1-a" in result["comments"]
        assert "Hardware Platform: standard-v3" in result["comments"]
        assert "OS: ubuntu-22.04" in result["comments"]

    def test_cluster_fallback_when_missing_from_mapping(self):
        """When folder_id not in mapping, cluster is created on-the-fly."""
        netbox = make_mock_netbox_client()
        netbox.ensure_cluster.return_value = 99
        yc_vm = self._base_yc_vm(folder_id="unknown-folder", folder_name="fallback-folder", cloud_name="cloud-x")

        result = prepare_vm_data(yc_vm, netbox, {"zones": {}, "folders": {}})

        assert result["cluster"] == 99
        netbox.ensure_cluster.assert_called_once()

    def test_no_site_when_zone_not_in_mapping(self):
        """If zone_id is not in id_mapping, site is not set."""
        netbox = make_mock_netbox_client()
        yc_vm = self._base_yc_vm(zone_id="unknown-zone")

        result = prepare_vm_data(yc_vm, netbox, {"zones": {}, "folders": {"f1": 20}})

        assert "site" not in result

    def test_non_dict_resources_handled(self):
        """If resources is not a dict, defaults are used."""
        netbox = make_mock_netbox_client()
        yc_vm = self._base_yc_vm(resources="invalid")

        result = prepare_vm_data(yc_vm, netbox, self._base_id_mapping())

        assert result["memory"] == 0
        assert result["vcpus"] == 1


class TestUpdateVmParameters:
    """Tests for update_vm_parameters."""

    def test_updates_when_memory_differs(self):
        """VM is updated when memory has changed."""
        netbox = make_mock_netbox_client()
        netbox.ensure_cluster.return_value = 20

        vm = MockRecord(
            id=1, name="test-vm", memory=2048, vcpus=2,
            cluster=MockRecord(id=20), site=MockRecord(id=10),
            platform=MockRecord(id=8), status=MockRecord(value="active"),
            comments=(
                "YC VM ID: vm-123\nZone: ru-central1-a\n"
                "Hardware Platform: standard-v3\nOS: ubuntu-22.04\nCreated: 2024-01-01"
            ),
        )
        yc_vm = {
            "name": "test-vm", "id": "vm-123", "folder_id": "f1",
            "zone_id": "ru-central1-a", "status": "RUNNING",
            "platform_id": "standard-v3", "os": "ubuntu-22.04",
            "created_at": "2024-01-01",
            "resources": {"memory": 4294967296, "cores": 2},
        }
        id_mapping = {"zones": {"ru-central1-a": 10}, "folders": {"f1": 20}}

        result = update_vm_parameters(vm, yc_vm, netbox, id_mapping)

        assert result is True
        netbox.update_vm.assert_called_once()
        call_args = netbox.update_vm.call_args
        assert "memory" in call_args[0][1]
        assert call_args[0][1]["memory"] == 4096

    def test_no_update_when_up_to_date(self):
        """VM is not updated when all parameters match."""
        netbox = make_mock_netbox_client()
        netbox.ensure_cluster.return_value = 20

        vm = MockRecord(
            id=1, name="test-vm", memory=4096, vcpus=2,
            cluster=MockRecord(id=20), site=MockRecord(id=10),
            platform=MockRecord(id=8), status=MockRecord(value="active"),
            comments=(
                "YC VM ID: vm-123\nZone: ru-central1-a\n"
                "Hardware Platform: standard-v3\nOS: ubuntu-22.04\nCreated: 2024-01-01"
            ),
        )
        yc_vm = {
            "name": "test-vm", "id": "vm-123", "folder_id": "f1",
            "zone_id": "ru-central1-a", "status": "RUNNING",
            "platform_id": "standard-v3", "os": "ubuntu-22.04",
            "created_at": "2024-01-01",
            "resources": {"memory": 4294967296, "cores": 2},
        }
        id_mapping = {"zones": {"ru-central1-a": 10}, "folders": {"f1": 20}}

        result = update_vm_parameters(vm, yc_vm, netbox, id_mapping)

        assert result is False
        netbox.update_vm.assert_not_called()

    def test_updates_vcpus(self):
        """VM is updated when vCPUs change."""
        netbox = make_mock_netbox_client()
        netbox.ensure_cluster.return_value = 20

        vm = MockRecord(
            id=1, name="test-vm", memory=4096, vcpus=2,
            cluster=MockRecord(id=20), site=MockRecord(id=10),
            platform=MockRecord(id=8), status=MockRecord(value="active"),
            comments=(
                "YC VM ID: vm-123\nZone: ru-central1-a\n"
                "Hardware Platform: standard-v3\nOS: ubuntu-22.04\nCreated: 2024-01-01"
            ),
        )
        yc_vm = {
            "name": "test-vm", "id": "vm-123", "folder_id": "f1",
            "zone_id": "ru-central1-a", "status": "RUNNING",
            "platform_id": "standard-v3", "os": "ubuntu-22.04",
            "created_at": "2024-01-01",
            "resources": {"memory": 4294967296, "cores": 4},
        }
        id_mapping = {"zones": {"ru-central1-a": 10}, "folders": {"f1": 20}}

        result = update_vm_parameters(vm, yc_vm, netbox, id_mapping)

        assert result is True
        call_args = netbox.update_vm.call_args
        assert call_args[0][1]["vcpus"] == 4

    def test_exception_returns_false(self):
        """If prepare_vm_data raises, returns False."""
        netbox = make_mock_netbox_client()
        netbox.ensure_cluster.side_effect = Exception("boom")

        vm = MockRecord(id=1, name="test-vm", memory=0, vcpus=1)
        yc_vm = {
            "name": "test-vm", "id": "vm-1", "folder_id": "bad",
            "resources": {"memory": 0, "cores": 1},
        }

        result = update_vm_parameters(vm, yc_vm, netbox, {"zones": {}, "folders": {}})

        assert result is False


class TestSyncVmDisks:
    """Tests for sync_vm_disks."""

    def test_creates_new_disk(self):
        """New disks from YC are created in NetBox."""
        netbox = make_mock_netbox_client()
        netbox.nb.virtualization.virtual_disks.filter.return_value = []

        vm = MockRecord(id=1, name="test-vm")
        yc_vm = {"disks": [{"name": "boot", "size": 10737418240}]}  # 10 GB in bytes

        result = sync_vm_disks(vm, yc_vm, netbox)

        assert result["created"] == 1
        netbox.create_disk.assert_called_once()
        disk_data = netbox.create_disk.call_args[0][0]
        assert disk_data["name"] == "boot"
        assert disk_data["size"] == 10240  # 10 GB in MB

    def test_unchanged_disk(self):
        """Existing disks with matching size are unchanged."""
        netbox = make_mock_netbox_client()
        existing_disk = MockRecord(id=10, name="boot", size=10240)
        netbox.nb.virtualization.virtual_disks.filter.return_value = [existing_disk]

        vm = MockRecord(id=1, name="test-vm")
        yc_vm = {"disks": [{"name": "boot", "size": 10737418240}]}

        result = sync_vm_disks(vm, yc_vm, netbox)

        assert result["unchanged"] == 1
        assert result["created"] == 0
        netbox.create_disk.assert_not_called()

    def test_removes_orphaned_disk(self):
        """Disks in NetBox but not in YC are deleted."""
        netbox = make_mock_netbox_client()
        orphan_disk = MockRecord(id=10, name="old-disk", size=5120)
        netbox.nb.virtualization.virtual_disks.filter.return_value = [orphan_disk]

        vm = MockRecord(id=1, name="test-vm")
        yc_vm = {"disks": []}  # No disks in YC

        result = sync_vm_disks(vm, yc_vm, netbox, remove_orphaned=True)

        assert result["deleted"] == 1
        orphan_disk.delete.assert_called_once()

    def test_no_orphan_removal_when_disabled(self):
        """Orphan removal is skipped when remove_orphaned=False."""
        netbox = make_mock_netbox_client()
        orphan_disk = MockRecord(id=10, name="old-disk", size=5120)
        netbox.nb.virtualization.virtual_disks.filter.return_value = [orphan_disk]

        vm = MockRecord(id=1, name="test-vm")
        yc_vm = {"disks": []}

        result = sync_vm_disks(vm, yc_vm, netbox, remove_orphaned=False)

        assert result["deleted"] == 0
        orphan_disk.delete.assert_not_called()

    def test_invalid_disk_size_skipped(self):
        """Disks with zero or non-numeric size are skipped."""
        netbox = make_mock_netbox_client()
        netbox.nb.virtualization.virtual_disks.filter.return_value = []

        vm = MockRecord(id=1, name="test-vm")
        yc_vm = {"disks": [{"name": "bad-disk", "size": 0}]}

        result = sync_vm_disks(vm, yc_vm, netbox)

        assert result["created"] == 0
        netbox.create_disk.assert_not_called()

    def test_no_virtual_disks_support(self):
        """When NetBox doesn't support virtual_disks, returns zeros."""
        netbox = make_mock_netbox_client()
        del netbox.nb.virtualization.virtual_disks

        vm = MockRecord(id=1, name="test-vm")
        yc_vm = {"disks": [{"name": "boot", "size": 10737418240}]}

        result = sync_vm_disks(vm, yc_vm, netbox)

        assert result == {"created": 0, "deleted": 0, "unchanged": 0}

    def test_disk_with_type_description(self):
        """Disk type is included in description."""
        netbox = make_mock_netbox_client()
        netbox.nb.virtualization.virtual_disks.filter.return_value = []

        vm = MockRecord(id=1, name="test-vm")
        yc_vm = {"disks": [{"name": "boot", "size": 10737418240, "type": "network-ssd"}]}

        sync_vm_disks(vm, yc_vm, netbox)

        disk_data = netbox.create_disk.call_args[0][0]
        assert disk_data["description"] == "Type: network-ssd"


class TestUpdateVmPrimaryIp:
    """Tests for update_vm_primary_ip."""

    def test_no_interfaces_returns_false(self):
        """Returns False when VM has no network interfaces."""
        netbox = make_mock_netbox_client()
        vm = MockRecord(id=1, name="test-vm", primary_ip4=None)
        yc_vm = {"network_interfaces": []}

        result = update_vm_primary_ip(vm, yc_vm, netbox)

        assert result is False

    def test_prefers_private_ip(self):
        """Private IP is preferred over public for primary."""
        netbox = make_mock_netbox_client()

        existing_ip = MockRecord(
            id=400, address="10.0.0.5/32",
            assigned_object_id=300, assigned_object_type="virtualization.vminterface",
        )
        netbox.nb.ipam.ip_addresses.filter.return_value = [existing_ip]

        vm_iface = MockRecord(id=300, name="eth0")
        netbox.nb.virtualization.interfaces.filter.return_value = [vm_iface]
        netbox.nb.virtualization.virtual_machines.filter.return_value = []

        vm = MockRecord(id=1, name="test-vm", primary_ip4=None)
        yc_vm = {
            "network_interfaces": [
                {
                    "primary_v4_address": "10.0.0.5",
                    "primary_v4_address_one_to_one_nat": "203.0.113.5",
                }
            ]
        }

        result = update_vm_primary_ip(vm, yc_vm, netbox)

        assert result is True
        netbox.set_vm_primary_ip.assert_called_once_with(1, 400, ip_version=4)

    def test_correct_primary_already_set(self):
        """Returns False when VM already has the correct primary IP."""
        netbox = make_mock_netbox_client()
        vm = MockRecord(
            id=1, name="test-vm",
            primary_ip4=MockRecord(address="10.0.0.5/32"),
        )
        yc_vm = {
            "network_interfaces": [{"primary_v4_address": "10.0.0.5"}]
        }

        result = update_vm_primary_ip(vm, yc_vm, netbox)

        assert result is False


class TestSyncVmInterfaces:
    """Tests for sync_vm_interfaces."""

    def test_creates_new_interface(self):
        """New interfaces are created."""
        netbox = make_mock_netbox_client()
        netbox.nb.virtualization.interfaces.filter.return_value = []
        created_iface = MockRecord(id=300, name="eth0")
        netbox.create_interface.return_value = created_iface
        netbox.nb.ipam.ip_addresses.filter.return_value = []

        vm = MockRecord(id=1, name="test-vm")
        yc_vm = {
            "network_interfaces": [{"primary_v4_address": "10.0.0.5"}]
        }

        result = sync_vm_interfaces(vm, yc_vm, netbox)

        assert result["interfaces_created"] == 1
        netbox.create_interface.assert_called_once()

    def test_existing_interface_reused(self):
        """Existing interfaces are not re-created."""
        netbox = make_mock_netbox_client()
        existing_iface = MockRecord(id=300, name="eth0")
        netbox.nb.virtualization.interfaces.filter.return_value = [existing_iface]
        netbox.nb.ipam.ip_addresses.filter.return_value = []

        vm = MockRecord(id=1, name="test-vm")
        yc_vm = {
            "network_interfaces": [{"primary_v4_address": "10.0.0.5"}]
        }

        result = sync_vm_interfaces(vm, yc_vm, netbox)

        assert result["interfaces_created"] == 0
        netbox.create_interface.assert_not_called()

    def test_creates_ip_for_interface(self):
        """IP addresses are created and attached to interfaces."""
        netbox = make_mock_netbox_client()
        existing_iface = MockRecord(id=300, name="eth0")
        netbox.nb.virtualization.interfaces.filter.return_value = [existing_iface]
        netbox.nb.ipam.ip_addresses.filter.return_value = []
        created_ip = MockRecord(id=400, address="10.0.0.5/32")
        netbox.create_ip.return_value = created_ip

        vm = MockRecord(id=1, name="test-vm")
        yc_vm = {
            "network_interfaces": [{"primary_v4_address": "10.0.0.5"}]
        }

        result = sync_vm_interfaces(vm, yc_vm, netbox)

        assert result["ips_created"] == 1
        netbox.create_ip.assert_called_once()

    def test_creates_public_ip(self):
        """Public NAT IPs are created."""
        netbox = make_mock_netbox_client()
        existing_iface = MockRecord(id=300, name="eth0")
        netbox.nb.virtualization.interfaces.filter.return_value = [existing_iface]
        netbox.nb.ipam.ip_addresses.filter.return_value = []
        netbox.create_ip.return_value = MockRecord(id=400, address="203.0.113.1/32")

        vm = MockRecord(id=1, name="test-vm")
        yc_vm = {
            "network_interfaces": [
                {"primary_v4_address_one_to_one_nat": "203.0.113.1"}
            ]
        }

        result = sync_vm_interfaces(vm, yc_vm, netbox)

        assert result["ips_created"] == 1

    def test_non_list_interfaces_returns_empty(self):
        """If network_interfaces is not a list, returns empty result."""
        netbox = make_mock_netbox_client()
        vm = MockRecord(id=1, name="test-vm")
        yc_vm = {"network_interfaces": "invalid"}

        result = sync_vm_interfaces(vm, yc_vm, netbox)

        assert result == {"interfaces_created": 0, "ips_created": 0, "errors": 0}

    def test_interface_creation_failure_counted(self):
        """Failed interface creation increments error count."""
        netbox = make_mock_netbox_client()
        netbox.nb.virtualization.interfaces.filter.return_value = []
        netbox.create_interface.return_value = None  # creation failed

        vm = MockRecord(id=1, name="test-vm")
        yc_vm = {"network_interfaces": [{"primary_v4_address": "10.0.0.5"}]}

        result = sync_vm_interfaces(vm, yc_vm, netbox)

        assert result["errors"] == 1
        assert result["interfaces_created"] == 0


class TestSyncVms:
    """Tests for sync_vms (top-level orchestration)."""

    def test_no_vms_returns_early(self):
        """When no VMs in YC data, returns immediately."""
        netbox = make_mock_netbox_client()
        yc_data = {"vms": []}

        result = sync_vms(yc_data, netbox, {"zones": {}, "folders": {}})

        netbox.fetch_vms.assert_not_called()
        assert result == {"created": 0, "updated": 0, "skipped": 0, "deleted": 0, "errors": 0}

    def test_creates_new_vm(self):
        """New VMs are created in NetBox."""
        netbox = make_mock_netbox_client()
        netbox.fetch_vms.return_value = []
        netbox.ensure_cluster.return_value = 20
        created_vm = MockRecord(id=100, name="new-vm")
        netbox.create_vm.return_value = created_vm

        yc_data = {
            "vms": [{
                "id": "vm-1", "name": "new-vm", "folder_id": "f1",
                "zone_id": "z1", "status": "RUNNING",
                "resources": {"memory": 4294967296, "cores": 2},
                "disks": [], "network_interfaces": [],
            }]
        }

        result = sync_vms(yc_data, netbox, {"zones": {}, "folders": {"f1": 20}}, cleanup_orphaned=False)

        netbox.create_vm.assert_called_once()
        assert result["created"] == 1

    def test_skips_vm_without_name(self):
        """VMs without a valid name are skipped."""
        netbox = make_mock_netbox_client()
        netbox.fetch_vms.return_value = []

        yc_data = {
            "vms": [{"id": "vm-1", "name": "", "resources": {"memory": 0, "cores": 1}}]
        }

        result = sync_vms(yc_data, netbox, {"zones": {}, "folders": {}}, cleanup_orphaned=False)

        netbox.create_vm.assert_not_called()
        assert result["skipped"] == 1

    def test_dry_run_doesnt_create(self):
        """In dry-run mode, VMs are not actually created."""
        netbox = make_mock_netbox_client()
        netbox.dry_run = True
        netbox.fetch_vms.return_value = []
        netbox.ensure_cluster.return_value = 20

        yc_data = {
            "vms": [{
                "id": "vm-1", "name": "new-vm", "folder_id": "f1",
                "status": "RUNNING",
                "resources": {"memory": 4294967296, "cores": 2},
            }]
        }

        sync_vms(yc_data, netbox, {"zones": {}, "folders": {"f1": 20}}, cleanup_orphaned=False)

        netbox.create_vm.assert_not_called()

    def test_calls_cleanup_when_enabled(self):
        """cleanup_orphaned_vms is called when cleanup_orphaned=True."""
        netbox = make_mock_netbox_client()
        netbox.fetch_vms.return_value = []

        yc_data = {
            "vms": [{
                "id": "vm-1", "name": "vm-1", "folder_id": "f1",
                "status": "RUNNING",
                "resources": {"memory": 4294967296, "cores": 2},
            }]
        }

        with patch("netbox_sync.sync.vms.cleanup_orphaned_vms") as mock_cleanup:
            mock_cleanup.return_value = 0
            sync_vms(yc_data, netbox, {"zones": {}, "folders": {"f1": 20}}, cleanup_orphaned=True)
            mock_cleanup.assert_called_once()

    def test_updates_existing_vm(self):
        """Existing VMs get their parameters, disks, interfaces, and primary IP updated."""
        netbox = make_mock_netbox_client()
        netbox.ensure_cluster.return_value = 20

        existing_vm = MockRecord(
            id=1, name="existing-vm", memory=2048, vcpus=2,
            cluster=MockRecord(id=20), site=MockRecord(id=10),
            platform=MockRecord(id=8), status=MockRecord(value="active"),
            comments="old comments", tags=[MockTag(id=1)],
            primary_ip4=None,
        )
        netbox.fetch_vms.return_value = [existing_vm]
        netbox.nb.virtualization.virtual_disks.filter.return_value = []
        netbox.nb.virtualization.interfaces.filter.return_value = []
        netbox.nb.ipam.ip_addresses.filter.return_value = []

        yc_data = {
            "vms": [{
                "id": "vm-1", "name": "existing-vm", "folder_id": "f1",
                "zone_id": "z1", "status": "RUNNING",
                "platform_id": "", "os": "", "created_at": "",
                "resources": {"memory": 4294967296, "cores": 2},
                "disks": [], "network_interfaces": [],
            }]
        }

        sync_vms(yc_data, netbox, {"zones": {"z1": 10}, "folders": {"f1": 20}}, cleanup_orphaned=False)

        # Should have been called for the parameter update (memory changed from 2048 -> 4096)
        netbox.update_vm.assert_called_once()

    def test_vm_creation_failure_counted(self):
        """Failed VM creation doesn't crash the sync."""
        netbox = make_mock_netbox_client()
        netbox.fetch_vms.return_value = []
        netbox.create_vm.return_value = None  # creation failed
        netbox.ensure_cluster.return_value = 20

        yc_data = {
            "vms": [{
                "id": "vm-1", "name": "fail-vm", "folder_id": "f1",
                "status": "RUNNING",
                "resources": {"memory": 4294967296, "cores": 2},
            }]
        }

        # Should not raise
        result = sync_vms(yc_data, netbox, {"zones": {}, "folders": {"f1": 20}}, cleanup_orphaned=False)
        assert result["errors"] == 1

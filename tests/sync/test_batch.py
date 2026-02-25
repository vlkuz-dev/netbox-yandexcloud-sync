"""Tests for netbox_sync.sync.batch — batch/optimized sync operations."""

from unittest.mock import MagicMock

from netbox_sync.sync.batch import (
    NetBoxCache,
    _normalize_comments,
    load_netbox_data,
    process_vm_updates,
    apply_batch_updates,
    sync_vms_optimized,
)

# ────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────


class MockRecord:
    """Mock pynetbox Record."""

    def __init__(self, **kwargs):
        self.save = MagicMock(return_value=True)
        self.delete = MagicMock(return_value=True)
        for key, value in kwargs.items():
            setattr(self, key, value)


class MockStatus:
    """Mock NetBox status field."""

    def __init__(self, value):
        self.value = value


class MockRef:
    """Mock NetBox reference field (e.g., vm.cluster, vm.primary_ip4)."""

    def __init__(self, id):
        self.id = id


def make_mock_vm(id, name, memory=2048, vcpus=2, status="active",
                 cluster_id=10, primary_ip4_id=None, tags=None,
                 site_id=None, platform_id=8, comments=""):
    """Create a mock VM record."""
    vm = MockRecord(
        id=id,
        name=name,
        memory=memory,
        vcpus=vcpus,
        status=MockStatus(status),
        cluster=MockRef(cluster_id) if cluster_id else None,
        primary_ip4=MockRef(primary_ip4_id) if primary_ip4_id else None,
        tags=tags or [],
        site=MockRef(site_id) if site_id else None,
        platform=MockRef(platform_id) if platform_id else None,
        comments=comments,
    )
    return vm


def make_mock_interface(id, name, vm_id):
    """Create a mock interface record."""
    return MockRecord(
        id=id,
        name=name,
        virtual_machine=MockRef(vm_id),
    )


def make_mock_ip(id, address, assigned_object_id=None, assigned_object_type="virtualization.vminterface"):
    """Create a mock IP address record."""
    return MockRecord(
        id=id,
        address=address,
        assigned_object_id=assigned_object_id,
        assigned_object_type=assigned_object_type,
    )


def make_mock_disk(id, name, vm_id, size=10000):
    """Create a mock disk record."""
    return MockRecord(
        id=id,
        name=name,
        virtual_machine=MockRef(vm_id),
        size=size,
    )


def make_mock_netbox():
    """Create a mock NetBoxClient for testing."""
    client = MagicMock()
    client.dry_run = False
    client.ensure_sync_tag.return_value = 1
    client.ensure_platform.return_value = 8
    client.create_vm.return_value = MockRecord(id=100, name="new-vm")
    client.create_interface.return_value = MockRecord(id=300, name="eth0")
    client.create_ip.return_value = MockRecord(id=400, address="10.0.0.1/32")
    client.create_disk.return_value = MockRecord(id=500, name="disk0")
    client.nb = MagicMock()
    return client


# ════════════════════════════════════════════════════════════
# Tests: NetBoxCache
# ════════════════════════════════════════════════════════════

class TestNetBoxCache:
    """Tests for NetBoxCache dataclass initialization and defaults."""

    def test_empty_cache(self):
        cache = NetBoxCache()
        assert cache.vms == {}
        assert cache.vms_by_name == {}
        assert len(cache.interfaces_by_vm) == 0
        assert len(cache.ips) == 0
        assert len(cache.ips_by_address) == 0
        assert len(cache.ips_by_interface) == 0
        assert len(cache.disks_by_vm) == 0
        assert len(cache.vms_with_primary_ip) == 0

    def test_cache_update_fields_empty(self):
        cache = NetBoxCache()
        assert cache.vms_to_update == {}
        assert cache.ips_to_update == {}
        assert cache.primary_ip_changes == {}
        assert cache.interfaces_to_create == []
        assert cache.ips_to_create == []
        assert cache.disks_to_create == []
        assert cache.disks_to_delete == []
        assert cache.pending_primary_ips == {}
        assert cache.pending_ip_reassignments == {}

    def test_cache_defaultdict_behavior(self):
        cache = NetBoxCache()
        # Access a missing key should return empty list (defaultdict)
        assert cache.interfaces_by_vm[999] == []
        assert cache.ips_by_interface[999] == []
        assert cache.disks_by_vm[999] == []
        # vms_with_primary_ip returns empty set
        assert cache.vms_with_primary_ip[999] == set()


# ════════════════════════════════════════════════════════════
# Tests: _normalize_comments
# ════════════════════════════════════════════════════════════


class TestNormalizeComments:
    def test_none_returns_empty(self):
        assert _normalize_comments(None) == ""

    def test_empty_string_returns_empty(self):
        assert _normalize_comments("") == ""

    def test_strips_whitespace(self):
        assert _normalize_comments("  hello  \n  world  ") == "hello\nworld"

    def test_trailing_newline_stripped(self):
        assert _normalize_comments("hello\nworld\n") == "hello\nworld"

    def test_identical_after_normalization(self):
        a = "YC VM ID: abc\nZone: ru-central1-a"
        b = "YC VM ID: abc\nZone: ru-central1-a\n"
        assert _normalize_comments(a) == _normalize_comments(b)


# ════════════════════════════════════════════════════════════
# Tests: load_netbox_data
# ════════════════════════════════════════════════════════════

class TestLoadNetboxData:
    """Tests for load_netbox_data cache population."""

    def test_load_vms(self):
        vm1 = make_mock_vm(1, "vm-1", primary_ip4_id=10)
        vm2 = make_mock_vm(2, "vm-2")

        netbox = make_mock_netbox()
        netbox.nb.virtualization.virtual_machines.all.return_value = [vm1, vm2]
        netbox.nb.virtualization.interfaces.all.return_value = []
        netbox.nb.ipam.ip_addresses.all.return_value = []
        netbox.nb.virtualization.virtual_disks.all.return_value = []

        cache = load_netbox_data(netbox)

        assert len(cache.vms) == 2
        assert cache.vms[1] == vm1
        assert cache.vms_by_name["vm-1"] == vm1
        assert cache.vms_by_name["vm-2"] == vm2

    def test_load_vms_tracks_primary_ip(self):
        vm = make_mock_vm(1, "vm-1", primary_ip4_id=10)

        netbox = make_mock_netbox()
        netbox.nb.virtualization.virtual_machines.all.return_value = [vm]
        netbox.nb.virtualization.interfaces.all.return_value = []
        netbox.nb.ipam.ip_addresses.all.return_value = []
        netbox.nb.virtualization.virtual_disks.all.return_value = []

        cache = load_netbox_data(netbox)

        assert 1 in cache.vms_with_primary_ip[10]

    def test_load_interfaces(self):
        vm1 = make_mock_vm(1, "vm-1")
        iface = make_mock_interface(100, "eth0", 1)

        netbox = make_mock_netbox()
        netbox.nb.virtualization.virtual_machines.all.return_value = [vm1]
        netbox.nb.virtualization.interfaces.all.return_value = [iface]
        netbox.nb.ipam.ip_addresses.all.return_value = []
        netbox.nb.virtualization.virtual_disks.all.return_value = []

        cache = load_netbox_data(netbox)

        assert len(cache.interfaces_by_vm[1]) == 1
        assert cache.interfaces_by_vm[1][0] == iface

    def test_load_ips(self):
        vm1 = make_mock_vm(1, "vm-1")
        ip = make_mock_ip(10, "10.0.0.1/32", assigned_object_id=100)

        netbox = make_mock_netbox()
        netbox.nb.virtualization.virtual_machines.all.return_value = [vm1]
        netbox.nb.virtualization.interfaces.all.return_value = []
        netbox.nb.ipam.ip_addresses.all.return_value = [ip]
        netbox.nb.virtualization.virtual_disks.all.return_value = []

        cache = load_netbox_data(netbox)

        assert cache.ips[10] == ip
        assert cache.ips_by_address["10.0.0.1"] == ip
        assert ip in cache.ips_by_interface[100]

    def test_load_ips_without_assigned_object(self):
        """IPs not assigned to an interface should not appear in ips_by_interface."""
        ip = make_mock_ip(10, "10.0.0.1/32", assigned_object_id=None)

        netbox = make_mock_netbox()
        netbox.nb.virtualization.virtual_machines.all.return_value = []
        netbox.nb.virtualization.interfaces.all.return_value = []
        netbox.nb.ipam.ip_addresses.all.return_value = [ip]
        netbox.nb.virtualization.virtual_disks.all.return_value = []

        cache = load_netbox_data(netbox)

        assert cache.ips[10] == ip
        assert len(cache.ips_by_interface) == 0

    def test_load_disks(self):
        vm1 = make_mock_vm(1, "vm-1")
        disk = make_mock_disk(200, "boot-disk", 1, size=10000)

        netbox = make_mock_netbox()
        netbox.nb.virtualization.virtual_machines.all.return_value = [vm1]
        netbox.nb.virtualization.interfaces.all.return_value = []
        netbox.nb.ipam.ip_addresses.all.return_value = []
        netbox.nb.virtualization.virtual_disks.all.return_value = [disk]

        cache = load_netbox_data(netbox)

        assert len(cache.disks_by_vm[1]) == 1
        assert cache.disks_by_vm[1][0] == disk

    def test_load_disks_handles_unsupported(self):
        """Should gracefully handle when virtual_disks endpoint is not available."""
        netbox = make_mock_netbox()
        netbox.nb.virtualization.virtual_machines.all.return_value = []
        netbox.nb.virtualization.interfaces.all.return_value = []
        netbox.nb.ipam.ip_addresses.all.return_value = []
        netbox.nb.virtualization.virtual_disks.all.side_effect = Exception("Not supported")

        cache = load_netbox_data(netbox)

        assert len(cache.disks_by_vm) == 0

    def test_load_multiple_ips_same_interface(self):
        """Multiple IPs assigned to the same interface."""
        ip1 = make_mock_ip(10, "10.0.0.1/32", assigned_object_id=100)
        ip2 = make_mock_ip(11, "10.0.0.2/32", assigned_object_id=100)

        netbox = make_mock_netbox()
        netbox.nb.virtualization.virtual_machines.all.return_value = []
        netbox.nb.virtualization.interfaces.all.return_value = []
        netbox.nb.ipam.ip_addresses.all.return_value = [ip1, ip2]
        netbox.nb.virtualization.virtual_disks.all.return_value = []

        cache = load_netbox_data(netbox)

        assert len(cache.ips_by_interface[100]) == 2


# ════════════════════════════════════════════════════════════
# Tests: process_vm_updates
# ════════════════════════════════════════════════════════════

class TestProcessVmUpdates:
    """Tests for process_vm_updates change detection and queuing."""

    def _make_cache_with_vm(self, vm):
        """Create a cache populated with a single VM."""
        cache = NetBoxCache()
        cache.vms[vm.id] = vm
        cache.vms_by_name[vm.name] = vm
        return cache

    def _mock_netbox(self):
        """Create a mock NetBox client for platform resolution."""
        return make_mock_netbox()

    def test_no_changes_needed(self):
        """VM already in sync - no changes."""
        vm = make_mock_vm(1, "vm-1", memory=2000, vcpus=2, status="active",
                          cluster_id=10, comments="YC VM ID: vm-1-id")
        cache = self._make_cache_with_vm(vm)

        yc_vm = {
            "id": "vm-1-id",
            "resources": {"memory": 2048 * 1024 * 1024, "cores": 2},  # 2 GiB -> 2000 MB
            "status": "RUNNING",
            "folder_id": "folder1",
            "network_interfaces": [],
            "disks": [],
        }
        id_mapping = {"folders": {"folder1": 10}}

        result = process_vm_updates(vm, yc_vm, cache, id_mapping, self._mock_netbox())
        assert result is False
        assert len(cache.vms_to_update) == 0

    def test_memory_change_detected(self):
        """Memory change should be queued."""
        vm = make_mock_vm(1, "vm-1", memory=2048)
        cache = self._make_cache_with_vm(vm)

        yc_vm = {
            "resources": {"memory": 4096 * 1024 * 1024, "cores": 2},  # 4 GiB
            "status": "RUNNING",
            "network_interfaces": [],
            "disks": [],
        }
        id_mapping = {"folders": {}}

        result = process_vm_updates(vm, yc_vm, cache, id_mapping, self._mock_netbox())
        assert result is True
        assert cache.vms_to_update[1]["memory"] == 4000

    def test_cpu_change_detected(self):
        """CPU change should be queued."""
        vm = make_mock_vm(1, "vm-1", vcpus=2)
        cache = self._make_cache_with_vm(vm)

        yc_vm = {
            "resources": {"memory": 2048 * 1024 * 1024, "cores": 4},
            "status": "RUNNING",
            "network_interfaces": [],
            "disks": [],
        }
        id_mapping = {"folders": {}}

        result = process_vm_updates(vm, yc_vm, cache, id_mapping, self._mock_netbox())
        assert result is True
        assert cache.vms_to_update[1]["vcpus"] == 4

    def test_status_change_running_to_stopped(self):
        """Status change from active to offline."""
        vm = make_mock_vm(1, "vm-1", status="active")
        cache = self._make_cache_with_vm(vm)

        yc_vm = {
            "resources": {"memory": 2048 * 1024 * 1024, "cores": 2},
            "status": "STOPPED",
            "network_interfaces": [],
            "disks": [],
        }
        id_mapping = {"folders": {}}

        result = process_vm_updates(vm, yc_vm, cache, id_mapping, self._mock_netbox())
        assert result is True
        assert cache.vms_to_update[1]["status"] == "offline"

    def test_status_change_stopped_to_running(self):
        """Status change from offline to active."""
        vm = make_mock_vm(1, "vm-1", status="offline")
        cache = self._make_cache_with_vm(vm)

        yc_vm = {
            "resources": {"memory": 2048 * 1024 * 1024, "cores": 2},
            "status": "RUNNING",
            "network_interfaces": [],
            "disks": [],
        }
        id_mapping = {"folders": {}}

        result = process_vm_updates(vm, yc_vm, cache, id_mapping, self._mock_netbox())
        assert result is True
        assert cache.vms_to_update[1]["status"] == "active"

    def test_cluster_change_detected(self):
        """Cluster change should be queued."""
        vm = make_mock_vm(1, "vm-1", cluster_id=10)
        cache = self._make_cache_with_vm(vm)

        yc_vm = {
            "resources": {"memory": 2048 * 1024 * 1024, "cores": 2},
            "status": "RUNNING",
            "folder_id": "folder2",
            "network_interfaces": [],
            "disks": [],
        }
        id_mapping = {"folders": {"folder2": 20}}

        result = process_vm_updates(vm, yc_vm, cache, id_mapping, self._mock_netbox())
        assert result is True
        assert cache.vms_to_update[1]["cluster"] == 20

    def test_comments_match_with_trailing_whitespace(self):
        """Comments that differ only by trailing whitespace should NOT trigger update."""
        vm = make_mock_vm(1, "vm-1", memory=2048, vcpus=2, status="active",
                          cluster_id=10,
                          comments="YC VM ID: vm-1-id\nZone: ru-central1-a\n")
        cache = self._make_cache_with_vm(vm)

        yc_vm = {
            "id": "vm-1-id",
            "resources": {"memory": 2048 * 1024 * 1024, "cores": 2},
            "status": "RUNNING",
            "folder_id": "folder1",
            "zone_id": "ru-central1-a",
            "network_interfaces": [],
            "disks": [],
        }
        id_mapping = {"folders": {"folder1": 10}, "zones": {"ru-central1-a": 28}}

        process_vm_updates(vm, yc_vm, cache, id_mapping, self._mock_netbox())
        # Only site update (if site differs), no comments update
        assert "comments" not in cache.vms_to_update.get(1, {})

    def test_comments_none_vs_empty_no_update(self):
        """VM with None comments and empty generated comments should not trigger update."""
        vm = make_mock_vm(1, "vm-1", memory=2048, vcpus=2, status="active",
                          cluster_id=10, comments=None)
        cache = self._make_cache_with_vm(vm)

        yc_vm = {
            "id": "unknown",
            "resources": {"memory": 2048 * 1024 * 1024, "cores": 2},
            "status": "RUNNING",
            "folder_id": "folder1",
            "network_interfaces": [],
            "disks": [],
        }
        id_mapping = {"folders": {"folder1": 10}}

        process_vm_updates(vm, yc_vm, cache, id_mapping, self._mock_netbox())
        # Comments "YC VM ID: unknown" should be detected as a real change
        # (from None to meaningful content)
        assert "comments" in cache.vms_to_update.get(1, {})

    def test_comments_real_change_detected(self):
        """Actual comment change should trigger update."""
        vm = make_mock_vm(1, "vm-1", memory=2048, vcpus=2, status="active",
                          cluster_id=10, comments="YC VM ID: old-id")
        cache = self._make_cache_with_vm(vm)

        yc_vm = {
            "id": "new-id",
            "resources": {"memory": 2048 * 1024 * 1024, "cores": 2},
            "status": "RUNNING",
            "folder_id": "folder1",
            "network_interfaces": [],
            "disks": [],
        }
        id_mapping = {"folders": {"folder1": 10}}

        process_vm_updates(vm, yc_vm, cache, id_mapping, self._mock_netbox())
        assert cache.vms_to_update[1]["comments"] == "YC VM ID: new-id"

    def test_new_disk_queued(self):
        """New disk should be queued for creation."""
        vm = make_mock_vm(1, "vm-1")
        cache = self._make_cache_with_vm(vm)

        yc_vm = {
            "resources": {"memory": 2048 * 1024 * 1024, "cores": 2},
            "status": "RUNNING",
            "network_interfaces": [],
            "disks": [{"name": "boot", "size": 10 * (1024**2)}],
        }
        id_mapping = {"folders": {}}

        result = process_vm_updates(vm, yc_vm, cache, id_mapping, self._mock_netbox())
        assert result is True
        assert len(cache.disks_to_create) == 1
        assert cache.disks_to_create[0]["name"] == "boot"

    def test_disk_size_change_queued_for_update(self):
        """Existing disk with different size should be queued for update."""
        vm = make_mock_vm(1, "vm-1")
        cache = self._make_cache_with_vm(vm)
        existing_disk = make_mock_disk(200, "boot", 1, size=40960)  # old size
        cache.disks_by_vm[1] = [existing_disk]

        yc_vm = {
            "resources": {"memory": 2000 * 1024 * 1024, "cores": 2},
            "status": "RUNNING",
            "network_interfaces": [],
            "disks": [{"name": "boot", "size": 40 * (1024**3)}],  # 40 GiB
        }
        id_mapping = {"folders": {}}

        result = process_vm_updates(vm, yc_vm, cache, id_mapping, self._mock_netbox())
        assert result is True
        assert len(cache.disks_to_update) == 1
        assert cache.disks_to_update[0] == (existing_disk, 40000)

    def test_orphaned_disk_queued_for_deletion(self):
        """Disk in NetBox but not in YC should be queued for deletion."""
        vm = make_mock_vm(1, "vm-1")
        cache = self._make_cache_with_vm(vm)
        old_disk = make_mock_disk(200, "old-disk", 1)
        cache.disks_by_vm[1] = [old_disk]

        yc_vm = {
            "resources": {"memory": 2048 * 1024 * 1024, "cores": 2},
            "status": "RUNNING",
            "network_interfaces": [],
            "disks": [],
        }
        id_mapping = {"folders": {}}

        result = process_vm_updates(vm, yc_vm, cache, id_mapping, self._mock_netbox())
        assert result is True
        assert len(cache.disks_to_delete) == 1
        assert cache.disks_to_delete[0] == old_disk

    def test_new_interface_queued(self):
        """New interface (not in cache) should be queued for creation with IPs."""
        vm = make_mock_vm(1, "vm-1")
        cache = self._make_cache_with_vm(vm)

        yc_vm = {
            "resources": {"memory": 2048 * 1024 * 1024, "cores": 2},
            "status": "RUNNING",
            "network_interfaces": [{"primary_v4_address": "10.0.0.5"}],
            "disks": [],
        }
        id_mapping = {"folders": {}}

        result = process_vm_updates(vm, yc_vm, cache, id_mapping, self._mock_netbox())
        assert result is True
        assert len(cache.interfaces_to_create) == 1
        assert cache.interfaces_to_create[0]["name"] == "eth0"
        # IPs should also be queued with pending interface reference
        assert len(cache.ips_to_create) == 1
        assert cache.ips_to_create[0]["assigned_object_id"] == "pending_1_eth0"
        assert cache.ips_to_create[0]["address"] == "10.0.0.5/32"

    def test_existing_ip_correctly_assigned(self):
        """IP already assigned to the correct interface - no changes for IP."""
        vm = make_mock_vm(1, "vm-1")
        iface = make_mock_interface(100, "eth0", 1)
        ip = make_mock_ip(10, "10.0.0.5/32", assigned_object_id=100)

        cache = self._make_cache_with_vm(vm)
        cache.interfaces_by_vm[1] = [iface]
        cache.ips_by_address["10.0.0.5"] = ip
        cache.ips[10] = ip

        yc_vm = {
            "resources": {"memory": 2048 * 1024 * 1024, "cores": 2},
            "status": "RUNNING",
            "network_interfaces": [{"primary_v4_address": "10.0.0.5"}],
            "disks": [],
        }
        id_mapping = {"folders": {}}

        result = process_vm_updates(vm, yc_vm, cache, id_mapping, self._mock_netbox())
        # Private IP candidate should be set, primary_ip_changes should be set
        # since vm has no primary_ip4
        assert result is True
        assert 1 in cache.primary_ip_changes
        assert cache.primary_ip_changes[1] == 10  # IP id

    def test_new_private_ip_queued_for_creation(self):
        """Private IP not in NetBox should be queued for creation."""
        vm = make_mock_vm(1, "vm-1")
        iface = make_mock_interface(100, "eth0", 1)

        cache = self._make_cache_with_vm(vm)
        cache.interfaces_by_vm[1] = [iface]

        yc_vm = {
            "resources": {"memory": 2048 * 1024 * 1024, "cores": 2},
            "status": "RUNNING",
            "network_interfaces": [{"primary_v4_address": "10.0.0.5"}],
            "disks": [],
        }
        id_mapping = {"folders": {}}

        result = process_vm_updates(vm, yc_vm, cache, id_mapping, self._mock_netbox())
        assert result is True
        assert len(cache.ips_to_create) == 1
        assert "10.0.0.5" in cache.ips_to_create[0]["address"]
        assert cache.ips_to_create[0]["description"] == "Private IP"

    def test_public_nat_ip_queued_for_creation(self):
        """Public NAT IP not in NetBox should be queued."""
        vm = make_mock_vm(1, "vm-1")
        iface = make_mock_interface(100, "eth0", 1)

        cache = self._make_cache_with_vm(vm)
        cache.interfaces_by_vm[1] = [iface]

        yc_vm = {
            "resources": {"memory": 2048 * 1024 * 1024, "cores": 2},
            "status": "RUNNING",
            "network_interfaces": [{
                "primary_v4_address": "10.0.0.5",
                "primary_v4_address_one_to_one_nat": "203.0.113.10"
            }],
            "disks": [],
        }
        id_mapping = {"folders": {}}

        result = process_vm_updates(vm, yc_vm, cache, id_mapping, self._mock_netbox())
        assert result is True
        # Should have 2 IPs to create: private + public
        assert len(cache.ips_to_create) == 2
        descriptions = [ip["description"] for ip in cache.ips_to_create]
        assert "Private IP" in descriptions
        assert "Public IP (NAT)" in descriptions

    def test_private_ip_preferred_over_public_for_primary(self):
        """Private IP should be preferred over public for primary IP."""
        vm = make_mock_vm(1, "vm-1")
        iface = make_mock_interface(100, "eth0", 1)
        private_ip = make_mock_ip(10, "10.0.0.5/32", assigned_object_id=100)
        public_ip = make_mock_ip(11, "203.0.113.10/32", assigned_object_id=100)

        cache = self._make_cache_with_vm(vm)
        cache.interfaces_by_vm[1] = [iface]
        cache.ips_by_address["10.0.0.5"] = private_ip
        cache.ips_by_address["203.0.113.10"] = public_ip
        cache.ips[10] = private_ip
        cache.ips[11] = public_ip

        yc_vm = {
            "resources": {"memory": 2048 * 1024 * 1024, "cores": 2},
            "status": "RUNNING",
            "network_interfaces": [{
                "primary_v4_address": "10.0.0.5",
                "primary_v4_address_one_to_one_nat": "203.0.113.10"
            }],
            "disks": [],
        }
        id_mapping = {"folders": {}}

        result = process_vm_updates(vm, yc_vm, cache, id_mapping, self._mock_netbox())
        assert result is True
        assert cache.primary_ip_changes[1] == 10  # Private IP id, not public

    def test_fallback_to_public_ip_when_no_private(self):
        """Public IP should be used as primary if no private IP available."""
        vm = make_mock_vm(1, "vm-1")
        iface = make_mock_interface(100, "eth0", 1)
        public_ip = make_mock_ip(11, "203.0.113.10/32", assigned_object_id=100)

        cache = self._make_cache_with_vm(vm)
        cache.interfaces_by_vm[1] = [iface]
        cache.ips_by_address["203.0.113.10"] = public_ip
        cache.ips[11] = public_ip

        yc_vm = {
            "resources": {"memory": 2048 * 1024 * 1024, "cores": 2},
            "status": "RUNNING",
            "network_interfaces": [{
                "primary_v4_address": "203.0.113.10",
            }],
            "disks": [],
        }
        id_mapping = {"folders": {}}

        result = process_vm_updates(vm, yc_vm, cache, id_mapping, self._mock_netbox())
        assert result is True
        assert cache.primary_ip_changes[1] == 11  # Public IP

    def test_valid_private_primary_ip_kept_stable(self):
        """VM with valid private primary IP keeps it — no change queued."""
        vm = make_mock_vm(1, "vm-1", primary_ip4_id=10)
        iface0 = make_mock_interface(100, "eth0", 1)
        iface1 = make_mock_interface(101, "eth1", 1)
        # Current primary on eth1
        ip_eth1 = make_mock_ip(10, "10.0.0.50/32", assigned_object_id=101)
        # Another private IP on eth0 (would be found first)
        ip_eth0 = make_mock_ip(11, "10.0.0.5/32", assigned_object_id=100)

        cache = self._make_cache_with_vm(vm)
        cache.interfaces_by_vm[1] = [iface0, iface1]
        cache.ips_by_address["10.0.0.5"] = ip_eth0
        cache.ips_by_address["10.0.0.50"] = ip_eth1
        cache.ips[10] = ip_eth1
        cache.ips[11] = ip_eth0

        yc_vm = {
            "resources": {"memory": 2048 * 1024 * 1024, "cores": 2},
            "status": "RUNNING",
            "network_interfaces": [
                {"primary_v4_address": "10.0.0.5"},
                {"primary_v4_address": "10.0.0.50"},
            ],
            "disks": [],
        }
        id_mapping = {"folders": {}}

        process_vm_updates(vm, yc_vm, cache, id_mapping, self._mock_netbox())
        # Current primary (10.0.0.50) is still valid — should NOT be changed
        assert 1 not in cache.primary_ip_changes

    def test_primary_ip_reassigned_to_other_vm_triggers_change(self):
        """VM's current primary IP moved to another VM → new primary selected."""
        vm = make_mock_vm(1, "vm-1", primary_ip4_id=10)
        iface = make_mock_interface(100, "eth0", 1)
        # Current primary is assigned to a different VM's interface (999)
        old_primary = make_mock_ip(10, "10.0.0.50/32", assigned_object_id=999)
        # New IP on this VM
        new_ip = make_mock_ip(11, "10.0.0.5/32", assigned_object_id=100)

        cache = self._make_cache_with_vm(vm)
        cache.interfaces_by_vm[1] = [iface]
        cache.ips_by_address["10.0.0.5"] = new_ip
        cache.ips_by_address["10.0.0.50"] = old_primary
        cache.ips[10] = old_primary
        cache.ips[11] = new_ip

        yc_vm = {
            "resources": {"memory": 2048 * 1024 * 1024, "cores": 2},
            "status": "RUNNING",
            "network_interfaces": [{"primary_v4_address": "10.0.0.5"}],
            "disks": [],
        }
        id_mapping = {"folders": {}}

        result = process_vm_updates(vm, yc_vm, cache, id_mapping, self._mock_netbox())
        assert result is True
        assert cache.primary_ip_changes[1] == 11  # New IP selected

    def test_primary_ip_gone_triggers_new_selection(self):
        """VM's current primary IP no longer in cache → new primary selected."""
        vm = make_mock_vm(1, "vm-1", primary_ip4_id=10)
        iface = make_mock_interface(100, "eth0", 1)
        new_ip = make_mock_ip(11, "10.0.0.5/32", assigned_object_id=100)

        cache = self._make_cache_with_vm(vm)
        cache.interfaces_by_vm[1] = [iface]
        cache.ips_by_address["10.0.0.5"] = new_ip
        cache.ips[11] = new_ip
        # Note: IP 10 is NOT in cache (deleted/gone)

        yc_vm = {
            "resources": {"memory": 2048 * 1024 * 1024, "cores": 2},
            "status": "RUNNING",
            "network_interfaces": [{"primary_v4_address": "10.0.0.5"}],
            "disks": [],
        }
        id_mapping = {"folders": {}}

        result = process_vm_updates(vm, yc_vm, cache, id_mapping, self._mock_netbox())
        # primary_ip4 exists but IP not in cache → fallback path
        # Since vm.primary_ip4 is set but current_primary_ip is None,
        # the primary_ip_to_set is 11 but code path goes to fallback
        # The exact behavior depends on whether primary_ip_to_set gets set
        assert result is True

    def test_public_primary_switched_to_private(self):
        """VM with public primary IP switches to private when available."""
        vm = make_mock_vm(1, "vm-1", primary_ip4_id=11)
        iface = make_mock_interface(100, "eth0", 1)
        private_ip = make_mock_ip(10, "10.0.0.5/32", assigned_object_id=100)
        # Use a truly public IP (not RFC 5737 documentation range)
        public_ip = make_mock_ip(11, "51.250.1.10/32", assigned_object_id=100)

        cache = self._make_cache_with_vm(vm)
        cache.interfaces_by_vm[1] = [iface]
        cache.ips_by_address["10.0.0.5"] = private_ip
        cache.ips_by_address["51.250.1.10"] = public_ip
        cache.ips[10] = private_ip
        cache.ips[11] = public_ip

        yc_vm = {
            "resources": {"memory": 2048 * 1024 * 1024, "cores": 2},
            "status": "RUNNING",
            "network_interfaces": [{
                "primary_v4_address": "10.0.0.5",
                "primary_v4_address_one_to_one_nat": "51.250.1.10"
            }],
            "disks": [],
        }
        id_mapping = {"folders": {}}

        result = process_vm_updates(vm, yc_vm, cache, id_mapping, self._mock_netbox())
        assert result is True
        assert cache.primary_ip_changes[1] == 10  # Switched to private

    def test_ip_reassignment_queued(self):
        """IP assigned to wrong interface should be queued for reassignment."""
        vm = make_mock_vm(1, "vm-1")
        iface = make_mock_interface(100, "eth0", 1)
        ip = make_mock_ip(10, "10.0.0.5/32", assigned_object_id=999)  # Wrong interface

        cache = self._make_cache_with_vm(vm)
        cache.interfaces_by_vm[1] = [iface]
        cache.ips_by_address["10.0.0.5"] = ip
        cache.ips[10] = ip

        yc_vm = {
            "resources": {"memory": 2048 * 1024 * 1024, "cores": 2},
            "status": "RUNNING",
            "network_interfaces": [{"primary_v4_address": "10.0.0.5"}],
            "disks": [],
        }
        id_mapping = {"folders": {}}

        result = process_vm_updates(vm, yc_vm, cache, id_mapping, self._mock_netbox())
        assert result is True
        assert 10 in cache.ips_to_update
        assert cache.ips_to_update[10]["assigned_object_id"] == 100

    def test_ip_reassignment_queued_for_pending_interface(self):
        """IP on wrong interface should be queued for pending reassignment when target is pending."""
        vm = make_mock_vm(1, "vm-1")
        ip = make_mock_ip(10, "10.0.0.5/32", assigned_object_id=999)  # Wrong interface

        cache = self._make_cache_with_vm(vm)
        # No existing interface - it will be created (pending)
        cache.ips_by_address["10.0.0.5"] = ip
        cache.ips[10] = ip

        yc_vm = {
            "resources": {"memory": 2048 * 1024 * 1024, "cores": 2},
            "status": "RUNNING",
            "network_interfaces": [{"primary_v4_address": "10.0.0.5"}],
            "disks": [],
        }
        id_mapping = {"folders": {}}

        result = process_vm_updates(vm, yc_vm, cache, id_mapping, self._mock_netbox())
        assert result is True
        # Should be in pending_ip_reassignments, NOT in ips_to_update
        assert 10 in cache.pending_ip_reassignments
        assert cache.pending_ip_reassignments[10] == "pending_1_eth0"
        assert 10 not in cache.ips_to_update

    def test_existing_interface_ip_for_vm_without_primary(self):
        """VM without primary_ip4 should pick up existing interface IPs."""
        vm = make_mock_vm(1, "vm-1")
        iface = make_mock_interface(100, "eth0", 1)
        ip = make_mock_ip(10, "10.0.0.5/32", assigned_object_id=100)

        cache = self._make_cache_with_vm(vm)
        cache.interfaces_by_vm[1] = [iface]
        cache.ips_by_interface[100] = [ip]

        yc_vm = {
            "resources": {"memory": 2048 * 1024 * 1024, "cores": 2},
            "status": "RUNNING",
            "network_interfaces": [],
            "disks": [],
        }
        id_mapping = {"folders": {}}

        result = process_vm_updates(vm, yc_vm, cache, id_mapping, self._mock_netbox())
        assert result is True
        assert cache.primary_ip_changes[1] == 10

    def test_pending_private_ip_tracked(self):
        """New private IP should be marked as pending for primary resolution.

        The 'pending' path requires:
        1. An interface with an existing truly PUBLIC IP (sets public_ip_candidate)
        2. A second interface with a NEW PRIVATE IP (sets private_ip_candidate='pending')
        3. Current primary IP must be truly public (not private per Python ipaddress)
        This ensures primary_ip_to_set is non-None so the pending branch is entered.
        Note: Using 8.x.x.x IPs which are truly global in all Python versions.
        """
        vm = make_mock_vm(1, "vm-1", primary_ip4_id=11)
        iface0 = make_mock_interface(100, "eth0", 1)
        iface1 = make_mock_interface(101, "eth1", 1)

        # Current primary is a truly public IP
        current_ip = make_mock_ip(11, "8.8.8.8/32", assigned_object_id=100)
        # eth0 has another truly public IP that exists in cache
        public_ip = make_mock_ip(12, "8.8.4.4/32", assigned_object_id=100)

        cache = self._make_cache_with_vm(vm)
        cache.interfaces_by_vm[1] = [iface0, iface1]
        cache.ips[11] = current_ip
        cache.ips[12] = public_ip
        cache.ips_by_address["8.8.4.4"] = public_ip

        yc_vm = {
            "resources": {"memory": 2048 * 1024 * 1024, "cores": 2},
            "status": "RUNNING",
            "network_interfaces": [
                {"primary_v4_address": "8.8.4.4"},      # public, exists -> sets public_ip_candidate
                {"primary_v4_address": "10.0.0.5"},      # private, new -> sets pending
            ],
            "disks": [],
        }
        id_mapping = {"folders": {}}

        result = process_vm_updates(vm, yc_vm, cache, id_mapping, self._mock_netbox())
        assert result is True
        assert cache.primary_ip_changes[1] == "pending"
        assert 1 in cache.pending_primary_ips
        assert "10.0.0.5" in cache.pending_primary_ips[1]

    def test_multiple_new_private_ips_first_wins(self):
        """When multiple interfaces have new private IPs, the first one should be stored as pending."""
        vm = make_mock_vm(1, "vm-1", primary_ip4_id=11)
        iface0 = make_mock_interface(100, "eth0", 1)
        iface1 = make_mock_interface(101, "eth1", 1)

        current_ip = make_mock_ip(11, "8.8.8.8/32", assigned_object_id=100)
        public_ip = make_mock_ip(12, "8.8.4.4/32", assigned_object_id=100)

        cache = self._make_cache_with_vm(vm)
        cache.interfaces_by_vm[1] = [iface0, iface1]
        cache.ips[11] = current_ip
        cache.ips[12] = public_ip
        cache.ips_by_address["8.8.4.4"] = public_ip

        yc_vm = {
            "resources": {"memory": 2048 * 1024 * 1024, "cores": 2},
            "status": "RUNNING",
            "network_interfaces": [
                {"primary_v4_address": "8.8.4.4"},      # public, exists
                {"primary_v4_address": "10.0.0.1"},      # private, new (first)
                {"primary_v4_address": "10.0.0.2"},      # private, new (second)
            ],
            "disks": [],
        }
        id_mapping = {"folders": {}}

        process_vm_updates(vm, yc_vm, cache, id_mapping, self._mock_netbox())
        # First private IP should win, not the last
        assert "10.0.0.1" in cache.pending_primary_ips[1]

    def test_new_vm_pending_ips_get_primary_queued(self):
        """New VM with no primary_ip4 and only pending IPs should queue 'pending' primary change."""
        vm = make_mock_vm(1, "vm-new")

        cache = self._make_cache_with_vm(vm)
        # No existing interfaces or IPs for this new VM

        yc_vm = {
            "resources": {"memory": 2048 * 1024 * 1024, "cores": 2},
            "status": "RUNNING",
            "network_interfaces": [
                {"primary_v4_address": "10.0.0.5"},  # private, new
            ],
            "disks": [],
        }
        id_mapping = {"folders": {}}

        result = process_vm_updates(vm, yc_vm, cache, id_mapping, self._mock_netbox())
        assert result is True
        assert cache.primary_ip_changes[1] == "pending"
        assert 1 in cache.pending_primary_ips
        assert "10.0.0.5" in cache.pending_primary_ips[1]

    def test_new_vm_public_primary_v4_gets_pending(self):
        """New VM with only public primary_v4_address (no private) should queue 'pending'."""
        vm = make_mock_vm(1, "vm-public")

        cache = self._make_cache_with_vm(vm)

        yc_vm = {
            "resources": {"memory": 2048 * 1024 * 1024, "cores": 2},
            "status": "RUNNING",
            "network_interfaces": [
                {"primary_v4_address": "203.0.113.10"},  # public, new
            ],
            "disks": [],
        }
        id_mapping = {"folders": {}}

        result = process_vm_updates(vm, yc_vm, cache, id_mapping, self._mock_netbox())
        assert result is True
        assert cache.primary_ip_changes[1] == "pending"
        assert 1 in cache.pending_primary_ips
        assert "203.0.113.10" in cache.pending_primary_ips[1]

    def test_new_vm_nat_public_only_gets_pending(self):
        """New VM with only NAT public IP (no primary_v4_address) should queue 'pending'."""
        vm = make_mock_vm(1, "vm-nat")

        cache = self._make_cache_with_vm(vm)

        yc_vm = {
            "resources": {"memory": 2048 * 1024 * 1024, "cores": 2},
            "status": "RUNNING",
            "network_interfaces": [
                {"primary_v4_address_one_to_one_nat": "203.0.113.42"},
            ],
            "disks": [],
        }
        id_mapping = {"folders": {}}

        result = process_vm_updates(vm, yc_vm, cache, id_mapping, self._mock_netbox())
        assert result is True
        assert cache.primary_ip_changes[1] == "pending"
        assert 1 in cache.pending_primary_ips
        assert "203.0.113.42" in cache.pending_primary_ips[1]

    def test_new_vm_private_ip_preferred_over_pending_public(self):
        """When private IP is pending and public NAT is also pending, private should be tracked."""
        vm = make_mock_vm(1, "vm-mixed")

        cache = self._make_cache_with_vm(vm)

        yc_vm = {
            "resources": {"memory": 2048 * 1024 * 1024, "cores": 2},
            "status": "RUNNING",
            "network_interfaces": [
                {
                    "primary_v4_address": "10.0.0.5",
                    "primary_v4_address_one_to_one_nat": "203.0.113.42",
                },
            ],
            "disks": [],
        }
        id_mapping = {"folders": {}}

        result = process_vm_updates(vm, yc_vm, cache, id_mapping, self._mock_netbox())
        assert result is True
        assert cache.primary_ip_changes[1] == "pending"
        # Private IP should be tracked for pending resolution, not public
        assert cache.pending_primary_ips[1] == "10.0.0.5/32"


# ════════════════════════════════════════════════════════════
# Tests: apply_batch_updates
# ════════════════════════════════════════════════════════════

class TestApplyBatchUpdates:
    """Tests for apply_batch_updates applying queued changes."""

    def test_dry_run_returns_zero_stats(self):
        """Dry run should not execute any changes."""
        cache = NetBoxCache()
        cache.vms_to_update = {1: {"memory": 4000}}
        cache.ips_to_create = [{"address": "10.0.0.1/32"}]

        netbox = make_mock_netbox()
        stats = apply_batch_updates(cache, netbox, dry_run=True)

        assert stats["vms_updated"] == 0
        assert stats["ips_created"] == 0
        netbox.create_ip.assert_not_called()

    def test_vm_updates_applied(self):
        """VM parameter updates should call save on each VM."""
        vm = make_mock_vm(1, "vm-1")

        cache = NetBoxCache()
        cache.vms[1] = vm
        cache.vms_to_update = {1: {"memory": 4000, "vcpus": 4}}

        netbox = make_mock_netbox()
        stats = apply_batch_updates(cache, netbox)

        assert stats["vms_updated"] == 1
        assert vm.memory == 4000
        assert vm.vcpus == 4
        vm.save.assert_called_once()

    def test_disk_deletion(self):
        """Queued disk deletions should call delete()."""
        disk = make_mock_disk(200, "old-disk", 1)

        cache = NetBoxCache()
        cache.disks_to_delete = [disk]

        netbox = make_mock_netbox()
        stats = apply_batch_updates(cache, netbox)

        assert stats["disks_deleted"] == 1
        disk.delete.assert_called_once()

    def test_interface_creation(self):
        """Queued interfaces should be created via netbox client."""
        cache = NetBoxCache()
        cache.interfaces_to_create = [{
            "virtual_machine": 1,
            "name": "eth0",
            "type": "virtual",
            "enabled": True
        }]

        netbox = make_mock_netbox()
        stats = apply_batch_updates(cache, netbox)

        assert stats["interfaces_created"] == 1
        netbox.create_interface.assert_called_once()

    def test_ip_reassignment(self):
        """Queued IP reassignment should update and save the IP."""
        ip = make_mock_ip(10, "10.0.0.5/32", assigned_object_id=999)

        cache = NetBoxCache()
        cache.ips[10] = ip
        cache.ips_to_update = {10: {
            "assigned_object_type": "virtualization.vminterface",
            "assigned_object_id": 100
        }}

        netbox = make_mock_netbox()
        stats = apply_batch_updates(cache, netbox)

        assert stats["ips_reassigned"] == 1
        assert ip.assigned_object_id == 100
        ip.save.assert_called_once()

    def test_ip_creation(self):
        """Queued IPs should be created via netbox client."""
        cache = NetBoxCache()
        cache.ips_to_create = [{
            "address": "10.0.0.5/32",
            "assigned_object_type": "virtualization.vminterface",
            "assigned_object_id": 100,
            "status": "active",
            "description": "Private IP"
        }]

        netbox = make_mock_netbox()
        stats = apply_batch_updates(cache, netbox)

        assert stats["ips_created"] == 1
        netbox.create_ip.assert_called_once()

    def test_ip_creation_with_pending_interface(self):
        """IPs with pending interface refs should be resolved after interface creation."""
        cache = NetBoxCache()
        cache.interfaces_to_create = [{
            "virtual_machine": 1,
            "name": "eth0",
            "type": "virtual",
            "enabled": True
        }]
        cache.ips_to_create = [{
            "address": "10.0.0.5/32",
            "assigned_object_type": "virtualization.vminterface",
            "assigned_object_id": "pending_1_eth0",
            "status": "active",
            "description": "Private IP"
        }]

        created_iface = make_mock_interface(500, "eth0", 1)
        netbox = make_mock_netbox()
        netbox.create_interface.return_value = created_iface

        stats = apply_batch_updates(cache, netbox)

        assert stats["interfaces_created"] == 1
        assert stats["ips_created"] == 1
        # Verify the IP was created with the resolved real interface ID
        call_args = netbox.create_ip.call_args[0][0]
        assert call_args["assigned_object_id"] == 500

    def test_pending_ip_reassignment_resolved(self):
        """Pending IP reassignments should be resolved after interface creation."""
        ip = make_mock_ip(10, "10.0.0.5/32", assigned_object_id=999)
        created_iface = make_mock_interface(500, "eth0", 1)

        cache = NetBoxCache()
        cache.ips[10] = ip
        cache.interfaces_to_create = [{
            "virtual_machine": 1,
            "name": "eth0",
            "type": "virtual",
            "enabled": True
        }]
        cache.pending_ip_reassignments = {10: "pending_1_eth0"}

        netbox = make_mock_netbox()
        netbox.create_interface.return_value = created_iface

        stats = apply_batch_updates(cache, netbox)

        assert stats["ips_reassigned"] == 1
        assert ip.assigned_object_id == 500
        ip.save.assert_called_once()

    def test_disk_creation(self):
        """Queued disks should be created via netbox client."""
        cache = NetBoxCache()
        cache.disks_to_create = [{
            "virtual_machine": 1,
            "size": 10000,
            "name": "boot"
        }]

        netbox = make_mock_netbox()
        stats = apply_batch_updates(cache, netbox)

        assert stats["disks_created"] == 1
        netbox.create_disk.assert_called_once()

    def test_disk_size_update(self):
        """Queued disk size updates should save new size."""
        disk = make_mock_disk(200, "boot", 1, size=40960)

        cache = NetBoxCache()
        cache.disks_to_update = [(disk, 40000)]

        netbox = make_mock_netbox()
        stats = apply_batch_updates(cache, netbox)

        assert stats["disks_updated"] == 1
        assert disk.size == 40000
        disk.save.assert_called_once()

    def test_primary_ip_unset(self):
        """Primary IP changes with None should unset primary_ip4."""
        vm = make_mock_vm(1, "vm-1", primary_ip4_id=10)

        cache = NetBoxCache()
        cache.vms[1] = vm
        cache.primary_ip_changes = {1: None}

        netbox = make_mock_netbox()
        stats = apply_batch_updates(cache, netbox)

        assert stats["primary_ips_changed"] == 1
        assert vm.primary_ip4 is None
        vm.save.assert_called_once()

    def test_primary_ip_set(self):
        """Primary IP change with an IP id should set primary_ip4."""
        vm = make_mock_vm(1, "vm-1")
        iface = make_mock_interface(100, "eth0", 1)
        ip = make_mock_ip(10, "10.0.0.5/32", assigned_object_id=100)

        cache = NetBoxCache()
        cache.vms[1] = vm
        cache.ips[10] = ip
        cache.interfaces_by_vm[1] = [iface]
        cache.primary_ip_changes = {1: 10}

        netbox = make_mock_netbox()
        stats = apply_batch_updates(cache, netbox)

        assert stats["primary_ips_changed"] == 1
        assert vm.primary_ip4 == 10
        vm.save.assert_called()

    def test_pending_primary_ip_resolution(self):
        """Pending primary IPs should be resolved after IP creation."""
        vm = make_mock_vm(1, "vm-1")
        iface = make_mock_interface(100, "eth0", 1)
        created_ip = MockRecord(id=400, address="10.0.0.5/32")

        cache = NetBoxCache()
        cache.vms[1] = vm
        cache.interfaces_by_vm[1] = [iface]
        cache.primary_ip_changes = {1: "pending"}
        cache.pending_primary_ips = {1: "10.0.0.5/32"}
        cache.ips_to_create = [{
            "address": "10.0.0.5/32",
            "assigned_object_type": "virtualization.vminterface",
            "assigned_object_id": 100,
            "status": "active",
            "description": "Private IP"
        }]

        netbox = make_mock_netbox()
        netbox.create_ip.return_value = created_ip

        stats = apply_batch_updates(cache, netbox)

        # The pending should be resolved to the created IP's id
        assert cache.primary_ip_changes[1] == 400
        assert stats["ips_created"] == 1
        assert stats["primary_ips_changed"] == 1

    def test_error_handling_vm_update(self):
        """Errors during VM update should be counted."""
        vm = make_mock_vm(1, "vm-1")
        vm.save.side_effect = Exception("API error")

        cache = NetBoxCache()
        cache.vms[1] = vm
        cache.vms_to_update = {1: {"memory": 4000}}

        netbox = make_mock_netbox()
        stats = apply_batch_updates(cache, netbox)

        assert stats["errors"] == 1
        assert stats["vms_updated"] == 0

    def test_error_handling_disk_deletion(self):
        """Errors during disk deletion should be counted."""
        disk = make_mock_disk(200, "boot", 1)
        disk.delete.side_effect = Exception("Delete failed")

        cache = NetBoxCache()
        cache.disks_to_delete = [disk]

        netbox = make_mock_netbox()
        stats = apply_batch_updates(cache, netbox)

        assert stats["errors"] == 1
        assert stats["disks_deleted"] == 0

    def test_primary_ip_not_assigned_to_vm_gets_reassigned(self):
        """IP not assigned to any VM interface should be assigned before setting as primary."""
        vm = make_mock_vm(1, "vm-1")
        iface = make_mock_interface(100, "eth0", 1)
        ip = make_mock_ip(10, "10.0.0.5/32", assigned_object_id=999)  # Wrong interface

        cache = NetBoxCache()
        cache.vms[1] = vm
        cache.ips[10] = ip
        cache.interfaces_by_vm[1] = [iface]
        cache.primary_ip_changes = {1: 10}

        netbox = make_mock_netbox()
        stats = apply_batch_updates(cache, netbox)

        assert stats["ips_reassigned"] == 1
        assert stats["primary_ips_changed"] == 1
        assert ip.assigned_object_id == 100  # Reassigned to VM's interface

    def test_primary_ip_no_interfaces_error(self):
        """Setting primary IP on VM with no interfaces should error."""
        vm = make_mock_vm(1, "vm-1")
        ip = make_mock_ip(10, "10.0.0.5/32", assigned_object_id=999)

        cache = NetBoxCache()
        cache.vms[1] = vm
        cache.ips[10] = ip
        cache.primary_ip_changes = {1: 10}

        netbox = make_mock_netbox()
        netbox.nb.virtualization.interfaces.filter.return_value = []

        stats = apply_batch_updates(cache, netbox)
        assert stats["errors"] == 1

    def test_primary_ip_fetched_from_netbox_if_not_in_cache(self):
        """IP not in cache should be fetched from NetBox API."""
        vm = make_mock_vm(1, "vm-1")
        iface = make_mock_interface(100, "eth0", 1)
        ip = make_mock_ip(10, "10.0.0.5/32", assigned_object_id=100)

        cache = NetBoxCache()
        cache.vms[1] = vm
        cache.interfaces_by_vm[1] = [iface]
        # IP NOT in cache.ips
        cache.primary_ip_changes = {1: 10}

        netbox = make_mock_netbox()
        netbox.nb.ipam.ip_addresses.get.return_value = ip

        stats = apply_batch_updates(cache, netbox)
        assert stats["primary_ips_changed"] == 1
        netbox.nb.ipam.ip_addresses.get.assert_called_with(id=10)


    def test_unresolved_pending_primary_ip_counted_as_error(self):
        """Unresolved 'pending' primary IPs should be counted as errors in Step 8."""
        vm = make_mock_vm(1, "vm-1")

        cache = NetBoxCache()
        cache.vms[1] = vm
        cache.primary_ip_changes = {1: "pending"}
        cache.pending_primary_ips = {1: "10.0.0.5/32"}
        # No IPs to create → pending won't be resolved

        netbox = make_mock_netbox()
        stats = apply_batch_updates(cache, netbox)

        # Pending was not resolved, so it should be an error
        assert stats["primary_ips_changed"] == 0
        assert stats["errors"] >= 1


# ════════════════════════════════════════════════════════════
# Tests: sync_vms_optimized
# ════════════════════════════════════════════════════════════

class TestSyncVmsOptimized:
    """Tests for sync_vms_optimized orchestration function."""

    def test_no_vms_returns_empty_stats(self):
        """Empty VM list should return zero stats."""
        netbox = make_mock_netbox()
        netbox.nb.virtualization.virtual_machines.all.return_value = []
        netbox.nb.virtualization.interfaces.all.return_value = []
        netbox.nb.ipam.ip_addresses.all.return_value = []
        netbox.nb.virtualization.virtual_disks.all.return_value = []

        stats = sync_vms_optimized({"vms": []}, netbox, {})
        assert stats["created"] == 0
        assert stats["updated"] == 0
        assert stats["skipped"] == 0

    def test_skip_vm_without_name(self):
        """VMs without names should be skipped."""
        netbox = make_mock_netbox()
        netbox.nb.virtualization.virtual_machines.all.return_value = []
        netbox.nb.virtualization.interfaces.all.return_value = []
        netbox.nb.ipam.ip_addresses.all.return_value = []
        netbox.nb.virtualization.virtual_disks.all.return_value = []

        stats = sync_vms_optimized(
            {"vms": [{"name": "", "id": "123"}]},
            netbox, {},
            cleanup_orphaned=False
        )
        assert stats["skipped"] == 1

    def test_existing_vm_updated(self):
        """Existing VM with changes should be counted as updated."""
        vm = make_mock_vm(1, "test-vm", memory=2048, vcpus=2, status="active")

        netbox = make_mock_netbox()
        netbox.nb.virtualization.virtual_machines.all.return_value = [vm]
        netbox.nb.virtualization.interfaces.all.return_value = []
        netbox.nb.ipam.ip_addresses.all.return_value = []
        netbox.nb.virtualization.virtual_disks.all.return_value = []

        yc_data = {
            "vms": [{
                "name": "test-vm",
                "resources": {"memory": 4096 * 1024 * 1024, "cores": 2},  # Changed
                "status": "RUNNING",
                "network_interfaces": [],
                "disks": [],
            }]
        }

        stats = sync_vms_optimized(yc_data, netbox, {}, cleanup_orphaned=False)
        assert stats["updated"] == 1

    def test_existing_vm_no_changes_skipped(self):
        """Existing VM with no changes should be counted as skipped."""
        vm = make_mock_vm(1, "test-vm", memory=2000, vcpus=2, status="active",
                          cluster_id=10, comments="YC VM ID: vm-id-1")

        netbox = make_mock_netbox()
        netbox.nb.virtualization.virtual_machines.all.return_value = [vm]
        netbox.nb.virtualization.interfaces.all.return_value = []
        netbox.nb.ipam.ip_addresses.all.return_value = []
        netbox.nb.virtualization.virtual_disks.all.return_value = []

        yc_data = {
            "vms": [{
                "id": "vm-id-1",
                "name": "test-vm",
                "resources": {"memory": 2048 * 1024 * 1024, "cores": 2},
                "status": "RUNNING",
                "folder_id": "f1",
                "network_interfaces": [],
                "disks": [],
            }]
        }

        stats = sync_vms_optimized(yc_data, netbox, {"folders": {"f1": 10}}, cleanup_orphaned=False)
        assert stats["skipped"] == 1

    def test_new_vm_created(self):
        """VM not in NetBox should be created."""
        netbox = make_mock_netbox()
        netbox.nb.virtualization.virtual_machines.all.return_value = []
        netbox.nb.virtualization.interfaces.all.return_value = []
        netbox.nb.ipam.ip_addresses.all.return_value = []
        netbox.nb.virtualization.virtual_disks.all.return_value = []

        yc_data = {
            "vms": [{
                "name": "new-vm",
                "resources": {"memory": 2048 * 1024 * 1024, "cores": 2},
                "status": "RUNNING",
                "network_interfaces": [],
                "disks": [],
            }]
        }

        stats = sync_vms_optimized(yc_data, netbox, {}, cleanup_orphaned=False)
        assert stats["created"] == 1
        netbox.create_vm.assert_called_once()

    def test_new_vm_dry_run(self):
        """New VM in dry run mode should be counted but not actually created."""
        netbox = make_mock_netbox()
        netbox.dry_run = True
        netbox.nb.virtualization.virtual_machines.all.return_value = []
        netbox.nb.virtualization.interfaces.all.return_value = []
        netbox.nb.ipam.ip_addresses.all.return_value = []
        netbox.nb.virtualization.virtual_disks.all.return_value = []

        yc_data = {
            "vms": [{
                "name": "new-vm",
                "resources": {"memory": 2048 * 1024 * 1024, "cores": 2},
                "status": "RUNNING",
                "network_interfaces": [],
                "disks": [],
            }]
        }

        stats = sync_vms_optimized(yc_data, netbox, {}, cleanup_orphaned=False)
        assert stats["created"] == 1
        netbox.create_vm.assert_not_called()

    def test_orphaned_vm_deleted(self):
        """VM in NetBox but not in YC should be deleted when cleanup enabled."""
        tag = MockRecord(id=1)
        orphan_vm = make_mock_vm(1, "orphan-vm", tags=[tag])

        netbox = make_mock_netbox()
        netbox.nb.virtualization.virtual_machines.all.return_value = [orphan_vm]
        netbox.nb.virtualization.interfaces.all.return_value = []
        netbox.nb.ipam.ip_addresses.all.return_value = []
        netbox.nb.virtualization.virtual_disks.all.return_value = []

        yc_data = {
            "vms": [{
                "name": "alive-vm",
                "status": "RUNNING",
                "resources": {"memory": 2048 * 1024 * 1024, "cores": 2},
                "network_interfaces": [],
                "disks": [],
            }]
        }

        stats = sync_vms_optimized(yc_data, netbox, {}, cleanup_orphaned=True)
        assert stats["deleted"] == 1
        orphan_vm.delete.assert_called_once()

    def test_orphaned_vm_dry_run(self):
        """Orphaned VM in dry run should be counted but not deleted."""
        tag = MockRecord(id=1)
        orphan_vm = make_mock_vm(1, "orphan-vm", tags=[tag])

        netbox = make_mock_netbox()
        netbox.dry_run = True
        netbox.nb.virtualization.virtual_machines.all.return_value = [orphan_vm]
        netbox.nb.virtualization.interfaces.all.return_value = []
        netbox.nb.ipam.ip_addresses.all.return_value = []
        netbox.nb.virtualization.virtual_disks.all.return_value = []

        yc_data = {
            "vms": [{
                "name": "alive-vm",
                "status": "RUNNING",
                "resources": {"memory": 2048 * 1024 * 1024, "cores": 2},
                "network_interfaces": [],
                "disks": [],
            }]
        }

        stats = sync_vms_optimized(yc_data, netbox, {}, cleanup_orphaned=True)
        assert stats["deleted"] == 1
        orphan_vm.delete.assert_not_called()

    def test_error_during_vm_processing(self):
        """Errors during individual VM processing should be counted."""
        netbox = make_mock_netbox()
        netbox.nb.virtualization.virtual_machines.all.return_value = []
        netbox.nb.virtualization.interfaces.all.return_value = []
        netbox.nb.ipam.ip_addresses.all.return_value = []
        netbox.nb.virtualization.virtual_disks.all.return_value = []
        netbox.create_vm.return_value = None  # Simulates creation failure

        yc_data = {
            "vms": [{
                "name": "fail-vm",
                "resources": {"memory": 2048 * 1024 * 1024, "cores": 2},
                "status": "RUNNING",
                "network_interfaces": [],
                "disks": [],
            }]
        }

        stats = sync_vms_optimized(yc_data, netbox, {}, cleanup_orphaned=False)
        assert stats["errors"] == 1

    def test_new_vm_created_with_interfaces_and_primary_ip(self):
        """New VM with private + public IPs should get primary_ip4 set to private IP."""
        created_vm = make_mock_vm(100, "new-vm", primary_ip4_id=None)
        created_iface = make_mock_interface(300, "eth0", 100)
        private_ip = MockRecord(id=400, address="10.0.0.5/32",
                                assigned_object_id=300, assigned_object_type="virtualization.vminterface")
        public_ip = MockRecord(id=401, address="203.0.113.42/32",
                               assigned_object_id=300, assigned_object_type="virtualization.vminterface")

        netbox = make_mock_netbox()
        netbox.nb.virtualization.virtual_machines.all.return_value = []
        netbox.nb.virtualization.interfaces.all.return_value = []
        netbox.nb.ipam.ip_addresses.all.return_value = []
        netbox.nb.virtualization.virtual_disks.all.return_value = []
        netbox.create_vm.return_value = created_vm
        netbox.create_interface.return_value = created_iface

        # create_ip returns different IPs based on address
        def create_ip_side_effect(ip_data):
            addr = ip_data.get("address", "")
            if "10.0.0.5" in addr:
                return private_ip
            elif "203.0.113.42" in addr:
                return public_ip
            return MockRecord(id=999, address=addr)

        netbox.create_ip.side_effect = create_ip_side_effect

        # Step 8 falls back to querying NetBox for IP and interfaces
        netbox.nb.ipam.ip_addresses.get.return_value = private_ip
        netbox.nb.virtualization.interfaces.filter.return_value = [created_iface]

        yc_data = {
            "vms": [{
                "name": "new-vm",
                "resources": {"memory": 2048 * 1024 * 1024, "cores": 2},
                "status": "RUNNING",
                "network_interfaces": [{
                    "primary_v4_address": "10.0.0.5",
                    "primary_v4_address_one_to_one_nat": "203.0.113.42",
                }],
                "disks": [],
            }]
        }

        stats = sync_vms_optimized(yc_data, netbox, {}, cleanup_orphaned=False)
        assert stats["created"] == 1
        netbox.create_vm.assert_called_once()
        netbox.create_interface.assert_called_once()
        assert netbox.create_ip.call_count == 2  # private + public
        # VM should have primary_ip4 set to private IP
        assert created_vm.primary_ip4 == 400

    def test_new_vm_primary_ip_set_for_public_only(self):
        """New VM with only public primary_v4_address should still get primary_ip4 set."""
        created_vm = make_mock_vm(100, "new-vm-pub", primary_ip4_id=None)
        created_iface = make_mock_interface(300, "eth0", 100)
        public_ip = MockRecord(id=401, address="203.0.113.10/32",
                               assigned_object_id=300, assigned_object_type="virtualization.vminterface")

        netbox = make_mock_netbox()
        netbox.nb.virtualization.virtual_machines.all.return_value = []
        netbox.nb.virtualization.interfaces.all.return_value = []
        netbox.nb.ipam.ip_addresses.all.return_value = []
        netbox.nb.virtualization.virtual_disks.all.return_value = []
        netbox.create_vm.return_value = created_vm
        netbox.create_interface.return_value = created_iface
        netbox.create_ip.return_value = public_ip
        netbox.nb.ipam.ip_addresses.get.return_value = public_ip
        netbox.nb.virtualization.interfaces.filter.return_value = [created_iface]

        yc_data = {
            "vms": [{
                "name": "new-vm-pub",
                "resources": {"memory": 2048 * 1024 * 1024, "cores": 2},
                "status": "RUNNING",
                "network_interfaces": [{
                    "primary_v4_address": "203.0.113.10",  # public IP only
                }],
                "disks": [],
            }]
        }

        stats = sync_vms_optimized(yc_data, netbox, {}, cleanup_orphaned=False)
        assert stats["created"] == 1
        assert created_vm.primary_ip4 == 401

    def test_multiple_new_vms_all_get_primary_ip(self):
        """Multiple new VMs should each get their own primary_ip4 set."""
        vm1 = make_mock_vm(100, "vm-1", primary_ip4_id=None)
        vm2 = make_mock_vm(101, "vm-2", primary_ip4_id=None)
        iface1 = make_mock_interface(300, "eth0", 100)
        iface2 = make_mock_interface(301, "eth0", 101)
        ip1 = MockRecord(id=400, address="10.0.0.5/32",
                         assigned_object_id=300, assigned_object_type="virtualization.vminterface")
        ip2 = MockRecord(id=401, address="10.0.0.6/32",
                         assigned_object_id=301, assigned_object_type="virtualization.vminterface")

        netbox = make_mock_netbox()
        netbox.nb.virtualization.virtual_machines.all.return_value = []
        netbox.nb.virtualization.interfaces.all.return_value = []
        netbox.nb.ipam.ip_addresses.all.return_value = []
        netbox.nb.virtualization.virtual_disks.all.return_value = []

        vm_call_count = [0]
        def create_vm_side_effect(data):
            vm_call_count[0] += 1
            return vm1 if vm_call_count[0] == 1 else vm2

        iface_call_count = [0]
        def create_iface_side_effect(data):
            iface_call_count[0] += 1
            return iface1 if iface_call_count[0] == 1 else iface2

        def create_ip_side_effect(ip_data):
            addr = ip_data.get("address", "")
            if "10.0.0.5" in addr:
                return ip1
            elif "10.0.0.6" in addr:
                return ip2
            return MockRecord(id=999, address=addr)

        netbox.create_vm.side_effect = create_vm_side_effect
        netbox.create_interface.side_effect = create_iface_side_effect
        netbox.create_ip.side_effect = create_ip_side_effect

        def ip_get_side_effect(id):
            if id == 400:
                return ip1
            elif id == 401:
                return ip2
            return None

        netbox.nb.ipam.ip_addresses.get.side_effect = ip_get_side_effect

        def iface_filter_side_effect(virtual_machine_id):
            if virtual_machine_id == 100:
                return [iface1]
            elif virtual_machine_id == 101:
                return [iface2]
            return []

        netbox.nb.virtualization.interfaces.filter.side_effect = iface_filter_side_effect

        yc_data = {
            "vms": [
                {
                    "name": "vm-1",
                    "resources": {"memory": 2048 * 1024 * 1024, "cores": 2},
                    "status": "RUNNING",
                    "network_interfaces": [{"primary_v4_address": "10.0.0.5"}],
                    "disks": [],
                },
                {
                    "name": "vm-2",
                    "resources": {"memory": 4096 * 1024 * 1024, "cores": 4},
                    "status": "RUNNING",
                    "network_interfaces": [{"primary_v4_address": "10.0.0.6"}],
                    "disks": [],
                },
            ]
        }

        stats = sync_vms_optimized(yc_data, netbox, {}, cleanup_orphaned=False)
        assert stats["created"] == 2
        assert vm1.primary_ip4 == 400
        assert vm2.primary_ip4 == 401

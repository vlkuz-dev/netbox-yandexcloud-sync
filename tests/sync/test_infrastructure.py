"""Tests for netbox_sync.sync.infrastructure module."""

from unittest.mock import patch

from netbox_sync.sync.infrastructure import sync_infrastructure
from tests.conftest import MockRecord, make_mock_netbox_client


class TestSyncInfrastructure:
    """Tests for sync_infrastructure."""

    def test_creates_sites_from_zones(self):
        """Each YC zone creates a corresponding site."""
        netbox = make_mock_netbox_client()
        netbox.ensure_site.side_effect = [10, 11]

        yc_data = {
            "zones": [
                {"id": "ru-central1-a", "name": "ru-central1-a"},
                {"id": "ru-central1-b", "name": "ru-central1-b"},
            ],
            "folders": [],
            "subnets": [],
        }

        result = sync_infrastructure(yc_data, netbox, cleanup_orphaned=False)

        assert result["zones"] == {
            "ru-central1-a": 10,
            "ru-central1-b": 11,
        }
        assert netbox.ensure_site.call_count == 2

    def test_creates_clusters_from_folders(self):
        """Each YC folder creates a corresponding cluster."""
        netbox = make_mock_netbox_client()
        netbox.ensure_cluster.side_effect = [20, 21]

        yc_data = {
            "zones": [],
            "folders": [
                {"id": "f1", "name": "dev", "cloud_name": "cloud-1", "description": "Dev folder"},
                {"id": "f2", "name": "prod", "cloud_name": "cloud-1", "description": "Prod folder"},
            ],
            "subnets": [],
        }

        result = sync_infrastructure(yc_data, netbox, cleanup_orphaned=False)

        assert result["folders"] == {"f1": 20, "f2": 21}
        assert netbox.ensure_cluster.call_count == 2
        netbox.ensure_cluster.assert_any_call(
            folder_name="dev", folder_id="f1", cloud_name="cloud-1", description="Dev folder"
        )

    def test_syncs_prefixes_with_site(self):
        """Prefixes are created with site assignment from zone mapping."""
        netbox = make_mock_netbox_client()
        netbox.ensure_site.return_value = 10

        yc_data = {
            "zones": [{"id": "ru-central1-a", "name": "ru-central1-a"}],
            "folders": [],
            "subnets": [
                {
                    "cidr": "10.0.0.0/24",
                    "zone_id": "ru-central1-a",
                    "vpc_name": "default",
                    "description": "main subnet",
                }
            ],
        }

        sync_infrastructure(yc_data, netbox, cleanup_orphaned=False)

        netbox.ensure_prefix.assert_called_once_with(
            prefix="10.0.0.0/24",
            vpc_name="default",
            site_id=10,
            description="main subnet",
        )

    def test_prefix_without_zone_created_without_site(self):
        """Prefixes without matching zone get site_id=None."""
        netbox = make_mock_netbox_client()

        yc_data = {
            "zones": [],
            "folders": [],
            "subnets": [
                {
                    "cidr": "172.16.0.0/24",
                    "zone_id": "unknown-zone",
                    "vpc_name": "test",
                    "description": "",
                }
            ],
        }

        sync_infrastructure(yc_data, netbox, cleanup_orphaned=False)

        netbox.ensure_prefix.assert_called_once_with(
            prefix="172.16.0.0/24",
            vpc_name="test",
            site_id=None,
            description="",
        )

    def test_uses_default_zones_when_none_provided(self):
        """When no zones in yc_data, uses default ru-central1 zones."""
        netbox = make_mock_netbox_client()
        netbox.ensure_site.return_value = 10

        yc_data = {"zones": [], "folders": [], "subnets": []}

        result = sync_infrastructure(yc_data, netbox, cleanup_orphaned=False)

        assert netbox.ensure_site.call_count == 4
        expected_zones = ["ru-central1-a", "ru-central1-b", "ru-central1-c", "ru-central1-d"]
        for zone_id in expected_zones:
            assert zone_id in result["zones"]

    def test_zone_with_empty_id_skipped(self):
        """Zones with empty id are skipped."""
        netbox = make_mock_netbox_client()

        yc_data = {
            "zones": [{"id": "", "name": "empty"}],
            "folders": [],
            "subnets": [],
        }

        result = sync_infrastructure(yc_data, netbox, cleanup_orphaned=False)

        # empty id zone is skipped, but default zones are NOT used because zones list is non-empty
        assert result["zones"] == {}
        netbox.ensure_site.assert_not_called()

    def test_folder_with_empty_id_skipped(self):
        """Folders with empty id are skipped."""
        netbox = make_mock_netbox_client()

        yc_data = {
            "zones": [{"id": "ru-central1-a", "name": "ru-central1-a"}],
            "folders": [{"id": "", "name": "empty-folder"}],
            "subnets": [],
        }

        result = sync_infrastructure(yc_data, netbox, cleanup_orphaned=False)

        assert result["folders"] == {}
        netbox.ensure_cluster.assert_not_called()

    def test_site_creation_failure_continues(self):
        """If one site fails, other zones still get processed."""
        netbox = make_mock_netbox_client()
        netbox.ensure_site.side_effect = [Exception("API error"), 11]

        yc_data = {
            "zones": [
                {"id": "zone-a", "name": "zone-a"},
                {"id": "zone-b", "name": "zone-b"},
            ],
            "folders": [],
            "subnets": [],
        }

        result = sync_infrastructure(yc_data, netbox, cleanup_orphaned=False)

        assert "zone-a" not in result["zones"]
        assert result["zones"]["zone-b"] == 11

    def test_cluster_creation_failure_continues(self):
        """If one cluster fails, other folders still get processed."""
        netbox = make_mock_netbox_client()
        netbox.ensure_cluster.side_effect = [Exception("API error"), 21]

        yc_data = {
            "zones": [],
            "folders": [
                {"id": "f1", "name": "fail-folder", "cloud_name": "c1", "description": ""},
                {"id": "f2", "name": "ok-folder", "cloud_name": "c1", "description": ""},
            ],
            "subnets": [],
        }

        result = sync_infrastructure(yc_data, netbox, cleanup_orphaned=False)

        assert "f1" not in result["folders"]
        assert result["folders"]["f2"] == 21

    def test_calls_cleanup_when_enabled(self):
        """When cleanup_orphaned=True, cleanup_orphaned_infrastructure is called."""
        netbox = make_mock_netbox_client()
        netbox.nb.dcim.sites.all.return_value = []
        netbox.nb.virtualization.clusters.all.return_value = []
        netbox.nb.ipam.prefixes.all.return_value = []

        yc_data = {"zones": [], "folders": [], "subnets": []}

        with patch("netbox_sync.sync.infrastructure.cleanup_orphaned_infrastructure") as mock_cleanup:
            mock_cleanup.return_value = {"sites": 0, "clusters": 0, "prefixes": 0}
            sync_infrastructure(yc_data, netbox, cleanup_orphaned=True)
            mock_cleanup.assert_called_once_with(yc_data, netbox, netbox.dry_run)

    def test_skips_cleanup_when_disabled(self):
        """When cleanup_orphaned=False, no cleanup runs."""
        netbox = make_mock_netbox_client()

        yc_data = {"zones": [], "folders": [], "subnets": []}

        with patch("netbox_sync.sync.infrastructure.cleanup_orphaned_infrastructure") as mock_cleanup:
            sync_infrastructure(yc_data, netbox, cleanup_orphaned=False)
            mock_cleanup.assert_not_called()

    def test_subnet_with_non_string_cidr_skipped(self):
        """Subnets where cidr is not a string are skipped."""
        netbox = make_mock_netbox_client()

        yc_data = {
            "zones": [],
            "folders": [],
            "subnets": [{"cidr": 12345, "zone_id": "a"}],
        }

        sync_infrastructure(yc_data, netbox, cleanup_orphaned=False)

        netbox.ensure_prefix.assert_not_called()

    def test_ensures_cluster_type_called(self):
        """ensure_cluster_type is always called."""
        netbox = make_mock_netbox_client()

        yc_data = {"zones": [], "folders": [], "subnets": []}

        sync_infrastructure(yc_data, netbox, cleanup_orphaned=False)

        netbox.ensure_cluster_type.assert_called_once()

    def test_prefix_creation_failure_continues(self):
        """If one prefix creation fails, others still get processed."""
        netbox = make_mock_netbox_client()
        netbox.ensure_prefix.side_effect = [Exception("fail"), MockRecord(id=30, prefix="10.1.0.0/24")]

        yc_data = {
            "zones": [],
            "folders": [],
            "subnets": [
                {"cidr": "10.0.0.0/24", "zone_id": None, "vpc_name": "v1", "description": ""},
                {"cidr": "10.1.0.0/24", "zone_id": None, "vpc_name": "v2", "description": ""},
            ],
        }

        # Should not raise
        sync_infrastructure(yc_data, netbox, cleanup_orphaned=False)

        assert netbox.ensure_prefix.call_count == 2

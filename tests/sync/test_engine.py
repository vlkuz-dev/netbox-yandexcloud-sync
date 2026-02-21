"""Tests for netbox_sync.sync.engine module."""

from unittest.mock import patch

import pytest

from netbox_sync.config import Config
from netbox_sync.sync.engine import SyncEngine


@pytest.fixture
def config():
    return Config(
        yc_token="test-yc-token",
        netbox_url="https://netbox.test",
        netbox_token="test-nb-token",
        dry_run=False,
    )


@pytest.fixture
def yc_data():
    return {
        "zones": [{"id": "ru-central1-a", "name": "ru-central1-a"}],
        "clouds": [{"id": "cloud-1", "name": "my-cloud"}],
        "folders": [{"id": "folder-1", "name": "default", "cloud_name": "my-cloud"}],
        "vpcs": [],
        "subnets": [],
        "vms": [{"id": "vm-1", "name": "test-vm", "folder_id": "folder-1"}],
    }


@pytest.fixture
def id_mapping():
    return {
        "zones": {"ru-central1-a": 10},
        "folders": {"folder-1": 20},
    }


@pytest.fixture
def batch_stats():
    return {
        "created": 1,
        "updated": 0,
        "skipped": 0,
        "deleted": 0,
        "errors": 0,
    }


class TestSyncEngineInit:
    @patch("netbox_sync.sync.engine.NetBoxClient")
    @patch("netbox_sync.sync.engine.YandexCloudClient")
    def test_creates_clients(self, mock_yc_cls, mock_nb_cls, config):
        engine = SyncEngine(config)

        mock_yc_cls.assert_called_once_with("test-yc-token")
        mock_nb_cls.assert_called_once_with(
            url="https://netbox.test",
            token="test-nb-token",
            dry_run=False,
        )
        assert engine.config is config

    @patch("netbox_sync.sync.engine.NetBoxClient")
    @patch("netbox_sync.sync.engine.YandexCloudClient")
    def test_passes_dry_run(self, mock_yc_cls, mock_nb_cls):
        cfg = Config(
            yc_token="t", netbox_url="u", netbox_token="n", dry_run=True
        )
        SyncEngine(cfg)
        mock_nb_cls.assert_called_once_with(url="u", token="n", dry_run=True)


class TestSyncEngineRunBatch:
    @patch("netbox_sync.sync.engine.sync_vms_optimized")
    @patch("netbox_sync.sync.engine.sync_infrastructure")
    @patch("netbox_sync.sync.engine.NetBoxClient")
    @patch("netbox_sync.sync.engine.YandexCloudClient")
    def test_batch_sync_flow(
        self, mock_yc_cls, mock_nb_cls, mock_infra, mock_batch,
        config, yc_data, id_mapping, batch_stats,
    ):
        mock_yc = mock_yc_cls.return_value
        mock_nb = mock_nb_cls.return_value
        mock_yc.fetch_all_data.return_value = yc_data
        mock_infra.return_value = id_mapping
        mock_batch.return_value = batch_stats

        engine = SyncEngine(config)
        result = engine.run(use_batch=True, cleanup=True)

        mock_yc.fetch_all_data.assert_called_once()
        mock_nb.ensure_sync_tag.assert_called_once()
        mock_infra.assert_called_once_with(yc_data, mock_nb, cleanup_orphaned=True)
        mock_batch.assert_called_once_with(
            yc_data, mock_nb, id_mapping, cleanup_orphaned=True
        )
        assert result == batch_stats

    @patch("netbox_sync.sync.engine.sync_vms_optimized")
    @patch("netbox_sync.sync.engine.sync_infrastructure")
    @patch("netbox_sync.sync.engine.NetBoxClient")
    @patch("netbox_sync.sync.engine.YandexCloudClient")
    def test_batch_no_cleanup(
        self, mock_yc_cls, mock_nb_cls, mock_infra, mock_batch,
        config, yc_data, id_mapping, batch_stats,
    ):
        mock_yc_cls.return_value.fetch_all_data.return_value = yc_data
        mock_infra.return_value = id_mapping
        mock_batch.return_value = batch_stats

        engine = SyncEngine(config)
        engine.run(use_batch=True, cleanup=False)

        mock_infra.assert_called_once_with(
            yc_data, mock_nb_cls.return_value, cleanup_orphaned=False
        )
        mock_batch.assert_called_once_with(
            yc_data, mock_nb_cls.return_value, id_mapping, cleanup_orphaned=False
        )


class TestSyncEngineRunStandard:
    @patch("netbox_sync.sync.engine.sync_vms")
    @patch("netbox_sync.sync.engine.sync_infrastructure")
    @patch("netbox_sync.sync.engine.NetBoxClient")
    @patch("netbox_sync.sync.engine.YandexCloudClient")
    def test_standard_sync_flow(
        self, mock_yc_cls, mock_nb_cls, mock_infra, mock_vms,
        config, yc_data, id_mapping,
    ):
        mock_yc = mock_yc_cls.return_value
        mock_nb = mock_nb_cls.return_value
        mock_yc.fetch_all_data.return_value = yc_data
        mock_infra.return_value = id_mapping
        mock_vms.return_value = {"created": 1, "updated": 0, "skipped": 0, "errors": 0}

        engine = SyncEngine(config)
        result = engine.run(use_batch=False, cleanup=True)

        mock_vms.assert_called_once_with(
            yc_data, mock_nb, id_mapping, cleanup_orphaned=True
        )
        assert result == {"created": 1, "updated": 0, "skipped": 0, "errors": 0}

    @patch("netbox_sync.sync.engine.sync_vms_optimized")
    @patch("netbox_sync.sync.engine.sync_vms")
    @patch("netbox_sync.sync.engine.sync_infrastructure")
    @patch("netbox_sync.sync.engine.NetBoxClient")
    @patch("netbox_sync.sync.engine.YandexCloudClient")
    def test_standard_does_not_call_batch(
        self, mock_yc_cls, mock_nb_cls, mock_infra, mock_vms, mock_batch,
        config, yc_data, id_mapping,
    ):
        mock_yc_cls.return_value.fetch_all_data.return_value = yc_data
        mock_infra.return_value = id_mapping

        engine = SyncEngine(config)
        engine.run(use_batch=False)

        mock_batch.assert_not_called()


class TestSyncEngineFetchErrors:
    @patch("netbox_sync.sync.engine.sync_vms_optimized")
    @patch("netbox_sync.sync.engine.sync_infrastructure")
    @patch("netbox_sync.sync.engine.NetBoxClient")
    @patch("netbox_sync.sync.engine.YandexCloudClient")
    def test_skips_cleanup_on_fetch_errors(
        self, mock_yc_cls, mock_nb_cls, mock_infra, mock_batch,
        config, id_mapping, batch_stats,
    ):
        yc_data_with_errors = {
            "zones": [{"id": "ru-central1-a", "name": "ru-central1-a"}],
            "clouds": [{"id": "cloud-1", "name": "my-cloud"}],
            "folders": [{"id": "folder-1", "name": "default", "cloud_name": "my-cloud"}],
            "vpcs": [],
            "subnets": [],
            "vms": [],
            "_has_fetch_errors": True,
        }
        mock_yc_cls.return_value.fetch_all_data.return_value = yc_data_with_errors
        mock_infra.return_value = id_mapping
        mock_batch.return_value = batch_stats

        engine = SyncEngine(config)
        engine.run(use_batch=True, cleanup=True)

        mock_infra.assert_called_once_with(
            yc_data_with_errors, mock_nb_cls.return_value, cleanup_orphaned=False
        )
        mock_batch.assert_called_once_with(
            yc_data_with_errors, mock_nb_cls.return_value, id_mapping, cleanup_orphaned=False
        )

    @patch("netbox_sync.sync.engine.sync_vms_optimized")
    @patch("netbox_sync.sync.engine.sync_infrastructure")
    @patch("netbox_sync.sync.engine.NetBoxClient")
    @patch("netbox_sync.sync.engine.YandexCloudClient")
    def test_allows_cleanup_without_fetch_errors(
        self, mock_yc_cls, mock_nb_cls, mock_infra, mock_batch,
        config, yc_data, id_mapping, batch_stats,
    ):
        mock_yc_cls.return_value.fetch_all_data.return_value = yc_data
        mock_infra.return_value = id_mapping
        mock_batch.return_value = batch_stats

        engine = SyncEngine(config)
        engine.run(use_batch=True, cleanup=True)

        mock_infra.assert_called_once_with(
            yc_data, mock_nb_cls.return_value, cleanup_orphaned=True
        )


class TestSyncEngineDefaults:
    @patch("netbox_sync.sync.engine.sync_vms_optimized")
    @patch("netbox_sync.sync.engine.sync_infrastructure")
    @patch("netbox_sync.sync.engine.NetBoxClient")
    @patch("netbox_sync.sync.engine.YandexCloudClient")
    def test_defaults_to_batch_with_cleanup(
        self, mock_yc_cls, mock_nb_cls, mock_infra, mock_batch,
        config, yc_data, id_mapping, batch_stats,
    ):
        mock_yc_cls.return_value.fetch_all_data.return_value = yc_data
        mock_infra.return_value = id_mapping
        mock_batch.return_value = batch_stats

        engine = SyncEngine(config)
        engine.run()

        mock_infra.assert_called_once_with(
            yc_data, mock_nb_cls.return_value, cleanup_orphaned=True
        )
        mock_batch.assert_called_once_with(
            yc_data, mock_nb_cls.return_value, id_mapping, cleanup_orphaned=True
        )

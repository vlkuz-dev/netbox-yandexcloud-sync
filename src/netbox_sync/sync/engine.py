"""Top-level sync engine that orchestrates the full sync cycle."""

import logging
from typing import Any, Dict

from netbox_sync.config import Config
from netbox_sync.clients.yandex import YandexCloudClient
from netbox_sync.clients.netbox import NetBoxClient
from netbox_sync.sync.infrastructure import sync_infrastructure
from netbox_sync.sync.vms import sync_vms
from netbox_sync.sync.batch import sync_vms_optimized

logger = logging.getLogger(__name__)


class SyncEngine:
    """Orchestrates the full Yandex Cloud -> NetBox sync cycle."""

    def __init__(self, config: Config):
        self.config = config
        self.yc = YandexCloudClient(config.yc_token)
        self.nb = NetBoxClient(
            url=config.netbox_url,
            token=config.netbox_token,
            dry_run=config.dry_run,
        )

    def run(self, use_batch: bool = True, cleanup: bool = True) -> Dict[str, Any]:
        """Execute full sync cycle.

        Args:
            use_batch: Use optimized batch sync (True) or standard sequential (False).
            cleanup: Whether to clean up orphaned objects.

        Returns:
            Summary statistics from the sync run.
        """
        logger.info("Starting Yandex Cloud to NetBox sync...")
        logger.info("Dry run mode: %s", self.config.dry_run)

        # Fetch data from Yandex Cloud
        logger.info("Fetching data from Yandex Cloud...")
        yc_data = self.yc.fetch_all_data()
        if not yc_data or not isinstance(yc_data, dict):
            raise RuntimeError("Failed to fetch data from Yandex Cloud (empty or invalid response)")

        # Skip cleanup if there were fetch errors to avoid deleting valid objects
        # that appear orphaned only because their data failed to load
        do_cleanup = cleanup
        if cleanup and yc_data.get("_has_fetch_errors"):
            logger.warning(
                "Skipping orphan cleanup because some YC API calls failed "
                "(incomplete data could cause incorrect deletions)"
            )
            do_cleanup = False

        # Ensure sync tag exists early
        logger.info("Initializing NetBox sync tag...")
        self.nb.ensure_sync_tag()

        # Sync infrastructure and get ID mappings
        id_mapping = sync_infrastructure(
            yc_data, self.nb, cleanup_orphaned=do_cleanup
        )

        # Sync VMs
        if use_batch:
            logger.info("Using optimized sync with batch operations...")
            stats = sync_vms_optimized(
                yc_data, self.nb, id_mapping, cleanup_orphaned=do_cleanup
            )
        else:
            logger.info("Using standard sync (sequential operations)...")
            stats = sync_vms(yc_data, self.nb, id_mapping, cleanup_orphaned=do_cleanup)

        logger.info("Synchronization completed successfully!")
        return stats

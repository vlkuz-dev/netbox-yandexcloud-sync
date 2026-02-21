"""Sync infrastructure components (zones as sites, folders as clusters, prefixes)."""

import logging
from typing import Any, Dict

from netbox_sync.clients.netbox import NetBoxClient
from netbox_sync.sync.cleanup import cleanup_orphaned_infrastructure

logger = logging.getLogger(__name__)


def sync_infrastructure(
    yc_data: Dict[str, Any],
    netbox: NetBoxClient,
    cleanup_orphaned: bool = True
) -> Dict[str, Dict[str, int]]:
    """
    Sync infrastructure components (zones as sites, folders as clusters, prefixes).

    Args:
        yc_data: Data from Yandex Cloud
        netbox: NetBox client
        cleanup_orphaned: Whether to clean up orphaned infrastructure objects

    Returns:
        Dictionary with mapping of IDs:
        - zones: {zone_id: netbox_site_id}
        - folders: {folder_id: netbox_cluster_id}
    """
    logger.info("Syncing infrastructure components...")

    # Ensure sync tag exists before creating any objects
    logger.info("Ensuring sync tag exists...")
    tag_id = netbox.ensure_sync_tag()
    if tag_id:
        logger.info(f"Using sync tag ID: {tag_id}")

    # Clean up orphaned infrastructure if requested
    if cleanup_orphaned:
        logger.info("Checking for orphaned infrastructure objects...")
        cleanup_counts = cleanup_orphaned_infrastructure(yc_data, netbox, netbox.dry_run)
        total_cleaned = sum(cleanup_counts.values())
        if total_cleaned > 0:
            logger.info(f"Cleaned up {total_cleaned} orphaned infrastructure objects")

    if not tag_id:
        logger.warning("Could not create sync tag, objects will not be tagged")

    id_mapping = {
        "zones": {},
        "folders": {}
    }

    # Create sites for each availability zone
    zones = yc_data.get("zones", [])
    if not zones:
        # Use default zones if none fetched
        zones = [
            {"id": "ru-central1-a", "name": "ru-central1-a"},
            {"id": "ru-central1-b", "name": "ru-central1-b"},
            {"id": "ru-central1-c", "name": "ru-central1-c"},
            {"id": "ru-central1-d", "name": "ru-central1-d"},
        ]
        logger.info("Using default zones as none were fetched from API")

    for zone in zones:
        zone_id = zone.get("id", "")
        zone_name = zone.get("name", zone_id)

        if zone_id:
            try:
                site_id = netbox.ensure_site(zone_id, zone_name)
                id_mapping["zones"][zone_id] = site_id
                logger.info(f"Ensured site for zone: {zone_name} (ID: {site_id})")
            except Exception as e:
                logger.error(f"Failed to ensure site for zone {zone_name}: {e}")
                # Continue without this zone in the mapping
                continue

    # Ensure cluster type exists
    netbox.ensure_cluster_type()

    # Create clusters for each folder
    folders = yc_data.get("folders", [])
    for folder in folders:
        folder_id = folder.get("id", "")
        folder_name = folder.get("name", folder_id)
        cloud_name = folder.get("cloud_name", "")
        description = folder.get("description", "")

        if folder_id:
            try:
                # Optionally assign to a default site if needed
                cluster_id = netbox.ensure_cluster(
                    folder_name=folder_name,
                    folder_id=folder_id,
                    cloud_name=cloud_name,
                    description=description
                )
                id_mapping["folders"][folder_id] = cluster_id
                logger.info(f"Ensured cluster for folder: {folder_name} (ID: {cluster_id})")
            except Exception as e:
                logger.error(f"Failed to ensure cluster for folder {folder_name}: {e}")

    # Sync prefixes for each subnet
    subnets = yc_data.get("subnets", [])
    for subnet in subnets:
        cidr = subnet.get("cidr")
        zone_id = subnet.get("zone_id")

        if cidr and isinstance(cidr, str):
            vpc_name = subnet.get("vpc_name", "")
            description = subnet.get("description", "")

            # Get site ID for this zone
            site_id = None
            if zone_id and zone_id in id_mapping.get("zones", {}):
                site_id = id_mapping["zones"][zone_id]
                logger.debug(f"Found site {site_id} for zone {zone_id}")
            elif zone_id:
                logger.debug(f"No site mapping found for zone {zone_id}, prefix {cidr} will be created without site")
            else:
                logger.debug(f"No zone_id for subnet with prefix {cidr}, will create without site")

            # Always try to create/update the prefix, even without a site
            try:
                # Pass site_id only if it's valid (not None and not 0)
                result = netbox.ensure_prefix(
                    prefix=cidr,
                    vpc_name=vpc_name if isinstance(vpc_name, str) else "",
                    site_id=site_id if site_id and site_id > 0 else None,
                    description=description if isinstance(description, str) else ""
                )

                if result:
                    if site_id and site_id > 0:
                        logger.info(f"Synced prefix: {cidr} in zone {zone_id}")
                    else:
                        logger.info(f"Synced prefix: {cidr} (no zone assignment)")
                else:
                    logger.warning(f"Failed to sync prefix {cidr}: no result returned")
            except Exception as e:
                logger.error(f"Failed to sync prefix {cidr}: {e}")
                # Continue with other prefixes
                continue

    return id_mapping

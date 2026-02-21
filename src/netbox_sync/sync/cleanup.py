"""Cleanup of orphaned objects in NetBox that no longer exist in Yandex Cloud."""

import logging
from typing import Any, Dict, List

from netbox_sync.clients.netbox import NetBoxClient

logger = logging.getLogger(__name__)


def cleanup_orphaned_infrastructure(
    yc_data: Dict[str, Any],
    netbox: NetBoxClient,
    dry_run: bool = False
) -> Dict[str, int]:
    """
    Delete infrastructure objects that have synced-from-yc tag but don't exist in Yandex Cloud.

    Args:
        yc_data: Data from Yandex Cloud
        netbox: NetBox client
        dry_run: If True, only log what would be deleted

    Returns:
        Dictionary with counts of deleted objects by type
    """
    deleted_counts = {
        "sites": 0,
        "clusters": 0,
        "prefixes": 0
    }

    try:
        tag_id = netbox.ensure_sync_tag()

        # Check sites (zones)
        yc_zones = {zone["id"] for zone in yc_data.get("zones", [])}
        all_sites = list(netbox.nb.dcim.sites.all())

        for site in all_sites:
            site_tags = [t.id if hasattr(t, 'id') else t for t in (site.tags or [])]
            # Extract zone_id from site slug (ensure_site sets slug = zone_id.lower())
            # or from description which has format "Yandex Cloud Availability Zone: {zone_id}"
            zone_id = None
            if hasattr(site, 'slug') and site.slug:
                zone_id = site.slug
            elif site.description and "Availability Zone:" in site.description:
                zone_id = site.description.split("Availability Zone:")[-1].strip()

            if tag_id in site_tags and zone_id and zone_id not in yc_zones:
                if dry_run:
                    logger.info(f"[DRY-RUN] Would delete orphaned site: {site.name} (zone: {zone_id})")
                    deleted_counts["sites"] += 1
                else:
                    try:
                        site.delete()
                        logger.info(f"Deleted orphaned site: {site.name} (zone: {zone_id})")
                        deleted_counts["sites"] += 1
                    except Exception as e:
                        logger.error(f"Failed to delete orphaned site {site.name}: {e}")

        # Check clusters (folders)
        yc_folders = {folder["id"] for folder in yc_data.get("folders", [])}
        logger.debug(f"YC folders found: {yc_folders}")

        all_clusters = list(netbox.nb.virtualization.clusters.all())
        logger.debug(f"Checking {len(all_clusters)} clusters in NetBox")

        for cluster in all_clusters:
            cluster_tags = [t.id if hasattr(t, 'id') else t for t in (cluster.tags or [])]
            has_sync_tag = tag_id in cluster_tags

            logger.debug(f"Cluster {cluster.name}: tags={cluster_tags}, has_sync_tag={has_sync_tag}")

            # Extract folder_id from cluster comments
            folder_id = None
            if cluster.comments:
                logger.debug(f"Cluster {cluster.name} comments: {cluster.comments[:200]}")
                if "Folder ID:" in cluster.comments:
                    folder_id = cluster.comments.split("Folder ID:")[1].split("\n")[0].strip()
                    logger.debug(f"Extracted folder_id: {folder_id}")
                else:
                    logger.debug("No 'Folder ID:' found in comments")
            else:
                logger.debug(f"Cluster {cluster.name} has no comments")

            if has_sync_tag:
                if not folder_id:
                    logger.warning(f"Cluster {cluster.name} has sync tag but no folder_id in comments")
                elif folder_id not in yc_folders:
                    logger.info(f"Cluster {cluster.name} is orphaned: folder_id {folder_id} not in YC")
                else:
                    logger.debug(f"Cluster {cluster.name} is valid: folder_id {folder_id} exists in YC")

            if tag_id in cluster_tags and folder_id and folder_id not in yc_folders:
                if dry_run:
                    logger.info(f"[DRY-RUN] Would delete orphaned cluster: {cluster.name} (folder: {folder_id})")
                    deleted_counts["clusters"] += 1
                else:
                    try:
                        cluster.delete()
                        logger.info(f"Deleted orphaned cluster: {cluster.name} (folder: {folder_id})")
                        deleted_counts["clusters"] += 1
                    except Exception as e:
                        logger.error(f"Failed to delete orphaned cluster {cluster.name}: {e}")

        # Check prefixes
        yc_subnet_cidrs = {subnet.get("cidr") for subnet in yc_data.get("subnets", []) if subnet.get("cidr")}
        all_prefixes = list(netbox.nb.ipam.prefixes.all())

        for prefix in all_prefixes:
            prefix_tags = [t.id if hasattr(t, 'id') else t for t in (prefix.tags or [])]
            # Check if this tagged prefix's CIDR still exists in YC subnets
            # Since ensure_prefix doesn't store subnet_id, match by CIDR
            prefix_cidr = str(prefix.prefix) if hasattr(prefix, 'prefix') else None

            if tag_id in prefix_tags and prefix_cidr and prefix_cidr not in yc_subnet_cidrs:
                if dry_run:
                    logger.info(f"[DRY-RUN] Would delete orphaned prefix: {prefix.prefix}")
                    deleted_counts["prefixes"] += 1
                else:
                    try:
                        prefix.delete()
                        logger.info(f"Deleted orphaned prefix: {prefix.prefix}")
                        deleted_counts["prefixes"] += 1
                    except Exception as e:
                        logger.error(f"Failed to delete orphaned prefix {prefix.prefix}: {e}")

        total_deleted = sum(deleted_counts.values())
        if total_deleted > 0:
            logger.info(f"Cleaned up {total_deleted} orphaned infrastructure objects: {deleted_counts}")
        elif not dry_run:
            logger.debug("No orphaned infrastructure objects to clean up")

    except Exception as e:
        logger.error(f"Error during infrastructure cleanup: {e}")

    return deleted_counts


def cleanup_orphaned_vms(
    yc_vms: List[Dict[str, Any]],
    netbox: NetBoxClient,
    dry_run: bool = False
) -> int:
    """
    Delete VMs that have synced-from-yc tag but don't exist in Yandex Cloud.

    Args:
        yc_vms: List of VMs from Yandex Cloud
        netbox: NetBox client
        dry_run: If True, only log what would be deleted

    Returns:
        Number of VMs deleted
    """
    deleted_count = 0

    try:
        # Get YC VM names for comparison
        yc_vm_names = {vm.get("name") for vm in yc_vms if vm.get("name")}
        logger.debug(f"Found {len(yc_vm_names)} VMs in Yandex Cloud for cleanup check")

        # Get all VMs from NetBox with synced-from-yc tag
        all_netbox_vms = netbox.fetch_vms()
        tag_id = netbox.ensure_sync_tag()

        for vm in all_netbox_vms:
            # Check if VM has synced-from-yc tag
            vm_tags = []
            if hasattr(vm, 'tags') and vm.tags:
                vm_tags = [t.id if hasattr(t, 'id') else t for t in vm.tags]

            # If VM has our tag but doesn't exist in YC, delete it
            if tag_id in vm_tags and vm.name not in yc_vm_names:
                if dry_run:
                    logger.info(f"[DRY-RUN] Would delete orphaned VM: {vm.name} (ID: {vm.id})")
                    deleted_count += 1
                else:
                    try:
                        vm.delete()
                        logger.info(f"Deleted orphaned VM: {vm.name} (ID: {vm.id})")
                        deleted_count += 1
                    except Exception as e:
                        logger.error(f"Failed to delete orphaned VM {vm.name}: {e}")

        if deleted_count > 0:
            logger.info(f"Cleaned up {deleted_count} orphaned VMs")
        elif not dry_run:
            logger.debug("No orphaned VMs to clean up")

    except Exception as e:
        logger.error(f"Error during VM cleanup: {e}")

    return deleted_count

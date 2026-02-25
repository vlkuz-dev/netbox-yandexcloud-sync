"""VM synchronization — create, update, sync disks, interfaces, and primary IPs."""

import logging
from typing import Any, Dict

from netbox_sync.clients.netbox import NetBoxClient
from netbox_sync.ip import is_private_ip, get_ip_without_cidr, ensure_cidr_notation
from netbox_sync.sync.cleanup import cleanup_orphaned_vms

logger = logging.getLogger(__name__)

# Default platform slug for unrecognized operating systems
DEFAULT_PLATFORM_SLUG = "linux"


def parse_memory_mb(resources: Dict[str, Any], vm_name: str = "unknown") -> int:
    """Parse memory from YC resources dict, handling string/int/float types.

    Returns MB value suitable for NetBox (which displays GB = MB / 1000).
    YC returns bytes in binary units (GiB), so we convert: bytes → GiB → * 1000 → MB.
    """
    memory = resources.get("memory", 0)
    memory_mb = 0
    if memory:
        try:
            if isinstance(memory, str):
                memory_clean = ''.join(filter(str.isdigit, memory))
                if memory_clean:
                    memory_int = int(memory_clean)
                    if memory_int < 1000:
                        # Value in GB — convert to NetBox MB
                        memory_mb = memory_int * 1000
                    elif memory_int < 1000000:
                        memory_mb = memory_int
                    else:
                        # Value in bytes — convert to GiB then to NetBox MB
                        memory_mb = round(memory_int / (1024 ** 3) * 1000)
                else:
                    logger.warning(f"VM {vm_name}: could not parse memory string '{memory}'")
            elif isinstance(memory, (int, float)):
                memory_int = int(memory)
                if memory_int < 1000:
                    # Value in GB — convert to NetBox MB
                    memory_mb = memory_int * 1000
                elif memory_int < 1000000:
                    memory_mb = memory_int
                else:
                    # Value in bytes — convert to GiB then to NetBox MB
                    memory_mb = round(memory_int / (1024 ** 3) * 1000)
            else:
                logger.warning(f"VM {vm_name}: unexpected memory type {type(memory).__name__}: {memory}")
        except (ValueError, TypeError) as e:
            logger.error(f"VM {vm_name}: failed to parse memory value {memory}: {e}")
            memory_mb = 0
    return memory_mb


def parse_cores(resources: Dict[str, Any], vm_name: str = "unknown") -> int:
    """Parse cores from YC resources dict, handling string/int/float types."""
    cores = resources.get("cores", 1)
    vcpus = 1
    if cores:
        try:
            if isinstance(cores, str):
                cores_clean = ''.join(filter(str.isdigit, cores))
                if cores_clean:
                    vcpus = int(cores_clean)
                else:
                    logger.warning(f"VM {vm_name}: could not parse cores string '{cores}'")
            elif isinstance(cores, (int, float)):
                vcpus = int(cores)
            else:
                logger.warning(f"VM {vm_name}: unexpected cores type {type(cores).__name__}: {cores}")
        except (ValueError, TypeError) as e:
            logger.error(f"VM {vm_name}: failed to parse cores value {cores}: {e}")
            vcpus = 1
    return vcpus


def detect_platform_slug(os_name: str) -> str:
    """Detect platform slug from OS name string. Returns a slug suitable for NetBox platform lookup."""
    if not os_name:
        return DEFAULT_PLATFORM_SLUG

    os_name_lower = os_name.lower()

    if "windows" in os_name_lower:
        if "2019" in os_name_lower:
            return "windows-2019"
        elif "2022" in os_name_lower:
            return "windows-2022"
        elif "2025" in os_name_lower:
            return "windows-2025"
        else:
            return "windows"
    elif "ubuntu" in os_name_lower:
        if "22.04" in os_name_lower or "22-04" in os_name_lower or "jammy" in os_name_lower:
            return "ubuntu-22-04"
        elif "24.04" in os_name_lower or "24-04" in os_name_lower or "noble" in os_name_lower:
            return "ubuntu-24-04"
        else:
            return "ubuntu-22-04"
    elif "debian" in os_name_lower:
        if "11" in os_name_lower or "bullseye" in os_name_lower:
            return "debian-11"
        else:
            return "debian-11"
    elif "centos" in os_name_lower:
        if "7" in os_name_lower:
            return "centos-7"
        else:
            return DEFAULT_PLATFORM_SLUG
    elif "alma" in os_name_lower or "almalinux" in os_name_lower:
        if "9" in os_name_lower:
            return "almalinux-9"
        else:
            return DEFAULT_PLATFORM_SLUG
    elif "oracle" in os_name_lower:
        if "9" in os_name_lower:
            return "oracle-linux-9"
        else:
            return DEFAULT_PLATFORM_SLUG
    elif any(k in os_name_lower for k in ("rocky", "rhel", "red hat", "fedora")):
        return DEFAULT_PLATFORM_SLUG
    elif "linux" in os_name_lower:
        return DEFAULT_PLATFORM_SLUG

    return DEFAULT_PLATFORM_SLUG


def detect_platform_id(os_name: str, netbox: NetBoxClient = None) -> int:
    """Detect NetBox platform ID from OS name string.

    If a NetBox client is provided, resolves the platform by slug at runtime.
    Otherwise returns 0 (caller must handle).
    """
    slug = detect_platform_slug(os_name)
    if netbox:
        return netbox.ensure_platform(slug)
    return 0


def prepare_vm_data(
    yc_vm: Dict[str, Any],
    netbox: NetBoxClient,
    id_mapping: Dict[str, Dict[str, int]]
) -> Dict[str, Any]:
    """Prepare VM data for NetBox creation."""
    # Get cluster ID from folder mapping
    folder_id = yc_vm.get("folder_id", "")
    cluster_id = id_mapping["folders"].get(folder_id) if folder_id else None

    if not cluster_id:
        # Fallback: create cluster on the fly if needed
        folder_name = yc_vm.get("folder_name", "default")
        cloud_name = yc_vm.get("cloud_name", "")
        if isinstance(folder_name, str) and isinstance(cloud_name, str):
            cluster_id = netbox.ensure_cluster(
                folder_name=folder_name,
                folder_id=folder_id,
                cloud_name=cloud_name
            )

    # Calculate resources using shared helpers
    resources = yc_vm.get("resources", {})
    if not isinstance(resources, dict):
        resources = {}

    vm_name_for_log = yc_vm.get('name', 'unknown')
    memory_mb = parse_memory_mb(resources, vm_name_for_log)

    if memory_mb == 0 and resources:
        logger.warning(f"VM {vm_name_for_log}: memory calculated as 0 MB, resources: {resources}")

    vcpus = parse_cores(resources, vm_name_for_log)

    # Determine status
    status_value = yc_vm.get("status")
    if status_value == "RUNNING":
        status = "active"
    else:
        status = "offline"

    # Get VM name
    vm_name = yc_vm.get("name", "unknown")
    if not isinstance(vm_name, str):
        vm_name = "unknown"

    # Get VM ID and other metadata for comments
    vm_id = yc_vm.get("id", "unknown")
    if not isinstance(vm_id, str):
        vm_id = "unknown"

    platform_id = yc_vm.get("platform_id", "")  # Hardware platform (e.g., standard-v3)
    os_name = yc_vm.get("os", "")  # Operating system from image
    created_at = yc_vm.get("created_at", "")
    zone_id = yc_vm.get("zone_id", "")

    # Build comments with metadata
    comments_parts = [
        f"YC VM ID: {vm_id}",
        f"Zone: {zone_id}" if zone_id else None,
        f"Hardware Platform: {platform_id}" if platform_id else None,
        f"OS: {os_name}" if os_name else None,
        f"Created: {created_at}" if created_at else None,
    ]
    comments = "\n".join(filter(None, comments_parts))

    vm_data = {
        "name": vm_name,
        "vcpus": vcpus,
        "memory": memory_mb,
        "status": status,
        "comments": comments
    }

    # Add cluster if available
    if cluster_id:
        vm_data["cluster"] = cluster_id

    # Add site assignment based on zone if available
    if zone_id and zone_id in id_mapping.get("zones", {}):
        site_id = id_mapping["zones"][zone_id]
        if site_id and site_id > 0:
            vm_data["site"] = site_id

    # Map OS to NetBox platform (operating system)
    vm_data["platform"] = detect_platform_id(os_name, netbox)

    return vm_data


def update_vm_parameters(
    vm: Any,
    yc_vm: Dict[str, Any],
    netbox: NetBoxClient,
    id_mapping: Dict[str, Any]
) -> bool:
    """
    Update VM parameters (memory, CPU, site, cluster) for an existing VM.

    Args:
        vm: Existing NetBox VM object
        yc_vm: Yandex Cloud VM data
        netbox: NetBox client
        id_mapping: ID mapping for cluster and sites

    Returns:
        True if updated, False otherwise
    """
    try:
        vm_data = prepare_vm_data(yc_vm, netbox, id_mapping)
        updates = {}

        # Check memory
        if hasattr(vm, 'memory'):
            current_memory = vm.memory if vm.memory is not None else 0
            new_memory = vm_data['memory']
            if current_memory != new_memory:
                updates['memory'] = new_memory
                logger.info(f"VM {vm.name}: memory will be updated from {current_memory} to {new_memory} MB")

        # Check vCPUs
        if hasattr(vm, 'vcpus') and vm.vcpus != vm_data['vcpus']:
            updates['vcpus'] = vm_data['vcpus']
            logger.info(f"VM {vm.name}: vCPUs will be updated from {vm.vcpus} to {vm_data['vcpus']}")

        # Check cluster
        if 'cluster' in vm_data:
            current_cluster_id = None
            if hasattr(vm, 'cluster') and vm.cluster:
                current_cluster_id = vm.cluster.id if hasattr(vm.cluster, 'id') else vm.cluster

            new_cluster_id = vm_data['cluster']
            if current_cluster_id != new_cluster_id:
                updates['cluster'] = new_cluster_id
                logger.info(f"VM {vm.name}: cluster will be updated from {current_cluster_id} to {new_cluster_id}")

        # Check site
        if 'site' in vm_data:
            current_site_id = None
            if hasattr(vm, 'site') and vm.site:
                current_site_id = vm.site.id if hasattr(vm.site, 'id') else vm.site

            new_site_id = vm_data['site']
            if current_site_id != new_site_id:
                updates['site'] = new_site_id
                logger.info(f"VM {vm.name}: site will be updated from {current_site_id} to {new_site_id}")

        # Check platform (operating system)
        if 'platform' in vm_data:
            current_platform_id = None
            if hasattr(vm, 'platform') and vm.platform:
                current_platform_id = vm.platform.id if hasattr(vm.platform, 'id') else vm.platform

            new_platform_id = vm_data['platform']
            if current_platform_id != new_platform_id:
                updates['platform'] = new_platform_id
                logger.info(f"VM {vm.name}: platform will be updated from {current_platform_id} to {new_platform_id}")

        # Check status
        if hasattr(vm, 'status') and vm.status:
            if hasattr(vm.status, 'value'):
                current_status = vm.status.value
            else:
                current_status = str(vm.status)
            if current_status != vm_data['status']:
                updates['status'] = vm_data['status']
                logger.info(f"VM {vm.name}: status will be updated from {current_status} to {vm_data['status']}")

        # Check comments
        if hasattr(vm, 'comments') and vm.comments != vm_data.get('comments', ''):
            updates['comments'] = vm_data.get('comments', '')

        # Update VM if there are changes
        if updates:
            if netbox.update_vm(vm.id, updates):
                logger.info(f"Updated VM {vm.name} parameters: {list(updates.keys())}")
                return True
            else:
                logger.error(f"Failed to update VM {vm.name}")
                return False
        else:
            logger.debug(f"VM {vm.name} is up to date")
            return False

    except Exception as e:
        logger.error(f"Failed to update parameters for VM {vm.name}: {e}")
        return False


def sync_vm_disks(
    vm: Any,
    yc_vm: Dict[str, Any],
    netbox: NetBoxClient,
    remove_orphaned: bool = True
) -> Dict[str, int]:
    """
    Sync virtual disks for an existing VM.

    Args:
        vm: Existing NetBox VM object
        yc_vm: Yandex Cloud VM data
        netbox: NetBox client
        remove_orphaned: Whether to remove disks that don't exist in YC

    Returns:
        Dictionary with counts of created, deleted, and unchanged disks
    """
    try:
        disks_created = 0
        disks_deleted = 0
        disks_unchanged = 0

        # Get existing disks for this VM
        existing_disks = []
        if hasattr(netbox.nb.virtualization, 'virtual_disks'):
            try:
                existing_disks = list(netbox.nb.virtualization.virtual_disks.filter(virtual_machine_id=vm.id))
                logger.debug(f"VM {vm.name}: found {len(existing_disks)} existing disks in NetBox")
            except Exception as e:
                logger.debug(f"Could not fetch existing disks for VM {vm.name}: {e}")
        else:
            logger.debug("Virtual disks not supported in this NetBox version")
            return {"created": 0, "deleted": 0, "unchanged": 0}

        # Create a map of existing disks by name for comparison
        existing_disks_by_name = {disk.name: disk for disk in existing_disks}

        # Get disks from Yandex Cloud
        yc_disks = yc_vm.get("disks", [])
        if not isinstance(yc_disks, list):
            yc_disks = []

        # Track which disks we've seen in YC
        yc_disk_names = set()

        # Process each disk from Yandex Cloud
        for idx, disk in enumerate(yc_disks):
            if not isinstance(disk, dict):
                continue

            disk_name = str(disk.get("name", f"disk-{idx}"))
            yc_disk_names.add(disk_name)

            # Check if disk already exists
            if disk_name in existing_disks_by_name:
                existing_disk = existing_disks_by_name[disk_name]

                # Check if size needs updating
                size = disk.get("size", 0)
                if isinstance(size, (int, float)) and size > 0:
                    size_mb = int(size) // (1024**2)
                    if existing_disk.size != size_mb:
                        if netbox.dry_run:
                            logger.info(
                                f"[DRY-RUN] VM {vm.name}: would update disk {disk_name} "
                                f"size from {existing_disk.size} MB to {size_mb} MB"
                            )
                        else:
                            logger.info(
                                f"VM {vm.name}: updating disk {disk_name} "
                                f"size from {existing_disk.size} MB to {size_mb} MB"
                            )
                            try:
                                existing_disk.size = size_mb
                                existing_disk.save()
                            except Exception as e:
                                logger.error(f"VM {vm.name}: failed to update disk {disk_name} size: {e}")
                    else:
                        disks_unchanged += 1
                        logger.debug(f"VM {vm.name}: disk {disk_name} is up to date")
                else:
                    disks_unchanged += 1
                continue

            # Get disk size
            size = disk.get("size", 0)
            if not isinstance(size, (int, float)) or size == 0:
                logger.warning(f"VM {vm.name}: invalid disk size for {disk_name}: {size}")
                continue

            disk_data = {
                "virtual_machine": vm.id,
                "size": int(size) // (1024**2),
                "name": disk_name
            }

            disk_type = disk.get("type", "")
            if disk_type:
                disk_data["description"] = f"Type: {disk_type}"

            if netbox.create_disk(disk_data):
                disks_created += 1
                logger.info(f"VM {vm.name}: created disk {disk_name} ({disk_data['size']} MB)")
            else:
                logger.error(f"VM {vm.name}: failed to create disk {disk_name}")

        # Remove orphaned disks (exist in NetBox but not in YC)
        if remove_orphaned:
            for disk_name, disk in existing_disks_by_name.items():
                if disk_name not in yc_disk_names:
                    if netbox.dry_run:
                        logger.info(f"[DRY-RUN] VM {vm.name}: would remove orphaned disk {disk_name}")
                        disks_deleted += 1
                    else:
                        try:
                            logger.info(f"VM {vm.name}: removing orphaned disk {disk_name}")
                            disk.delete()
                            disks_deleted += 1
                        except Exception as e:
                            logger.error(f"VM {vm.name}: failed to delete orphaned disk {disk_name}: {e}")

        if disks_created > 0 or disks_deleted > 0:
            logger.info(
                f"VM {vm.name}: disk sync complete - created: {disks_created}, "
                f"deleted: {disks_deleted}, unchanged: {disks_unchanged}"
            )

        return {
            "created": disks_created,
            "deleted": disks_deleted,
            "unchanged": disks_unchanged
        }

    except Exception as e:
        logger.error(f"Failed to sync disks for VM {vm.name}: {e}")
        return {"created": 0, "deleted": 0, "unchanged": 0}


def update_vm_primary_ip(
    vm: Any,
    yc_vm: Dict[str, Any],
    netbox: NetBoxClient
) -> bool:
    """
    Check and update primary IP for an existing VM if not set.
    Always prefers private IPs over public IPs for primary assignment.

    Args:
        vm: Existing NetBox VM object
        yc_vm: Yandex Cloud VM data
        netbox: NetBox client

    Returns:
        True if updated, False otherwise
    """
    try:
        network_interfaces = yc_vm.get("network_interfaces", [])
        if not isinstance(network_interfaces, list) or not network_interfaces:
            return False

        # Look for private IPs first, then public
        expected_private_ip = None
        expected_public_ip = None

        for iface in network_interfaces:
            if not isinstance(iface, dict):
                continue

            primary_v4 = iface.get("primary_v4_address")
            if primary_v4 and isinstance(primary_v4, str):
                if is_private_ip(primary_v4):
                    expected_private_ip = primary_v4
                    break
                elif not expected_public_ip:
                    expected_public_ip = primary_v4

            nat_v4 = iface.get("primary_v4_address_one_to_one_nat")
            if nat_v4 and isinstance(nat_v4, str) and not expected_public_ip:
                expected_public_ip = nat_v4

        # Prefer private IP over public
        expected_primary_ip = expected_private_ip if expected_private_ip else expected_public_ip
        if not expected_primary_ip:
            return False

        expected_base_ip = get_ip_without_cidr(expected_primary_ip)

        # Check if VM already has the correct primary IPv4 set
        if hasattr(vm, 'primary_ip4') and vm.primary_ip4:
            current_primary_base = get_ip_without_cidr(str(vm.primary_ip4.address))
            if current_primary_base == expected_base_ip:
                logger.debug(f"VM {vm.name} already has correct primary IPv4 set: {current_primary_base}")
                return False
            else:
                current_is_private = is_private_ip(current_primary_base)
                expected_is_private = is_private_ip(expected_base_ip)
                if not current_is_private and expected_is_private:
                    logger.info(
                        f"VM {vm.name}: Will switch primary from public IP "
                        f"{current_primary_base} to private IP {expected_base_ip}"
                    )
                else:
                    logger.info(f"VM {vm.name}: Will update primary from {current_primary_base} to {expected_base_ip}")

        primary_v4 = expected_primary_ip
        base_ip = expected_base_ip
        primary_v4 = ensure_cidr_notation(primary_v4)

        # Try to find this IP in NetBox
        try:
            existing_ips = list(netbox.nb.ipam.ip_addresses.filter(
                address__ic=base_ip
            ))

            existing_ip = None
            for ip in existing_ips:
                if ip.address.split('/')[0] == base_ip:
                    existing_ip = ip
                    break

            if existing_ip:
                if netbox.dry_run:
                    logger.info(f"[DRY-RUN] Would set primary IPv4 for VM {vm.name}: {base_ip}")
                    return True

                # First, check if this IP is set as primary on any other VM
                vms_with_primary = list(netbox.nb.virtualization.virtual_machines.filter(
                    primary_ip4_id=existing_ip.id
                ))

                for vm_with_primary in vms_with_primary:
                    if vm_with_primary.id != vm.id:
                        logger.info(f"Unsetting IP {base_ip} as primary on VM {vm_with_primary.name}")
                        vm_with_primary.primary_ip4 = None
                        vm_with_primary.save()

                # Check if IP is assigned to this VM's interface
                vm_interfaces = list(netbox.nb.virtualization.interfaces.filter(virtual_machine_id=vm.id))
                ip_assigned_to_vm = False

                if hasattr(existing_ip, 'assigned_object_id') and existing_ip.assigned_object_id:
                    for iface in vm_interfaces:
                        if existing_ip.assigned_object_id == iface.id:
                            ip_assigned_to_vm = True
                            break

                # If not assigned to this VM, assign it to the first interface
                if not ip_assigned_to_vm and vm_interfaces:
                    logger.info(f"Assigning IP {base_ip} to VM {vm.name}'s first interface")
                    existing_ip.assigned_object_type = "virtualization.vminterface"
                    existing_ip.assigned_object_id = vm_interfaces[0].id
                    existing_ip.save()

                # Now set as primary IP
                if netbox.set_vm_primary_ip(vm.id, existing_ip.id, ip_version=4):
                    logger.info(f"Updated primary IPv4 for VM {vm.name}: {base_ip} (as {existing_ip.address})")
                    return True
        except Exception as e:
            logger.debug(f"Could not find or set primary IP for {vm.name}: {e}")

        return False
    except Exception as e:
        logger.error(f"Failed to update primary IP for VM {vm.name}: {e}")
        return False


def sync_vm_interfaces(
    vm: Any,
    yc_vm: Dict[str, Any],
    netbox: NetBoxClient
) -> Dict[str, int]:
    """
    Synchronize network interfaces and IP addresses for an existing VM.

    Args:
        vm: Existing NetBox VM object
        yc_vm: Yandex Cloud VM data
        netbox: NetBox client

    Returns:
        Dictionary with sync statistics
    """
    result = {
        "interfaces_created": 0,
        "ips_created": 0,
        "errors": 0
    }

    try:
        yc_interfaces = yc_vm.get("network_interfaces", [])
        if not isinstance(yc_interfaces, list):
            return result

        try:
            existing_interfaces = list(netbox.nb.virtualization.interfaces.filter(virtual_machine_id=vm.id))
        except Exception as e:
            logger.error(f"Failed to get interfaces for VM {vm.name}: {e}")
            return result

        existing_interface_names = {iface.name: iface for iface in existing_interfaces}

        for idx, yc_iface in enumerate(yc_interfaces):
            if not isinstance(yc_iface, dict):
                continue

            interface_name = f"eth{idx}"

            if interface_name in existing_interface_names:
                nb_interface = existing_interface_names[interface_name]
                logger.debug(f"Interface {interface_name} already exists for VM {vm.name}")
            else:
                interface_data = {
                    "virtual_machine": vm.id,
                    "name": interface_name,
                    "type": "virtual",
                    "enabled": True
                }

                nb_interface = netbox.create_interface(interface_data)
                if nb_interface:
                    logger.info(f"Created interface {interface_name} for VM {vm.name}")
                    result["interfaces_created"] += 1
                else:
                    logger.error(f"Failed to create interface {interface_name} for VM {vm.name}")
                    result["errors"] += 1
                    continue

            # Process IP addresses for this interface
            primary_v4 = yc_iface.get("primary_v4_address")
            if primary_v4 and isinstance(primary_v4, str):
                base_ip = get_ip_without_cidr(primary_v4)
                primary_v4 = ensure_cidr_notation(primary_v4)

                try:
                    existing_ips = list(netbox.nb.ipam.ip_addresses.filter(
                        address__ic=base_ip
                    ))

                    existing_ip = None
                    for ip in existing_ips:
                        if ip.address.split('/')[0] == base_ip:
                            existing_ip = ip
                            break

                    if existing_ip:
                        if (hasattr(existing_ip, 'assigned_object_id')
                                and existing_ip.assigned_object_id == nb_interface.id):
                            logger.debug(
                                f"IP {base_ip} (as {existing_ip.address}) already exists "
                                f"for interface {interface_name} on VM {vm.name}"
                            )
                        else:
                            logger.debug(
                                f"IP {base_ip} (as {existing_ip.address}) exists but "
                                f"not assigned to {interface_name}, updating assignment"
                            )
                            if netbox.dry_run:
                                logger.info(
                                    f"[DRY-RUN] Would reassign IP {base_ip} to "
                                    f"interface {interface_name} on VM {vm.name}"
                                )
                                result["ips_created"] += 1
                            else:
                                try:
                                    if hasattr(existing_ip, 'assigned_object_id') and existing_ip.assigned_object_id:
                                        vms_with_primary = list(netbox.nb.virtualization.virtual_machines.filter(
                                            primary_ip4_id=existing_ip.id
                                        ))
                                        for vm_with_primary in vms_with_primary:
                                            logger.info(
                                                f"Unsetting IP {base_ip} as primary "
                                                f"on VM {vm_with_primary.name}"
                                            )
                                            vm_with_primary.primary_ip4 = None
                                            vm_with_primary.save()

                                    existing_ip.assigned_object_type = "virtualization.vminterface"
                                    existing_ip.assigned_object_id = nb_interface.id
                                    existing_ip.save()
                                    logger.info(
                                        f"Updated IP {base_ip} (as {existing_ip.address}) "
                                        f"assignment to interface {interface_name} "
                                        f"on VM {vm.name}"
                                    )
                                    result["ips_created"] += 1
                                except Exception as e:
                                    logger.error(f"Failed to update IP {base_ip} assignment: {e}")
                                    result["errors"] += 1
                    else:
                        ip_data = {
                            "address": primary_v4,
                            "assigned_object_type": "virtualization.vminterface",
                            "assigned_object_id": nb_interface.id,
                            "status": "active",
                            "description": "Private IP" if is_private_ip(primary_v4) else ""
                        }

                        created_ip = netbox.create_ip(ip_data)
                        if created_ip:
                            logger.info(f"Created IP {primary_v4} for interface {interface_name} on VM {vm.name}")
                            result["ips_created"] += 1
                        else:
                            logger.error(
                                f"Failed to create IP {primary_v4} for "
                                f"interface {interface_name} on VM {vm.name}"
                            )
                            result["errors"] += 1

                except Exception as e:
                    logger.error(f"Failed to process IP {primary_v4} for VM {vm.name}: {e}")
                    result["errors"] += 1

            # Process public IP (NAT) if exists
            public_v4 = yc_iface.get("primary_v4_address_one_to_one_nat")
            if public_v4 and isinstance(public_v4, str):
                base_public_ip = get_ip_without_cidr(public_v4)
                public_v4 = ensure_cidr_notation(public_v4)

                try:
                    existing_public_ips = list(netbox.nb.ipam.ip_addresses.filter(
                        address__ic=base_public_ip
                    ))

                    existing_public_ip = None
                    for ip in existing_public_ips:
                        if ip.address.split('/')[0] == base_public_ip:
                            existing_public_ip = ip
                            break

                    if not existing_public_ip:
                        public_ip_data = {
                            "address": public_v4,
                            "assigned_object_type": "virtualization.vminterface",
                            "assigned_object_id": nb_interface.id,
                            "status": "active",
                            "description": "Public IP (NAT)"
                        }

                        created_public_ip = netbox.create_ip(public_ip_data)
                        if created_public_ip:
                            logger.info(f"Created public IP {public_v4} for interface {interface_name} on VM {vm.name}")
                            result["ips_created"] += 1
                        else:
                            logger.error(
                                f"Failed to create public IP {public_v4} for "
                                f"interface {interface_name} on VM {vm.name}"
                            )
                            result["errors"] += 1
                    else:
                        if (hasattr(existing_public_ip, 'assigned_object_id')
                                and existing_public_ip.assigned_object_id != nb_interface.id):
                            if netbox.dry_run:
                                logger.info(
                                    f"[DRY-RUN] Would reassign public IP "
                                    f"{base_public_ip} to interface "
                                    f"{interface_name} on VM {vm.name}"
                                )
                                result["ips_created"] += 1
                            else:
                                try:
                                    vms_with_primary = list(netbox.nb.virtualization.virtual_machines.filter(
                                        primary_ip4_id=existing_public_ip.id
                                    ))
                                    for vm_with_primary in vms_with_primary:
                                        logger.info(
                                            f"Unsetting public IP {base_public_ip} "
                                            f"as primary on VM {vm_with_primary.name}"
                                        )
                                        vm_with_primary.primary_ip4 = None
                                        vm_with_primary.save()

                                    existing_public_ip.assigned_object_type = "virtualization.vminterface"
                                    existing_public_ip.assigned_object_id = nb_interface.id
                                    existing_public_ip.save()
                                    logger.info(
                                        f"Updated public IP {base_public_ip} "
                                        f"(as {existing_public_ip.address}) assignment "
                                        f"to interface {interface_name} on VM {vm.name}"
                                    )
                                    result["ips_created"] += 1
                                except Exception as e:
                                    logger.error(f"Failed to update public IP {base_public_ip} assignment: {e}")
                                    result["errors"] += 1
                        else:
                            logger.debug(
                                f"Public IP {base_public_ip} "
                                f"(as {existing_public_ip.address}) "
                                f"already exists and properly assigned"
                            )

                except Exception as e:
                    logger.error(f"Failed to process public IP {public_v4} for VM {vm.name}: {e}")
                    result["errors"] += 1

        return result

    except Exception as e:
        logger.error(f"Failed to sync interfaces for VM {vm.name}: {e}")
        result["errors"] += 1
        return result


def sync_vms(
    yc_data: Dict[str, Any],
    netbox: NetBoxClient,
    id_mapping: Dict[str, Dict[str, int]],
    cleanup_orphaned: bool = True
) -> Dict[str, int]:
    """Sync VMs from Yandex Cloud to NetBox. Returns statistics."""
    yc_vms = yc_data.get("vms", [])

    if not yc_vms:
        logger.info("No VMs found in Yandex Cloud")
        return {"created": 0, "updated": 0, "skipped": 0, "deleted": 0, "errors": 0}

    logger.info(f"Found {len(yc_vms)} VMs in Yandex Cloud")

    # Clean up orphaned VMs first if requested
    deleted_count = 0
    if cleanup_orphaned:
        logger.info("Checking for orphaned VMs to clean up...")
        deleted_count = cleanup_orphaned_vms(yc_vms, netbox, netbox.dry_run)
        if deleted_count > 0:
            logger.info(f"Cleanup complete: removed {deleted_count} orphaned VMs")

    # Get existing VMs from NetBox
    existing_vms = netbox.fetch_vms()
    logger.info(f"Found {len(existing_vms)} existing VMs in NetBox")

    existing_vm_names = {}
    for vm in existing_vms:
        if hasattr(vm, 'name'):
            existing_vm_names[vm.name] = vm

    # Create or update VMs
    created_count = 0
    skipped_count = 0
    updated_count = 0
    failed_count = 0

    for yc_vm in yc_vms:
        vm_name = yc_vm.get("name", "")
        vm_id = yc_vm.get("id", "")

        if not vm_name or not isinstance(vm_name, str):
            logger.warning(f"Skipping VM without valid name: {vm_id}")
            skipped_count += 1
            continue

        try:
            if vm_name in existing_vm_names:
                existing_vm = existing_vm_names[vm_name]

                params_updated = update_vm_parameters(existing_vm, yc_vm, netbox, id_mapping)
                disk_sync_result = sync_vm_disks(existing_vm, yc_vm, netbox)
                disks_changed = disk_sync_result["created"] > 0 or disk_sync_result["deleted"] > 0
                interface_sync_result = sync_vm_interfaces(existing_vm, yc_vm, netbox)
                interfaces_changed = (
                    interface_sync_result["interfaces_created"] > 0
                    or interface_sync_result["ips_created"] > 0
                )
                ip_updated = update_vm_primary_ip(existing_vm, yc_vm, netbox)

                if params_updated or disks_changed or interfaces_changed or ip_updated:
                    updated_count += 1
                    if disks_changed:
                        logger.info(
                            f"VM {vm_name}: disk changes - "
                            f"created: {disk_sync_result['created']}, "
                            f"deleted: {disk_sync_result['deleted']}"
                        )
                    if interfaces_changed:
                        logger.info(
                            f"VM {vm_name}: interface changes - "
                            f"interfaces created: {interface_sync_result['interfaces_created']}, "
                            f"IPs created: {interface_sync_result['ips_created']}"
                        )
                else:
                    logger.debug(f"VM already exists and up to date: {vm_name}")
                    skipped_count += 1
                continue

            vm_data = prepare_vm_data(yc_vm, netbox, id_mapping)

            if netbox.dry_run:
                logger.info(f"[DRY-RUN] Would create VM: {vm_name}")
                created_count += 1
                continue

            created_vm = netbox.create_vm(vm_data)
            if not created_vm:
                logger.error(f"Failed to create VM: {vm_name}")
                failed_count += 1
                continue

            logger.info(f"Created VM: {vm_name}")
            created_count += 1

            # Add disks
            disks = yc_vm.get("disks", [])
            if isinstance(disks, list):
                for disk in disks:
                    if not isinstance(disk, dict):
                        continue

                    size = disk.get("size", 0)
                    if isinstance(size, (int, float)):
                        disk_data = {
                            "virtual_machine": created_vm.id,
                            "size": int(size) // (1024**2),
                            "name": str(disk.get("name", "disk"))
                        }
                        netbox.create_disk(disk_data)

            # Add network interfaces and IPs
            network_interfaces = yc_vm.get("network_interfaces", [])
            private_ip_id = None
            public_ip_id = None

            if isinstance(network_interfaces, list):
                for idx, iface in enumerate(network_interfaces):
                    if not isinstance(iface, dict):
                        continue

                    interface_data = {
                        "virtual_machine": created_vm.id,
                        "name": f"eth{idx}"
                    }
                    created_iface = netbox.create_interface(interface_data)

                    if not created_iface:
                        continue

                    primary_v4 = iface.get("primary_v4_address")
                    if primary_v4 and isinstance(primary_v4, str):
                        ip_data = {
                            "address": primary_v4,
                            "interface": created_iface.id
                        }
                        created_ip = netbox.create_ip(ip_data)
                        if created_ip:
                            if is_private_ip(primary_v4) and private_ip_id is None:
                                private_ip_id = created_ip.id
                            elif public_ip_id is None:
                                public_ip_id = created_ip.id

                    public_v4 = iface.get("primary_v4_address_one_to_one_nat")
                    if public_v4 and isinstance(public_v4, str):
                        ip_data = {
                            "address": public_v4,
                            "interface": created_iface.id
                        }
                        created_pub_ip = netbox.create_ip(ip_data)
                        if created_pub_ip and public_ip_id is None:
                            public_ip_id = created_pub_ip.id

            primary_ip_id = private_ip_id or public_ip_id
            if primary_ip_id:
                netbox.set_vm_primary_ip(created_vm.id, primary_ip_id, ip_version=4)
                logger.debug(f"Set primary IPv4 (ID: {primary_ip_id}) for VM: {vm_name}")

        except Exception as e:
            logger.error(f"Failed to sync VM {vm_name}: {e}")
            failed_count += 1
            continue

    logger.info(
        f"VM sync completed: {created_count} created, "
        f"{updated_count} updated, {skipped_count} skipped, {failed_count} failed"
    )

    return {
        "created": created_count,
        "updated": updated_count,
        "skipped": skipped_count,
        "deleted": deleted_count,
        "errors": failed_count,
    }

"""Optimized sync with batch operations and caching to minimize API calls."""

import logging
from typing import Any, Dict, List, Optional, Set
from collections import defaultdict
from dataclasses import dataclass, field

from netbox_sync.ip import is_private_ip, get_ip_without_cidr, ensure_cidr_notation
from netbox_sync.clients.netbox import NetBoxClient
from netbox_sync.sync.vms import prepare_vm_data, parse_memory_mb, parse_cores, detect_platform_id

logger = logging.getLogger(__name__)


@dataclass
class NetBoxCache:
    """Cache for NetBox data to minimize API calls."""
    vms: Dict[int, Any] = field(default_factory=dict)
    vms_by_name: Dict[str, Any] = field(default_factory=dict)
    interfaces_by_vm: Dict[int, List[Any]] = field(default_factory=lambda: defaultdict(list))
    ips: Dict[int, Any] = field(default_factory=dict)
    ips_by_address: Dict[str, Any] = field(default_factory=dict)
    ips_by_interface: Dict[int, List[Any]] = field(default_factory=lambda: defaultdict(list))
    disks_by_vm: Dict[int, List[Any]] = field(default_factory=lambda: defaultdict(list))
    vms_with_primary_ip: Dict[int, Set[int]] = field(default_factory=lambda: defaultdict(set))

    # Updates to be applied
    vms_to_update: Dict[int, Dict[str, Any]] = field(default_factory=dict)
    ips_to_update: Dict[int, Dict[str, Any]] = field(default_factory=dict)
    primary_ip_changes: Dict[int, Optional[Any]] = field(default_factory=dict)
    interfaces_to_create: List[Dict[str, Any]] = field(default_factory=list)
    ips_to_create: List[Dict[str, Any]] = field(default_factory=list)
    disks_to_create: List[Dict[str, Any]] = field(default_factory=list)
    disks_to_delete: List[Any] = field(default_factory=list)
    pending_primary_ips: Dict[int, str] = field(default_factory=dict)
    # Pending IP reassignments keyed by ip_id, value is pending interface key
    pending_ip_reassignments: Dict[int, str] = field(default_factory=dict)


def load_netbox_data(netbox: NetBoxClient) -> NetBoxCache:
    """Load all relevant data from NetBox in batch."""
    cache = NetBoxCache()

    logger.info("Loading NetBox data into cache...")

    # Load all VMs
    logger.info("Loading VMs...")
    all_vms = list(netbox.nb.virtualization.virtual_machines.all())
    for vm in all_vms:
        cache.vms[vm.id] = vm
        cache.vms_by_name[vm.name] = vm
        if hasattr(vm, 'primary_ip4') and vm.primary_ip4:
            cache.vms_with_primary_ip[vm.primary_ip4.id].add(vm.id)
    logger.info(f"Loaded {len(cache.vms)} VMs")

    # Load all interfaces
    logger.info("Loading interfaces...")
    all_interfaces = list(netbox.nb.virtualization.interfaces.all())
    for iface in all_interfaces:
        if hasattr(iface, 'virtual_machine') and iface.virtual_machine:
            vm_id = iface.virtual_machine.id
            cache.interfaces_by_vm[vm_id].append(iface)
    logger.info(f"Loaded {len(all_interfaces)} interfaces")

    # Load all IPs
    logger.info("Loading IP addresses...")
    all_ips = list(netbox.nb.ipam.ip_addresses.all())
    for ip in all_ips:
        cache.ips[ip.id] = ip
        base_address = ip.address.split('/')[0]
        cache.ips_by_address[base_address] = ip

        if hasattr(ip, 'assigned_object_id') and ip.assigned_object_id:
            if hasattr(ip, 'assigned_object_type') and 'vminterface' in str(ip.assigned_object_type):
                cache.ips_by_interface[ip.assigned_object_id].append(ip)
    logger.info(f"Loaded {len(cache.ips)} IP addresses")

    # Load all virtual disks
    logger.info("Loading virtual disks...")
    try:
        all_disks = list(netbox.nb.virtualization.virtual_disks.all())
        for disk in all_disks:
            if hasattr(disk, 'virtual_machine') and disk.virtual_machine:
                vm_id = disk.virtual_machine.id
                cache.disks_by_vm[vm_id].append(disk)
        logger.info(f"Loaded {len(all_disks)} virtual disks")
    except Exception as e:
        logger.warning(f"Could not load virtual disks (may not be supported): {e}")

    logger.info("Cache loading complete")
    return cache


def process_vm_updates(vm: Any, yc_vm: Dict[str, Any], cache: NetBoxCache,
                       id_mapping: Dict[str, Dict[str, int]],
                       netbox: NetBoxClient = None) -> bool:
    """Process all updates for a VM and queue them in cache. Returns True if changes needed."""
    vm_id = vm.id
    vm_name = vm.name
    changes_made = False

    # 1. Check VM parameters
    updates = {}

    # Memory - use shared parser for type safety
    resources = yc_vm.get("resources", {})
    if not isinstance(resources, dict):
        resources = {}
    memory_mb = parse_memory_mb(resources, vm_name)
    if memory_mb > 0 and vm.memory != memory_mb:
        updates["memory"] = memory_mb

    # CPU - use shared parser for type safety
    cpu_count = parse_cores(resources, vm_name)
    if cpu_count > 0 and vm.vcpus != cpu_count:
        updates["vcpus"] = cpu_count

    # Status
    yc_status = yc_vm.get("status", "")
    nb_status = "active" if yc_status == "RUNNING" else "offline"
    if hasattr(vm, 'status') and vm.status:
        if hasattr(vm.status, 'value'):
            current_status = vm.status.value
        else:
            current_status = str(vm.status)
        if current_status != nb_status:
            updates["status"] = nb_status

    # Cluster
    folder_id = yc_vm.get("folder_id")
    if folder_id and folder_id in id_mapping.get("folders", {}):
        cluster_id = id_mapping["folders"][folder_id]
        if not vm.cluster or vm.cluster.id != cluster_id:
            updates["cluster"] = cluster_id

    # Site
    zone_id = yc_vm.get("zone_id", "")
    if zone_id and zone_id in id_mapping.get("zones", {}):
        site_id = id_mapping["zones"][zone_id]
        if site_id and site_id > 0:
            current_site_id = None
            if hasattr(vm, 'site') and vm.site:
                current_site_id = vm.site.id if hasattr(vm.site, 'id') else vm.site
            if current_site_id != site_id:
                updates["site"] = site_id

    # Platform
    os_name = yc_vm.get("os", "")
    new_platform_id = detect_platform_id(os_name, netbox)
    if new_platform_id:
        current_platform_id = None
        if hasattr(vm, 'platform') and vm.platform:
            current_platform_id = vm.platform.id if hasattr(vm.platform, 'id') else vm.platform
        if current_platform_id != new_platform_id:
            updates["platform"] = new_platform_id

    # Comments
    yc_vm_id = yc_vm.get("id", "unknown")
    platform_id_str = yc_vm.get("platform_id", "")
    created_at = yc_vm.get("created_at", "")
    comments_parts = [
        f"YC VM ID: {yc_vm_id}",
        f"Zone: {zone_id}" if zone_id else None,
        f"Hardware Platform: {platform_id_str}" if platform_id_str else None,
        f"OS: {os_name}" if os_name else None,
        f"Created: {created_at}" if created_at else None,
    ]
    new_comments = "\n".join(filter(None, comments_parts))
    if hasattr(vm, 'comments') and vm.comments != new_comments:
        updates["comments"] = new_comments

    if updates:
        cache.vms_to_update[vm_id] = updates
        changes_made = True

    # 2. Process disks
    yc_disks = yc_vm.get("disks", [])
    existing_disks = cache.disks_by_vm[vm_id]

    if isinstance(yc_disks, list):
        yc_disk_map = {}
        for i, d in enumerate(yc_disks):
            if isinstance(d, dict):
                name = d.get("name", f"disk{i}")
                size_mb = d.get("size", 0) // (1024**2)
                if size_mb > 0:
                    yc_disk_map[name] = size_mb

        existing_disk_map = {d.name: d for d in existing_disks}

        # Find disks to create
        for name, size in yc_disk_map.items():
            if name not in existing_disk_map:
                cache.disks_to_create.append({
                    "virtual_machine": vm_id,
                    "size": size,
                    "name": name
                })
                changes_made = True

        # Find disks to delete
        for disk in existing_disks:
            if disk.name not in yc_disk_map:
                cache.disks_to_delete.append(disk)
                changes_made = True

    # 3. Process interfaces and IPs
    yc_interfaces = yc_vm.get("network_interfaces", [])
    existing_interfaces = cache.interfaces_by_vm[vm_id]
    existing_interface_map = {iface.name: iface for iface in existing_interfaces}

    private_ip_candidate = None
    public_ip_candidate = None

    for idx, yc_iface in enumerate(yc_interfaces):
        if not isinstance(yc_iface, dict):
            continue

        interface_name = f"eth{idx}"

        # Get or queue interface creation
        if interface_name in existing_interface_map:
            nb_interface = existing_interface_map[interface_name]
            nb_interface_id = nb_interface.id
        else:
            cache.interfaces_to_create.append({
                "virtual_machine": vm_id,
                "name": interface_name,
                "type": "virtual",
                "enabled": True
            })
            changes_made = True
            # Use pending key so IPs can be resolved after interface creation
            nb_interface_id = f"pending_{vm_id}_{interface_name}"

        # Process primary IPv4
        primary_v4 = yc_iface.get("primary_v4_address")
        if primary_v4 and isinstance(primary_v4, str):
            base_ip = get_ip_without_cidr(primary_v4)
            existing_ip = cache.ips_by_address.get(base_ip)

            if existing_ip:
                # Queue IP reassignment if needed
                if existing_ip.assigned_object_id != nb_interface_id:
                    for other_vm_id in cache.vms_with_primary_ip.get(existing_ip.id, set()):
                        if other_vm_id != vm_id:
                            cache.primary_ip_changes[other_vm_id] = None

                    if isinstance(nb_interface_id, str) and nb_interface_id.startswith("pending_"):
                        # Queue for resolution after interface creation
                        cache.pending_ip_reassignments[existing_ip.id] = nb_interface_id
                    else:
                        cache.ips_to_update[existing_ip.id] = {
                            "assigned_object_type": "virtualization.vminterface",
                            "assigned_object_id": nb_interface_id
                        }
                    changes_made = True

                if is_private_ip(base_ip):
                    if private_ip_candidate is None:
                        private_ip_candidate = existing_ip.id
                        logger.debug(f"Found private IP candidate: {base_ip}")
                else:
                    if public_ip_candidate is None:
                        public_ip_candidate = existing_ip.id
                        logger.debug(f"Found public IP candidate: {base_ip}")
            else:
                primary_v4 = ensure_cidr_notation(primary_v4)
                cache.ips_to_create.append({
                    "address": primary_v4,
                    "assigned_object_type": "virtualization.vminterface",
                    "assigned_object_id": nb_interface_id,
                    "status": "active",
                    "description": "Private IP" if is_private_ip(primary_v4) else ""
                })
                changes_made = True

                if is_private_ip(primary_v4):
                    if private_ip_candidate is None:
                        private_ip_candidate = "pending"
                        cache.pending_primary_ips[vm_id] = primary_v4

        # Process public IP
        public_v4 = yc_iface.get("primary_v4_address_one_to_one_nat")
        if public_v4 and isinstance(public_v4, str):
            base_public_ip = get_ip_without_cidr(public_v4)
            existing_public_ip = cache.ips_by_address.get(base_public_ip)

            if not existing_public_ip:
                public_v4 = ensure_cidr_notation(public_v4)
                cache.ips_to_create.append({
                    "address": public_v4,
                    "assigned_object_type": "virtualization.vminterface",
                    "assigned_object_id": nb_interface_id,
                    "status": "active",
                    "description": "Public IP (NAT)"
                })
                changes_made = True
            else:
                if not private_ip_candidate and public_ip_candidate is None:
                    public_ip_candidate = existing_public_ip.id

    # 4. Queue primary IP update if needed - ALWAYS prefer private IPs
    primary_ip_to_set = None

    if private_ip_candidate and private_ip_candidate != "pending":
        primary_ip_to_set = private_ip_candidate
        logger.debug(f"VM {vm_name}: Selecting private IP as primary")
    elif public_ip_candidate:
        primary_ip_to_set = public_ip_candidate
        logger.debug(f"VM {vm_name}: No private IP available, using public IP as primary")

    if not vm.primary_ip4 and primary_ip_to_set:
        cache.primary_ip_changes[vm_id] = primary_ip_to_set
        changes_made = True
    elif vm.primary_ip4 and primary_ip_to_set:
        current_primary_ip = cache.ips.get(vm.primary_ip4.id)
        if current_primary_ip:
            current_ip_str = get_ip_without_cidr(current_primary_ip.address)

            if private_ip_candidate:
                if private_ip_candidate == "pending":
                    if not is_private_ip(current_ip_str):
                        logger.info(
                            f"VM {vm_name}: Will switch primary from public IP"
                            f" {current_ip_str} to private IP (pending creation)"
                        )
                        cache.primary_ip_changes[vm_id] = "pending"
                        changes_made = True
                else:
                    if vm.primary_ip4.id != private_ip_candidate:
                        if not is_private_ip(current_ip_str):
                            logger.info(
                                f"VM {vm_name}: Switching primary from public"
                                f" IP {current_ip_str} to private IP"
                            )
                        else:
                            logger.info(
                                f"VM {vm_name}: Switching primary from"
                                f" {current_ip_str} to different private IP"
                            )
                        cache.primary_ip_changes[vm_id] = private_ip_candidate
                        changes_made = True
            elif public_ip_candidate and vm.primary_ip4.id != public_ip_candidate:
                logger.info(
                    f"VM {vm_name}: Updating primary to public IP"
                    f" {public_ip_candidate} (no private IP available)"
                )
                cache.primary_ip_changes[vm_id] = public_ip_candidate
                changes_made = True
    elif not vm.primary_ip4:
        # Fallback: find any existing IP, preferring private over public
        fallback_private = None
        fallback_public = None
        for iface in existing_interfaces:
            for ip_obj in cache.ips_by_interface.get(iface.id, []):
                ip_str = get_ip_without_cidr(ip_obj.address)
                if is_private_ip(ip_str) and not fallback_private:
                    fallback_private = ip_obj.id
                elif not fallback_public:
                    fallback_public = ip_obj.id
        fallback_ip = fallback_private or fallback_public
        if fallback_ip:
            cache.primary_ip_changes[vm_id] = fallback_ip
            changes_made = True
        elif private_ip_candidate == "pending":
            cache.primary_ip_changes[vm_id] = "pending"
            changes_made = True

    return changes_made


def apply_batch_updates(cache: NetBoxCache, netbox: NetBoxClient,
                        dry_run: bool = False) -> Dict[str, int]:
    """Apply all cached updates in batch."""
    stats = {
        "vms_updated": 0,
        "ips_updated": 0,
        "ips_reassigned": 0,
        "primary_ips_changed": 0,
        "interfaces_created": 0,
        "ips_created": 0,
        "disks_created": 0,
        "disks_deleted": 0,
        "errors": 0
    }

    if dry_run:
        logger.info("[DRY-RUN] Would apply the following updates:")
        logger.info(f"  VMs to update: {len(cache.vms_to_update)}")
        logger.info(f"  IPs to update: {len(cache.ips_to_update)}")
        logger.info(f"  Primary IP changes: {len(cache.primary_ip_changes)}")
        logger.info(f"  Interfaces to create: {len(cache.interfaces_to_create)}")
        logger.info(f"  IPs to create: {len(cache.ips_to_create)}")
        logger.info(f"  Disks to create: {len(cache.disks_to_create)}")
        logger.info(f"  Disks to delete: {len(cache.disks_to_delete)}")
        return stats

    logger.info("Applying batch updates...")

    # Step 1: Unset primary IPs that need to be moved
    logger.info("Step 1: Unsetting primary IPs that need reassignment...")
    for vm_id, new_ip_id in cache.primary_ip_changes.items():
        if new_ip_id is None:
            try:
                vm = cache.vms[vm_id]
                if vm.primary_ip4:
                    vm.primary_ip4 = None
                    vm.save()
                    stats["primary_ips_changed"] += 1
                    logger.debug(f"Unset primary IP on VM {vm.name}")
            except Exception as e:
                logger.error(f"Failed to unset primary IP on VM {vm_id}: {e}")
                stats["errors"] += 1

    # Step 2: Delete disks
    logger.info("Step 2: Deleting obsolete disks...")
    for disk in cache.disks_to_delete:
        try:
            disk.delete()
            stats["disks_deleted"] += 1
            logger.debug(f"Deleted disk {disk.name}")
        except Exception as e:
            logger.error(f"Failed to delete disk: {e}")
            stats["errors"] += 1

    # Step 3: Create interfaces
    logger.info("Step 3: Creating new interfaces...")
    created_interfaces = {}
    for iface_data in cache.interfaces_to_create:
        try:
            iface = netbox.create_interface(iface_data)
            if iface:
                stats["interfaces_created"] += 1
                vm_id = iface_data.get("virtual_machine")
                iface_name = iface_data.get("name")
                created_interfaces[f"{vm_id}_{iface_name}"] = iface
                logger.debug(f"Created interface {iface_name} for VM ID {vm_id}")
        except Exception as e:
            logger.error(f"Failed to create interface: {e}")
            stats["errors"] += 1

    # Step 3b: Resolve pending IP reassignments now that interfaces exist
    for ip_id, pending_key in cache.pending_ip_reassignments.items():
        lookup_key = pending_key[len("pending_"):]
        if lookup_key in created_interfaces:
            cache.ips_to_update[ip_id] = {
                "assigned_object_type": "virtualization.vminterface",
                "assigned_object_id": created_interfaces[lookup_key].id
            }
        else:
            logger.warning(f"Could not resolve pending interface {pending_key} for IP reassignment {ip_id}")
            stats["errors"] += 1

    # Step 4: Update/reassign existing IPs
    logger.info("Step 4: Updating IP assignments...")
    for ip_id, updates in cache.ips_to_update.items():
        try:
            ip = cache.ips[ip_id]
            for key, value in updates.items():
                setattr(ip, key, value)
            ip.save()
            stats["ips_reassigned"] += 1
            logger.debug(f"Updated IP {ip.address}")
        except Exception as e:
            logger.error(f"Failed to update IP {ip_id}: {e}")
            stats["errors"] += 1

    # Step 5: Create new IPs
    logger.info("Step 5: Creating new IPs...")
    created_ips = {}
    for ip_data in cache.ips_to_create:
        try:
            # Resolve pending interface IDs for newly created interfaces
            assigned_id = ip_data.get('assigned_object_id')
            if isinstance(assigned_id, str) and assigned_id.startswith("pending_"):
                lookup_key = assigned_id[len("pending_"):]
                if lookup_key in created_interfaces:
                    ip_data['assigned_object_id'] = created_interfaces[lookup_key].id
                else:
                    logger.warning(f"Could not resolve pending interface {assigned_id}, skipping IP creation")
                    stats["errors"] += 1
                    continue

            ip = netbox.create_ip(ip_data)
            if ip:
                stats["ips_created"] += 1
                base_ip = ip_data['address'].split('/')[0] if '/' in ip_data['address'] else ip_data['address']
                created_ips[base_ip] = ip
                logger.debug(f"Created IP {ip_data['address']} with ID {ip.id}")
        except Exception as e:
            logger.error(f"Failed to create IP: {e}")
            stats["errors"] += 1

    # Resolve pending primary IPs after creation
    for vm_id, pending_ip in cache.pending_primary_ips.items():
        if cache.primary_ip_changes.get(vm_id) == "pending":
            base_pending_ip = pending_ip.split('/')[0] if '/' in pending_ip else pending_ip
            if base_pending_ip in created_ips:
                cache.primary_ip_changes[vm_id] = created_ips[base_pending_ip].id
                logger.debug(
                    f"Resolved pending primary IP for VM ID {vm_id}"
                    f" to IP ID {created_ips[base_pending_ip].id}"
                )
            else:
                vm = cache.vms.get(vm_id)
                vm_name = vm.name if vm else vm_id
                logger.warning(
                    f"VM {vm_name}: pending primary IP {pending_ip}"
                    " could not be resolved (IP creation may have failed)"
                )
                stats["errors"] += 1

    # Step 6: Create disks
    logger.info("Step 6: Creating new disks...")
    for disk_data in cache.disks_to_create:
        try:
            disk = netbox.create_disk(disk_data)
            if disk:
                stats["disks_created"] += 1
                logger.debug(f"Created disk {disk_data['name']}")
        except Exception as e:
            logger.error(f"Failed to create disk: {e}")
            stats["errors"] += 1

    # Step 7: Update VMs
    logger.info("Step 7: Updating VM parameters...")
    for vm_id, updates in cache.vms_to_update.items():
        try:
            vm = cache.vms[vm_id]
            for key, value in updates.items():
                setattr(vm, key, value)
            vm.save()
            stats["vms_updated"] += 1
            logger.debug(f"Updated VM {vm.name}")
        except Exception as e:
            logger.error(f"Failed to update VM {vm_id}: {e}")
            stats["errors"] += 1

    # Step 8: Set new primary IPs with proper assignment check
    logger.info("Step 8: Setting new primary IPs...")
    for vm_id, ip_id in cache.primary_ip_changes.items():
        if ip_id is not None and ip_id != "pending":
            try:
                vm = cache.vms[vm_id]

                ip = cache.ips.get(ip_id)
                if not ip:
                    ip = netbox.nb.ipam.ip_addresses.get(id=ip_id)
                    if not ip:
                        logger.error(f"IP with ID {ip_id} not found")
                        stats["errors"] += 1
                        continue

                vm_interfaces = cache.interfaces_by_vm.get(vm_id, [])
                if not vm_interfaces:
                    vm_interfaces = list(netbox.nb.virtualization.interfaces.filter(virtual_machine_id=vm_id))

                if not vm_interfaces:
                    logger.error(f"VM {vm.name} has no interfaces to assign IP to")
                    stats["errors"] += 1
                    continue

                ip_assigned_to_vm = False
                if hasattr(ip, 'assigned_object_id') and ip.assigned_object_id:
                    for iface in vm_interfaces:
                        if ip.assigned_object_id == iface.id:
                            ip_assigned_to_vm = True
                            break

                if not ip_assigned_to_vm:
                    logger.info(
                        f"Assigning IP {ip.address} to VM {vm.name}'s"
                        " first interface before setting as primary"
                    )
                    ip.assigned_object_type = "virtualization.vminterface"
                    ip.assigned_object_id = vm_interfaces[0].id
                    ip.save()
                    stats["ips_reassigned"] += 1

                vm.primary_ip4 = ip_id
                vm.save()
                stats["primary_ips_changed"] += 1
                logger.debug(f"Set primary IP {ip.address} (ID: {ip_id}) on VM {vm.name}")
            except Exception as e:
                logger.error(f"Failed to set primary IP on VM {vm_id}: {e}")
                stats["errors"] += 1

    logger.info(f"Batch updates complete: {stats}")
    return stats


def sync_vms_optimized(yc_data: Dict[str, Any], netbox: NetBoxClient,
                       id_mapping: Dict[str, Dict[str, int]],
                       cleanup_orphaned: bool = True) -> Dict[str, int]:
    """
    Optimized VM synchronization with batch operations.

    Returns statistics dictionary.
    """
    stats = {
        "created": 0,
        "updated": 0,
        "skipped": 0,
        "deleted": 0,
        "errors": 0
    }

    yc_vms = yc_data.get("vms", [])
    if not yc_vms:
        logger.info("No VMs found in Yandex Cloud")
        return stats

    logger.info(f"Found {len(yc_vms)} VMs in Yandex Cloud")

    # Load all NetBox data into cache
    cache = load_netbox_data(netbox)

    # Process orphaned VMs if requested
    if cleanup_orphaned:
        logger.info("Checking for orphaned VMs...")
        yc_vm_names = {vm.get("name") for vm in yc_vms if vm.get("name")}
        tag_id = netbox.ensure_sync_tag()

        for vm_name, vm in cache.vms_by_name.items():
            vm_tags = []
            if hasattr(vm, 'tags') and vm.tags:
                vm_tags = [t.id if hasattr(t, 'id') else t for t in vm.tags]

            if tag_id in vm_tags and vm_name not in yc_vm_names:
                if not netbox.dry_run:
                    try:
                        vm.delete()
                        logger.info(f"Deleted orphaned VM: {vm_name}")
                        stats["deleted"] += 1
                    except Exception as e:
                        logger.error(f"Failed to delete orphaned VM {vm_name}: {e}")
                        stats["errors"] += 1
                else:
                    logger.info(f"[DRY-RUN] Would delete orphaned VM: {vm_name}")
                    stats["deleted"] += 1

    # Process each YC VM
    for yc_vm in yc_vms:
        vm_name = yc_vm.get("name", "")

        if not vm_name:
            logger.warning("Skipping VM without name")
            stats["skipped"] += 1
            continue

        try:
            if vm_name in cache.vms_by_name:
                # Update existing VM
                existing_vm = cache.vms_by_name[vm_name]
                if process_vm_updates(existing_vm, yc_vm, cache, id_mapping, netbox):
                    stats["updated"] += 1
                else:
                    stats["skipped"] += 1
            else:
                # Create new VM
                vm_data = prepare_vm_data(yc_vm, netbox, id_mapping)

                if not netbox.dry_run:
                    created_vm = netbox.create_vm(vm_data)
                    if created_vm:
                        logger.info(f"Created VM: {vm_name}")
                        stats["created"] += 1
                        cache.vms[created_vm.id] = created_vm
                        cache.vms_by_name[vm_name] = created_vm
                        process_vm_updates(created_vm, yc_vm, cache, id_mapping, netbox)
                    else:
                        stats["errors"] += 1
                else:
                    logger.info(f"[DRY-RUN] Would create VM: {vm_name}")
                    stats["created"] += 1

        except Exception as e:
            logger.error(f"Failed to process VM {vm_name}: {e}")
            stats["errors"] += 1

    # Apply all cached updates in batch
    batch_stats = apply_batch_updates(cache, netbox, dry_run=netbox.dry_run)

    # Log summary
    logger.info("=" * 60)
    logger.info("Sync Summary:")
    logger.info(f"  VMs created: {stats['created']}")
    logger.info(f"  VMs updated: {stats['updated']}")
    logger.info(f"  VMs deleted: {stats['deleted']}")
    logger.info(f"  VMs skipped: {stats['skipped']}")
    logger.info(f"  Errors: {stats['errors']}")
    logger.info("Batch update statistics:")
    for key, value in batch_stats.items():
        logger.info(f"  {key}: {value}")
    logger.info("=" * 60)

    return stats

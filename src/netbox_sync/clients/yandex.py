"""
Yandex Cloud API client using httpx for fetching cloud resources.
"""

import logging
from typing import Any, Dict, List

import httpx

logger = logging.getLogger(__name__)


class YandexCloudClient:
    """Collects VM, disk, network data from Yandex Cloud for all clouds/folders."""

    def __init__(self, token: str):
        """Initialize Yandex Cloud client with OAuth token."""
        self.token = token
        self.headers = {"Authorization": f"Bearer {self.token}"}
        self.client = httpx.Client(timeout=30.0, headers=self.headers)

    def __del__(self):
        """Close httpx client on deletion."""
        if hasattr(self, 'client'):
            self.client.close()

    def fetch_zones(self) -> List[Dict[str, Any]]:
        """Fetch availability zones."""
        url = "https://compute.api.cloud.yandex.net/compute/v1/zones"
        resp = self.client.get(url)
        resp.raise_for_status()
        zones = resp.json().get("zones", [])
        logger.info(f"Fetched {len(zones)} availability zones")
        return zones

    def fetch_clouds(self) -> List[Dict[str, Any]]:
        """Fetch all clouds with pagination support."""
        url = "https://resource-manager.api.cloud.yandex.net/resource-manager/v1/clouds"
        params: Dict[str, str] = {}
        all_clouds: List[Dict[str, Any]] = []

        while True:
            resp = self.client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()
            all_clouds.extend(data.get("clouds", []))
            next_page_token = data.get("nextPageToken")
            if not next_page_token:
                break
            params["pageToken"] = next_page_token

        return all_clouds

    def fetch_folders(self, cloud_id: str) -> List[Dict[str, Any]]:
        """Fetch folders in a cloud with pagination support."""
        url = "https://resource-manager.api.cloud.yandex.net/resource-manager/v1/folders"
        params: Dict[str, str] = {"cloudId": cloud_id}
        all_folders: List[Dict[str, Any]] = []

        while True:
            resp = self.client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()
            all_folders.extend(data.get("folders", []))
            next_page_token = data.get("nextPageToken")
            if not next_page_token:
                break
            params["pageToken"] = next_page_token

        return all_folders

    def fetch_vpcs(self, folder_id: str) -> List[Dict[str, Any]]:
        """Fetch VPCs in a folder with pagination support."""
        url = "https://vpc.api.cloud.yandex.net/vpc/v1/networks"
        params: Dict[str, str] = {"folderId": folder_id}
        all_networks: List[Dict[str, Any]] = []

        while True:
            resp = self.client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()
            all_networks.extend(data.get("networks", []))
            next_page_token = data.get("nextPageToken")
            if not next_page_token:
                break
            params["pageToken"] = next_page_token

        return all_networks

    def fetch_subnets(self, folder_id: str) -> List[Dict[str, Any]]:
        """Fetch all subnets in a folder with pagination support."""
        url = "https://vpc.api.cloud.yandex.net/vpc/v1/subnets"
        params = {"folderId": folder_id}
        all_subnets = []

        while True:
            resp = self.client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()

            subnets = data.get("subnets", [])
            all_subnets.extend(subnets)

            # Check if there are more pages
            next_page_token = data.get("nextPageToken")
            if not next_page_token:
                break

            params["pageToken"] = next_page_token

        return all_subnets

    def fetch_vms_in_folder(self, folder_id: str) -> List[Dict[str, Any]]:
        """Fetch VMs in a folder with pagination support."""
        url = "https://compute.api.cloud.yandex.net/compute/v1/instances"
        params = {"folderId": folder_id}
        all_instances = []

        while True:
            resp = self.client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()

            instances = data.get("instances", [])
            all_instances.extend(instances)

            next_page_token = data.get("nextPageToken")
            if not next_page_token:
                break
            params["pageToken"] = next_page_token

        # Log first VM structure for debugging
        if all_instances:
            first_vm = all_instances[0]
            logger.debug("Sample VM data structure from YC API:")
            logger.debug(f"  VM ID: {first_vm.get('id')}")
            logger.debug(f"  VM Name: {first_vm.get('name')}")

            resources = first_vm.get('resources', {})
            logger.debug(f"  Resources: {resources}")
            if resources:
                logger.debug(
                    f"    Memory (raw): {resources.get('memory')}"
                    f" (type: {type(resources.get('memory')).__name__})"
                )
                logger.debug(f"    Cores: {resources.get('cores')} (type: {type(resources.get('cores')).__name__})")
                logger.debug(f"    Core fraction: {resources.get('coreFraction')}")

        return all_instances

    def fetch_disk(self, disk_id: str) -> Dict[str, Any]:
        """Fetch disk details."""
        url = f"https://compute.api.cloud.yandex.net/compute/v1/disks/{disk_id}"
        resp = self.client.get(url)
        resp.raise_for_status()
        return resp.json()

    def fetch_image(self, image_id: str) -> Dict[str, Any]:
        """Fetch image details."""
        url = f"https://compute.api.cloud.yandex.net/compute/v1/images/{image_id}"
        resp = self.client.get(url)
        resp.raise_for_status()
        return resp.json()

    def fetch_all_data(self) -> Dict[str, Any]:
        """
        Fetches all data from Yandex Cloud including VMs, VPCs, subnets, and zones.

        Returns a structured dictionary with:
        - zones: availability zones (will be mapped to NetBox sites)
        - clouds: cloud organizations
        - folders: folders (will be mapped to NetBox clusters)
        - vpcs: virtual private clouds
        - subnets: network subnets
        - vms: virtual machines
        """
        result = {
            "zones": [],
            "clouds": [],
            "folders": [],
            "vpcs": [],
            "subnets": [],
            "vms": [],
            "_has_fetch_errors": False,
        }

        # Fetch availability zones first
        try:
            result["zones"] = self.fetch_zones()
        except Exception as e:
            logger.warning(f"Failed to fetch zones: {e}")
            # Create default zones if API call fails
            result["zones"] = [
                {"id": "ru-central1-a", "name": "ru-central1-a", "regionId": "ru-central1"},
                {"id": "ru-central1-b", "name": "ru-central1-b", "regionId": "ru-central1"},
                {"id": "ru-central1-c", "name": "ru-central1-c", "regionId": "ru-central1"},
                {"id": "ru-central1-d", "name": "ru-central1-d", "regionId": "ru-central1"},
            ]

        clouds = self.fetch_clouds()
        for cloud in clouds:
            cloud_id = cloud["id"]
            cloud_name = cloud.get("name", cloud_id)
            result["clouds"].append({
                "id": cloud_id,
                "name": cloud_name,
                "description": cloud.get("description", "")
            })

            folders = self.fetch_folders(cloud_id)
            for folder in folders:
                folder_id = folder["id"]
                folder_name = folder.get("name", folder_id)
                result["folders"].append({
                    "id": folder_id,
                    "name": folder_name,
                    "cloud_id": cloud_id,
                    "cloud_name": cloud_name,
                    "description": folder.get("description", "")
                })

                # Fetch VPCs and subnets
                try:
                    vpcs = self.fetch_vpcs(folder_id)
                except Exception as e:
                    logger.error(f"Failed to fetch VPCs for folder {folder_name}: {e}")
                    vpcs = []
                    result["_has_fetch_errors"] = True
                for vpc in vpcs:
                    vpc_id = vpc["id"]
                    vpc_name = vpc.get("name", vpc_id)
                    result["vpcs"].append({
                        "id": vpc_id,
                        "name": vpc_name,
                        "folder_id": folder_id,
                        "folder_name": folder_name,
                        "cloud_id": cloud_id,
                        "cloud_name": cloud_name,
                        "description": vpc.get("description", "")
                    })

                # Fetch all subnets in the folder
                try:
                    subnets = self.fetch_subnets(folder_id)
                except Exception as e:
                    logger.error(f"Failed to fetch subnets for folder {folder_name}: {e}")
                    subnets = []
                    result["_has_fetch_errors"] = True
                for subnet in subnets:
                    vpc_id = subnet["networkId"]
                    vpc_info = next((v for v in result["vpcs"] if v["id"] == vpc_id), None)
                    # Extract zone_id - it should be in zoneId field
                    zone_id = subnet.get("zoneId")
                    if not zone_id:
                        # Try alternative field names
                        zone_id = subnet.get("zone_id") or subnet.get("zone")

                    result["subnets"].append({
                        "id": subnet["id"],
                        "name": subnet.get("name", subnet["id"]),
                        "cidr": subnet["v4CidrBlocks"][0] if subnet.get("v4CidrBlocks") else None,
                        "vpc_id": vpc_id,
                        "vpc_name": vpc_info["name"] if vpc_info else None,
                        "folder_id": folder_id,
                        "folder_name": folder_name,
                        "cloud_id": cloud_id,
                        "cloud_name": cloud_name,
                        "zone_id": zone_id,
                        "description": subnet.get("description", "")
                    })

                # Fetch VMs
                try:
                    folder_vms = self.fetch_vms_in_folder(folder_id)
                except Exception as e:
                    logger.error(f"Failed to fetch VMs for folder {folder_name}: {e}")
                    folder_vms = []
                    result["_has_fetch_errors"] = True
                logger.info(f"Fetched {len(folder_vms)} VMs from folder {folder_name} ({folder_id})")

                for vm in folder_vms:
                    # Log memory for each VM
                    vm_resources = vm.get('resources', {})
                    vm_memory = vm_resources.get('memory', 0)
                    logger.debug(f"VM {vm.get('name')}: memory={vm_memory}, cores={vm_resources.get('cores', 0)}")

                    # Determine zone from VM data - try multiple field names
                    zone_id = vm.get("zoneId") or vm.get("zone_id") or vm.get("zone")
                    if not zone_id:
                        # Try to get zone from placement policy
                        placement_policy = vm.get("placementPolicy", {})
                        if placement_policy:
                            zone_id = placement_policy.get("zoneId") or placement_policy.get("zone")

                    # Fetch all disks (boot, secondary, local)
                    disk_ids = []
                    os_name = None

                    if "bootDisk" in vm and vm["bootDisk"].get("diskId"):
                        disk_ids.append(vm["bootDisk"]["diskId"])
                        # Try to get OS from source_image_id
                        try:
                            full_disk_info = self.fetch_disk(vm["bootDisk"]["diskId"])
                            image_id = full_disk_info.get("sourceImageId")
                            if image_id:
                                image = self.fetch_image(image_id)
                                os_name = image.get("name")
                        except Exception as e:
                            logger.debug(f"Could not determine OS for VM {vm.get('name')}: {e}")

                    for d in vm.get("secondaryDisks", []):
                        if d.get("diskId"):
                            disk_ids.append(d["diskId"])

                    # Local disks (size only, no diskId)
                    local_disks = vm.get("localDisks", [])
                    disks = []

                    for disk_id in disk_ids:
                        try:
                            disk = self.fetch_disk(disk_id)
                            disks.append({
                                "id": disk["id"],
                                "size": int(disk["size"]),
                                "name": disk.get("name", disk["id"]),
                                "type": "cloud"
                            })
                        except Exception as e:
                            logger.warning(f"Failed to fetch disk {disk_id}: {e}")

                    for ld in local_disks:
                        disks.append({
                            "id": None,
                            "size": int(ld["size"]),
                            "name": ld.get("deviceName", "local"),
                            "type": "local"
                        })

                    # Find VPC and subnet for each network interface
                    network_interfaces = []
                    for idx, iface in enumerate(vm.get("networkInterfaces", [])):
                        vpc_id = iface.get("networkId")
                        subnet_id = iface.get("subnetId")
                        vpc_info = next((v for v in result["vpcs"] if v["id"] == vpc_id), None)
                        subnet_info = next((s for s in result["subnets"] if s["id"] == subnet_id), None)

                        primary_v4_obj = iface.get("primaryV4Address") or {}
                        network_interfaces.append({
                            "index": idx,
                            "vpc_id": vpc_id,
                            "vpc_name": vpc_info["name"] if vpc_info else None,
                            "subnet_id": subnet_id,
                            "subnet_name": subnet_info["name"] if subnet_info else None,
                            "primary_v4_address": primary_v4_obj.get("address"),
                            "primary_v4_address_one_to_one_nat": (
                                primary_v4_obj.get("oneToOneNat") or {}
                            ).get("address"),
                            "zone_id": subnet_info.get("zone_id") if subnet_info else zone_id  # Fallback to VM zone
                        })

                        # If we still don't have zone_id, try to get it from subnet
                        if not zone_id and subnet_info:
                            zone_id = subnet_info.get("zone_id")

                    # Normalize VM structure
                    # Log if zone_id is missing
                    if not zone_id:
                        logger.debug(f"VM {vm.get('name', vm['id'])} has no zone_id")

                    result["vms"].append({
                        "id": vm["id"],
                        "name": vm.get("name", vm["id"]),
                        "status": vm["status"],
                        "folder_id": folder_id,
                        "folder_name": folder_name,
                        "cloud_id": cloud_id,
                        "cloud_name": cloud_name,
                        "zone_id": zone_id,  # Availability zone (can be None)
                        "resources": vm.get("resources", {}),
                        "disks": disks,
                        "network_interfaces": network_interfaces,
                        "os": os_name,
                        "description": vm.get("description", ""),
                        "labels": vm.get("labels", {}),
                        "created_at": vm.get("createdAt"),
                        "platform_id": vm.get("platformId", "standard-v3")
                    })

        logger.info(
            f"Fetched Yandex Cloud data: {len(result['zones'])} zones, "
            f"{len(result['clouds'])} clouds, {len(result['folders'])} folders, "
            f"{len(result['vpcs'])} VPCs, {len(result['subnets'])} subnets, "
            f"{len(result['vms'])} VMs"
        )

        return result

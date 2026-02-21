"""
NetBox client for managing virtualization resources.
Maps Yandex Cloud structure to NetBox:
- Sites = Availability Zones
- Cluster Type = yandex-cloud
- Clusters = Folders
"""

import logging
import re
from typing import Any, Dict, List, Optional

import pynetbox
from pynetbox.core.response import Record

logger = logging.getLogger(__name__)


class NetBoxClient:
    """NetBox API client for VM synchronization with Yandex Cloud mapping."""

    def __init__(
        self,
        url: str,
        token: str,
        dry_run: bool = False
    ):
        """
        Initialize NetBox client.

        Args:
            url: NetBox API URL
            token: NetBox API token
            dry_run: If True, don't make actual changes
        """
        self.nb = pynetbox.api(url, token=token)
        self.dry_run = dry_run
        self._cluster_type_id: Optional[int] = None

        logger.info(f"Initialized NetBox client for {url} (dry_run={dry_run})")
        self._sync_tag_id: Optional[int] = None

    def ensure_sync_tag(self) -> int:
        """
        Ensure the 'synced-from-yc' tag exists in NetBox.

        Returns:
            Tag ID
        """
        if self._sync_tag_id is not None:
            return self._sync_tag_id

        tag_name = "synced-from-yc"
        tag_slug = "synced-from-yc"
        tag_color = "2196f3"  # Blue color
        tag_description = "Object synced from Yandex Cloud"

        # Check if tag exists
        tag = None
        try:
            tag = self.nb.extras.tags.get(name=tag_name)
        except Exception:
            try:
                tag = self.nb.extras.tags.get(slug=tag_slug)
            except Exception:
                pass

        if tag:
            self._sync_tag_id = tag.id
            logger.debug(f"Found existing tag: {tag_name} (ID: {tag.id})")
            return tag.id

        # Create tag if it doesn't exist
        if self.dry_run:
            logger.info(f"[DRY-RUN] Would create tag: {tag_name}")
            self._sync_tag_id = 1  # Mock ID
            return 1

        try:
            tag = self.nb.extras.tags.create({
                "name": tag_name,
                "slug": tag_slug,
                "color": tag_color,
                "description": tag_description
            })
            self._sync_tag_id = tag.id
            logger.info(f"Created tag: {tag_name} (ID: {tag.id})")
            return tag.id
        except Exception as e:
            if '400' in str(e) and 'slug' in str(e).lower():
                # Tag might exist with different name, try to get by slug
                try:
                    tag = self.nb.extras.tags.get(slug=tag_slug)
                    if tag:
                        self._sync_tag_id = tag.id
                        return tag.id
                except Exception:
                    pass
            logger.error(f"Failed to create tag: {e}")
            # Continue without tag
            return 0

    def _add_tag_to_object(self, obj: Any, tag_id: int) -> bool:
        """
        Add sync tag to a NetBox object.

        Args:
            obj: NetBox object to tag
            tag_id: Tag ID to add

        Returns:
            True if tag was added, False otherwise
        """
        if not tag_id or self.dry_run:
            return False

        try:
            # Get current tags
            current_tags = []
            if hasattr(obj, 'tags'):
                current_tags = list(obj.tags) if obj.tags else []

            # Normalize to integer IDs to avoid mixed Record/int types
            tag_ids = [t.id if hasattr(t, 'id') else t for t in current_tags]
            if tag_id in tag_ids:
                return True

            # Add tag using normalized ID list
            tag_ids.append(tag_id)
            obj.tags = tag_ids
            obj.save()
            logger.debug(f"Added sync tag to object: {getattr(obj, 'name', str(obj))}")
            return True

        except Exception as e:
            logger.debug(f"Could not add tag to object: {e}")
            return False

    def _safe_update_object(self, obj: Any, updates: Dict[str, Any]) -> bool:
        """
        Safely update a NetBox object with the given updates.

        Args:
            obj: NetBox object to update
            updates: Dictionary of fields to update

        Returns:
            True if object was updated, False otherwise
        """
        if not updates or self.dry_run:
            return False

        needs_update = False

        try:
            # Check and apply updates
            for field, new_value in updates.items():
                current_value = getattr(obj, field, None)

                # Handle object comparisons (e.g., site.id vs site object)
                if hasattr(current_value, 'id'):
                    current_value = current_value.id
                elif hasattr(current_value, 'value'):
                    # Handle pynetbox ChoiceItem objects (e.g., status)
                    current_value = current_value.value

                if current_value != new_value:
                    setattr(obj, field, new_value)
                    needs_update = True
                    logger.debug(f"Setting {field} from {current_value} to {new_value}")

            # Save if there were changes
            if needs_update:
                obj.save()
                obj_name = getattr(obj, 'name', str(obj))
                logger.info(f"Updated object: {obj_name}")
                return True

        except Exception as e:
            obj_name = getattr(obj, 'name', str(obj))
            logger.warning(f"Could not update object {obj_name}: {e}")

        return False

    def ensure_site(self, zone_id: str, zone_name: Optional[str] = None) -> int:
        """
        Ensure availability zone exists as a site in NetBox.

        Args:
            zone_id: Zone ID (e.g., "ru-central1-a")
            zone_name: Optional zone display name

        Returns:
            Site ID
        """
        name = zone_name or zone_id
        slug = zone_id.lower().replace("_", "-")
        description = f"Yandex Cloud Availability Zone: {zone_id}"

        # Check if site exists by name or slug
        site = None

        # Try by name first
        try:
            site = self.nb.dcim.sites.get(name=name)
        except Exception:
            pass

        # If not found by name, try by slug
        if not site:
            try:
                site = self.nb.dcim.sites.get(slug=slug)
            except Exception:
                pass

        if site:
            # Check and apply updates if needed
            updates = {}

            if getattr(site, 'name', None) != name:
                updates['name'] = name
            if getattr(site, 'slug', None) != slug:
                updates['slug'] = slug
            if getattr(site, 'description', None) != description:
                updates['description'] = description
            current_status = getattr(site, 'status', None)
            status_value = getattr(current_status, 'value', current_status)
            if status_value != 'active':
                updates['status'] = 'active'

            self._safe_update_object(site, updates)

            # Add sync tag
            tag_id = self.ensure_sync_tag()
            if tag_id:
                self._add_tag_to_object(site, tag_id)

            return site.id

        # Create site if it doesn't exist
        if self.dry_run:
            logger.info(f"[DRY-RUN] Would create site for zone: {name}")
            return 1  # Mock ID for dry run

        # Ensure tag exists
        tag_id = self.ensure_sync_tag()

        site_data = {
            "name": name,
            "slug": slug,
            "status": "active",
            "description": description
        }

        # Add tag if available
        if tag_id:
            site_data["tags"] = [tag_id]

        try:
            site = self.nb.dcim.sites.create(site_data)
            logger.info(f"Created site for zone: {name} (ID: {site.id})")
            return site.id
        except Exception as e:
            error_msg = str(e)
            # Check if it's a duplicate slug error
            if '400' in error_msg and 'slug' in error_msg.lower():
                logger.warning(f"Site with slug '{slug}' already exists, trying to fetch it")
                # Try to get existing site
                try:
                    site = self.nb.dcim.sites.get(slug=slug)
                    if site:
                        logger.info(f"Found existing site: {site.name} (ID: {site.id})")
                        return site.id
                except Exception:
                    pass

                # Try by name again
                try:
                    site = self.nb.dcim.sites.get(name=name)
                    if site:
                        logger.info(f"Found existing site by name: {name} (ID: {site.id})")
                        return site.id
                except Exception:
                    pass

            logger.error(f"Failed to create or find site for zone {name}: {e}")
            raise

    def ensure_cluster_type(self) -> int:
        """
        Ensure 'yandex-cloud' cluster type exists.

        Returns:
            Cluster type ID
        """
        if self._cluster_type_id is not None:
            return self._cluster_type_id

        desired_name = "yandex-cloud"
        desired_slug = "yandex-cloud"
        desired_description = "Yandex Cloud Platform"

        # Check if cluster type exists by name or slug
        cluster_type = None

        # First try by name
        try:
            cluster_type = self.nb.virtualization.cluster_types.get(name=desired_name)
        except Exception:
            pass

        # If not found by name, try by slug
        if not cluster_type:
            try:
                cluster_type = self.nb.virtualization.cluster_types.get(slug=desired_slug)
            except Exception:
                pass

        if cluster_type:
            # Check and apply updates if needed
            updates = {}

            if getattr(cluster_type, 'name', None) != desired_name:
                updates['name'] = desired_name
            if getattr(cluster_type, 'slug', None) != desired_slug:
                updates['slug'] = desired_slug
            if getattr(cluster_type, 'description', None) != desired_description:
                updates['description'] = desired_description

            self._safe_update_object(cluster_type, updates)

            # Add sync tag
            tag_id = self.ensure_sync_tag()
            if tag_id:
                self._add_tag_to_object(cluster_type, tag_id)

            self._cluster_type_id = cluster_type.id
            return cluster_type.id

        # Create cluster type if it doesn't exist
        if self.dry_run:
            logger.info(f"[DRY-RUN] Would create cluster type: {desired_name}")
            self._cluster_type_id = 1  # Mock ID
            return 1

        # Ensure tag exists
        tag_id = self.ensure_sync_tag()

        cluster_type_data = {
            "name": desired_name,
            "slug": desired_slug,
            "description": desired_description
        }

        # Add tag if available
        if tag_id:
            cluster_type_data["tags"] = [tag_id]

        try:
            cluster_type = self.nb.virtualization.cluster_types.create(cluster_type_data)
            self._cluster_type_id = cluster_type.id
            logger.info(f"Created cluster type: {desired_name} (ID: {cluster_type.id})")
            return cluster_type.id
        except Exception as e:
            error_msg = str(e)
            # Check if it's a duplicate slug error
            if '400' in error_msg and 'slug' in error_msg.lower():
                logger.warning(f"Cluster type with slug '{desired_slug}' already exists, trying to fetch it")
                # Try one more time to get by slug
                try:
                    cluster_type = self.nb.virtualization.cluster_types.get(slug=desired_slug)
                    if cluster_type:
                        self._cluster_type_id = cluster_type.id
                        logger.info(f"Found existing cluster type: {cluster_type.name} (ID: {cluster_type.id})")
                        return cluster_type.id
                except Exception:
                    pass

                # If still not found, try listing all and finding by slug
                try:
                    all_types = list(self.nb.virtualization.cluster_types.all())
                    for ct in all_types:
                        if getattr(ct, 'slug', None) == desired_slug:
                            self._cluster_type_id = ct.id
                            logger.info(f"Found existing cluster type by iteration: {ct.name} (ID: {ct.id})")
                            return ct.id
                except Exception:
                    pass

            logger.error(f"Failed to create or find cluster type: {e}")
            raise

    def ensure_cluster(
        self,
        folder_name: str,
        folder_id: str,
        cloud_name: str,
        site_id: Optional[int] = None,
        description: str = ""
    ) -> int:
        """
        Ensure cluster exists for a Yandex Cloud folder.

        Args:
            folder_name: Folder display name
            folder_id: Folder ID
            cloud_name: Parent cloud name
            site_id: Optional site ID to assign cluster to
            description: Optional description

        Returns:
            Cluster ID
        """
        # Include cloud_name to avoid collisions across clouds
        if cloud_name:
            cluster_name = f"{cloud_name}/{folder_name}"
        else:
            cluster_name = f"{folder_name}"

        # Generate a slug from the cluster name
        cluster_slug = cluster_name.lower().replace("/", "-").replace(" ", "-").replace("_", "-")
        # Ensure slug is valid (alphanumeric and hyphens only)
        cluster_slug = re.sub(r'[^a-z0-9-]', '-', cluster_slug)
        cluster_slug = re.sub(r'-+', '-', cluster_slug)  # Replace multiple hyphens with single
        cluster_slug = cluster_slug.strip('-')  # Remove leading/trailing hyphens

        # Check if cluster exists by name or slug
        cluster = None

        # Try by name first
        try:
            cluster = self.nb.virtualization.clusters.get(name=cluster_name)
        except Exception:
            pass

        # If not found, try by slug
        if not cluster:
            try:
                # Note: clusters might not support get by slug, so we may need to iterate
                all_clusters = list(self.nb.virtualization.clusters.filter(name=cluster_name))
                if all_clusters:
                    cluster = all_clusters[0]
            except Exception:
                pass

        if cluster:
            # Check and apply updates if needed
            updates = {}

            # Check cluster type
            cluster_type_id = self.ensure_cluster_type()
            if hasattr(cluster, 'type') and cluster.type:
                current_type_id = cluster.type.id if hasattr(cluster.type, 'id') else cluster.type
                if current_type_id != cluster_type_id:
                    updates['type'] = cluster_type_id

            # Check site if provided
            if site_id is not None:
                current_site_id = None
                if hasattr(cluster, 'site') and cluster.site:
                    current_site_id = cluster.site.id if hasattr(cluster.site, 'id') else cluster.site
                if current_site_id != site_id:
                    updates['site'] = site_id

            # Check comments
            new_comments = f"Folder ID: {folder_id}\n{description}".strip()
            if getattr(cluster, 'comments', '') != new_comments:
                updates['comments'] = new_comments

            self._safe_update_object(cluster, updates)

            # Add sync tag
            tag_id = self.ensure_sync_tag()
            if tag_id:
                self._add_tag_to_object(cluster, tag_id)

            return cluster.id

        # Create cluster if it doesn't exist
        if self.dry_run:
            logger.info(f"[DRY-RUN] Would create cluster: {cluster_name}")
            return 1  # Mock ID for dry run

        # Ensure cluster type and tag exist
        cluster_type_id = self.ensure_cluster_type()
        tag_id = self.ensure_sync_tag()

        cluster_data = {
            "name": cluster_name,
            "type": cluster_type_id,
            "status": "active",
            "comments": f"Folder ID: {folder_id}\n{description}".strip()
        }

        # Optionally assign to site
        if site_id:
            cluster_data["site"] = site_id

        # Add tag if available
        if tag_id:
            cluster_data["tags"] = [tag_id]

        try:
            cluster = self.nb.virtualization.clusters.create(cluster_data)
            logger.info(f"Created cluster: {cluster_name} (ID: {cluster.id})")
            return cluster.id
        except Exception as e:
            error_msg = str(e)
            # Check if it's a duplicate name error
            if '400' in error_msg:
                logger.warning(f"Cluster '{cluster_name}' might already exist, trying to fetch it")
                # Try to get existing cluster
                try:
                    cluster = self.nb.virtualization.clusters.get(name=cluster_name)
                    if cluster:
                        logger.info(f"Found existing cluster: {cluster_name} (ID: {cluster.id})")
                        return cluster.id
                except Exception:
                    pass

                # Try listing all clusters with this name
                try:
                    all_clusters = list(self.nb.virtualization.clusters.filter(name=cluster_name))
                    if all_clusters:
                        cluster = all_clusters[0]
                        logger.info(f"Found existing cluster by filter: {cluster_name} (ID: {cluster.id})")
                        return cluster.id
                except Exception:
                    pass

            logger.error(f"Failed to create or find cluster {cluster_name}: {e}")
            raise

    def ensure_platform(self, slug: str, name: str = "") -> int:
        """
        Ensure a platform exists by slug, creating it if necessary.

        Args:
            slug: Platform slug (e.g., 'windows-2022', 'ubuntu-24-04')
            name: Display name; defaults to slug if not provided

        Returns:
            Platform ID
        """
        if not name:
            name = slug

        try:
            platform = self.nb.dcim.platforms.get(slug=slug)
            if platform:
                return platform.id
        except Exception:
            pass

        if self.dry_run:
            logger.info(f"[DRY-RUN] Would create platform: {name} (slug: {slug})")
            return 1

        try:
            platform = self.nb.dcim.platforms.create({
                "name": name,
                "slug": slug,
            })
            logger.info(f"Created platform: {name} (ID: {platform.id})")
            return platform.id
        except Exception as e:
            error_msg = str(e)
            if '400' in error_msg:
                try:
                    platform = self.nb.dcim.platforms.get(slug=slug)
                    if platform:
                        return platform.id
                except Exception:
                    pass
            logger.error(f"Failed to create or find platform {slug}: {e}")
            raise

    def ensure_prefix(
        self,
        prefix: str,
        vpc_name: str,
        site_id: Optional[int] = None,
        description: str = ""
    ) -> Optional[Record]:
        """
        Ensure IP prefix exists in NetBox.

        Args:
            prefix: CIDR prefix (e.g., "10.0.0.0/24")
            vpc_name: VPC name for description
            site_id: Optional Site ID (can be None)
            description: Optional description

        Returns:
            Prefix object or None
        """
        # Validate site_id - treat 0 as None
        if site_id == 0:
            site_id = None

        if site_id is None:
            logger.debug(f"No site_id provided for prefix {prefix}, will create without scope assignment")

        # Check if prefix exists
        try:
            existing = self.nb.ipam.prefixes.get(prefix=prefix)
        except Exception as e:
            logger.error(f"Failed to check existing prefix {prefix}: {e}")
            return None

        if existing:
            # Try to update scope if different and site_id is provided
            if site_id is not None and not self.dry_run:
                try:
                    # Check current scope (NetBox 4.2+ uses scope instead of site)
                    current_site_id = None
                    try:
                        # NetBox 4.2+ uses scope_type and scope_id
                        if hasattr(existing, 'scope_type') and hasattr(existing, 'scope_id'):
                            if existing.scope_type == "dcim.site" and existing.scope_id:
                                current_site_id = existing.scope_id
                        # Fallback for older NetBox versions
                        elif hasattr(existing, 'site'):
                            site_obj = getattr(existing, 'site', None)
                            if site_obj:
                                if hasattr(site_obj, 'id'):
                                    current_site_id = site_obj.id
                                elif isinstance(site_obj, dict):
                                    current_site_id = site_obj.get('id')
                                elif isinstance(site_obj, (int, str)):
                                    current_site_id = site_obj
                    except (AttributeError, TypeError) as e:
                        logger.debug(f"Could not get current scope/site for prefix {prefix}: {e}")

                    # Only update if site is different
                    if current_site_id != site_id:
                        # Update the scope using the appropriate fields
                        try:
                            # For NetBox 4.2+, use scope_type and scope_id
                            update_data = {
                                "scope_type": "dcim.site",
                                "scope_id": site_id
                            }
                            success = self.update_prefix(existing.id, update_data)
                            if success:
                                logger.info(
                                    f"Updated prefix {prefix} scope assignment "
                                    f"from site {current_site_id} to {site_id}"
                                )
                            else:
                                # Try fallback method for older NetBox versions
                                fallback_data = {"site": site_id}
                                success = self.update_prefix(existing.id, fallback_data)
                                if success:
                                    logger.info(
                                        f"Updated prefix {prefix} site assignment "
                                        f"from {current_site_id} to {site_id} (using legacy field)"
                                    )
                                else:
                                    logger.error(
                                        f"Cannot update prefix {prefix} scope/site assignment. "
                                        f"Check NetBox version compatibility and API token permissions."
                                    )
                        except Exception as e:
                            logger.warning(f"Failed to update prefix {prefix} scope/site: {e}")
                except Exception as e:
                    logger.debug(f"Error checking/updating scope for prefix {prefix}: {e}")

            # Add sync tag to existing prefix
            tag_id = self.ensure_sync_tag()
            if tag_id:
                self._add_tag_to_object(existing, tag_id)

            logger.debug(f"Using existing prefix {prefix}")
            return existing

        # Create prefix if it doesn't exist
        if self.dry_run:
            logger.info(f"[DRY-RUN] Would create prefix: {prefix}")
            return None

        # Ensure tag exists
        tag_id = self.ensure_sync_tag()

        try:
            # Build creation data
            prefix_data = {
                "prefix": prefix,
                "status": "active",
                "description": f"VPC: {vpc_name}\n{description}".strip()
            }

            # Only add scope if provided and valid
            # NetBox 4.2+ uses scope_type and scope_id instead of site
            if site_id is not None:
                # Try NetBox 4.2+ format first
                prefix_data["scope_type"] = "dcim.site"
                prefix_data["scope_id"] = site_id

            # Add tag if available
            if tag_id:
                prefix_data["tags"] = [tag_id]

            try:
                prefix_obj = self.nb.ipam.prefixes.create(prefix_data)
                site_msg = f" in site {site_id}" if site_id is not None else " (no site)"
                logger.info(f"Created prefix: {prefix}" + site_msg)
                return prefix_obj
            except Exception as e:
                # If scope_type/scope_id failed, try with legacy site field
                if site_id is not None and "scope" in str(e).lower():
                    logger.debug("Scope fields not supported, trying legacy site field")
                    prefix_data = {
                        "prefix": prefix,
                        "status": "active",
                        "description": f"VPC: {vpc_name}\n{description}".strip(),
                        "site": site_id
                    }
                    if tag_id:
                        prefix_data["tags"] = [tag_id]

                    try:
                        prefix_obj = self.nb.ipam.prefixes.create(prefix_data)
                        logger.info(f"Created prefix: {prefix} in site {site_id} (using legacy field)")
                        return prefix_obj
                    except Exception as e2:
                        logger.warning(f"Failed to create prefix {prefix}: {e2}")
                        return None
                else:
                    logger.warning(f"Failed to create prefix {prefix}: {e}")
                    return None
        except Exception as e:
            logger.warning(f"Failed to create prefix {prefix}: {e}")
            return None

    def update_prefix(self, prefix_id: int, updates: Dict[str, Any]) -> bool:
        """
        Update a prefix in NetBox.

        Args:
            prefix_id: Prefix ID to update
            updates: Dictionary of fields to update (e.g., {"site": site_id})

        Returns:
            True if successful, False otherwise

        Note:
            Requires 'ipam.change_prefix' permission on the NetBox API token.
            Without this permission, all update attempts will fail with 403 Forbidden.
            NetBox 4.2+ uses scope_type/scope_id instead of site field for prefixes.
        """
        if self.dry_run:
            logger.info(f"[DRY-RUN] Would update prefix {prefix_id} with: {updates}")
            return True

        try:
            # Get a fresh copy of the prefix object
            prefix_obj = self.nb.ipam.prefixes.get(id=prefix_id)
            if not prefix_obj:
                logger.error(f"Prefix with ID {prefix_id} not found")
                return False

            # Log the current state for debugging
            logger.debug(f"Current prefix state: {dict(prefix_obj)}")
            logger.debug(f"Attempting to apply updates: {updates}")

            # Update fields on the object
            for key, value in updates.items():
                # Handle scope fields for NetBox 4.2+ compatibility
                if key in ["scope_type", "scope_id"]:
                    setattr(prefix_obj, key, value)
                # For legacy site field, handle None values properly
                elif key == "site" and value is None:
                    # Clear the site assignment
                    if hasattr(prefix_obj, 'site'):
                        prefix_obj.site = None
                else:
                    setattr(prefix_obj, key, value)

            # Save changes using pynetbox
            try:
                result = prefix_obj.save()
                if result:
                    logger.info(f"Successfully updated prefix {prefix_id} with changes: {updates}")
                    return True
                else:
                    logger.warning(
                        f"Prefix save() returned False for {prefix_id}. "
                        f"This typically means the API token lacks 'ipam.change_prefix' permission."
                    )
            except Exception as save_error:
                # Check if it's a permission error
                error_str = str(save_error).lower()
                if "403" in error_str or "forbidden" in error_str or "permission" in error_str:
                    logger.error(
                        f"Permission denied when updating prefix {prefix_id}. "
                        f"The NetBox API token needs 'ipam.change_prefix' permission. Error: {save_error}"
                    )
                else:
                    logger.debug(f"Save failed, trying alternative method: {save_error}")

            # Alternative method: Use the update() method if available
            try:
                if hasattr(prefix_obj, 'update'):
                    prefix_obj.update(updates)
                    logger.info(f"Successfully updated prefix {prefix_id} using update() method")
                    return True
            except Exception as update_error:
                logger.debug(f"Update method failed: {update_error}")

            # Last resort: Use direct API call
            try:
                # Construct the URL properly
                base_url = str(self.nb.base_url).rstrip('/')
                # Check if base_url already has /api, if not add it
                if not base_url.endswith('/api'):
                    base_url = f"{base_url}/api"

                url = f"{base_url}/ipam/prefixes/{prefix_id}/"

                # Use PATCH to update only specified fields
                headers = {"Authorization": f"Token {self.nb.token}"}
                response = self.nb.http_session.patch(url, json=updates, headers=headers)
                response.raise_for_status()

                logger.info(f"Successfully updated prefix {prefix_id} using direct API")
                return True
            except Exception as api_error:
                error_str = str(api_error).lower()
                if "403" in error_str or "forbidden" in error_str:
                    logger.error(
                        f"HTTP 403 Forbidden when updating prefix {prefix_id}. "
                        f"The NetBox API token must have 'ipam.change_prefix' permission. "
                        f"Current token may only have 'ipam.add_prefix' which is insufficient for updates."
                    )
                    logger.info(
                        "To fix this issue:\n"
                        "1. Log into NetBox as an admin\n"
                        "2. Navigate to Admin -> API Tokens\n"
                        "3. Find your token and edit it\n"
                        "4. Add 'ipam | prefix | Can change prefix' permission\n"
                        "5. Save and retry the operation"
                    )
                else:
                    logger.error(f"All update methods failed for prefix {prefix_id}: {api_error}")
                return False

        except Exception as e:
            logger.error(f"Failed to update prefix {prefix_id}: {e}")
            return False

    def fetch_vms(self) -> List[Record]:
        """
        Fetch all VMs from NetBox.

        Returns:
            list of VM objects
        """
        try:
            vms = list(self.nb.virtualization.virtual_machines.all())
            logger.info(f"Fetched {len(vms)} VMs from NetBox")
            return vms
        except Exception as e:
            logger.error(f"Failed to fetch VMs: {e}")
            return []

    def create_vm(self, vm_data: Dict[str, Any]) -> Optional[Record]:
        """
        Create VM in NetBox.

        Args:
            vm_data: VM data dictionary with name, cluster, vcpus, memory, status
                     Note: disk field should not be set directly, it's calculated from virtual disks

        Returns:
            Created VM object or None
        """
        if self.dry_run:
            logger.info(f"[DRY-RUN] Would create VM: {vm_data.get('name')}")

            # Return mock object for dry run
            class MockVM:
                def __init__(self):
                    self.id = 1
                    self.name = vm_data.get('name')
                    self.site = None
                    self.cluster = None
            return MockVM()

        # Ensure tag exists
        tag_id = self.ensure_sync_tag()

        # Add tag to VM data if available
        if tag_id:
            vm_data["tags"] = [tag_id]

        # Remove disk field if present (it should be calculated from virtual disks)
        if "disk" in vm_data:
            logger.debug(
                f"Removing disk field from VM data for {vm_data.get('name')} "
                f"- will be calculated from virtual disks"
            )
            vm_data.pop("disk")

        try:
            vm = self.nb.virtualization.virtual_machines.create(vm_data)
            logger.info(f"Created VM: {vm.name} (ID: {vm.id})")
            return vm
        except Exception as e:
            logger.error(f"Failed to create VM {vm_data.get('name')}: {e}")
            return None

    def create_disk(self, disk_data: Dict[str, Any]) -> Optional[Record]:
        """
        Create virtual disk in NetBox.

        Args:
            disk_data: Disk data with virtual_machine, size, name

        Returns:
            Created disk object or None
        """
        if self.dry_run:
            logger.info(f"[DRY-RUN] Would create disk: {disk_data.get('name')}")
            return None

        try:
            # NetBox 3.x uses virtual-disks endpoint
            if hasattr(self.nb.virtualization, 'virtual_disks'):
                disk = self.nb.virtualization.virtual_disks.create(disk_data)
                logger.debug(f"Created disk: {disk.name}")
                return disk
            else:
                logger.debug("Virtual disks not supported in this NetBox version")
                return None
        except Exception as e:
            logger.error(f"Failed to create disk: {e}")
            return None

    def create_interface(self, interface_data: Dict[str, Any]) -> Optional[Record]:
        """
        Create VM interface in NetBox.

        Args:
            interface_data: Interface data with virtual_machine, name

        Returns:
            Created interface object or None
        """
        if self.dry_run:
            logger.info(f"[DRY-RUN] Would create interface: {interface_data.get('name')}")

            # Return mock object for dry run
            class MockInterface:
                def __init__(self):
                    self.id = 1
                    self.name = interface_data.get('name')
                    self.virtual_machine = interface_data.get('virtual_machine')
            return MockInterface()

        try:
            # Set default type if not provided
            if 'type' not in interface_data:
                interface_data['type'] = 'virtual'

            interface = self.nb.virtualization.interfaces.create(interface_data)
            logger.debug(f"Created interface: {interface.name}")
            return interface
        except Exception as e:
            logger.error(f"Failed to create interface: {e}")
            return None

    def create_ip(self, ip_data: Dict[str, Any]) -> Optional[Record]:
        """
        Create IP address in NetBox.

        Args:
            ip_data: IP data with address, interface

        Returns:
            Created IP object or None
        """
        if self.dry_run:
            logger.info(f"[DRY-RUN] Would create IP: {ip_data.get('address')}")
            return None

        try:
            # Ensure address has CIDR notation
            address = ip_data['address']
            if '/' not in address:
                address = f"{address}/32"
                ip_data['address'] = address

            # Get the base IP without mask for searching
            base_ip = address.split('/')[0]

            # Search for IPs matching the base address (regardless of mask)
            existing_ips = list(self.nb.ipam.ip_addresses.filter(
                address__ic=base_ip  # Case-insensitive contains search
            ))

            # Find exact IP match (same address, any mask)
            existing_ip = None
            for ip in existing_ips:
                if ip.address.split('/')[0] == base_ip:
                    existing_ip = ip
                    break

            if existing_ip:
                # Update interface assignment if different
                try:
                    current_interface_id = getattr(existing_ip, 'assigned_object_id', None)
                    # Handle both old and new data formats
                    if 'interface' in ip_data:
                        new_interface_id = ip_data['interface']
                        new_object_type = 'virtualization.vminterface'
                    else:
                        new_interface_id = ip_data.get('assigned_object_id')
                        new_object_type = ip_data.get('assigned_object_type', 'virtualization.vminterface')

                    if current_interface_id != new_interface_id and new_interface_id:
                        if not self.dry_run:
                            existing_ip.assigned_object_type = new_object_type
                            existing_ip.assigned_object_id = new_interface_id
                            existing_ip.save()
                            logger.debug(f"Updated existing IP: {base_ip} (as {existing_ip.address})")
                except Exception as e:
                    logger.debug(f"Could not update IP assignment for {address}: {e}")
                return existing_ip

            # Create new IP - handle both old and new data formats
            create_data = {
                "address": address,
                "status": ip_data.get("status", "active")
            }

            # Handle old format with 'interface' key
            if 'interface' in ip_data:
                create_data["assigned_object_type"] = "virtualization.vminterface"
                create_data["assigned_object_id"] = ip_data['interface']
            # Handle new format with direct assignment fields
            elif 'assigned_object_id' in ip_data:
                create_data["assigned_object_type"] = ip_data.get('assigned_object_type', 'virtualization.vminterface')
                create_data["assigned_object_id"] = ip_data['assigned_object_id']

            # Add description if provided
            if 'description' in ip_data:
                create_data["description"] = ip_data['description']

            ip_obj = self.nb.ipam.ip_addresses.create(create_data)
            logger.debug(f"Created IP: {ip_obj.address}")
            return ip_obj
        except Exception as e:
            logger.error(f"Failed to create IP {ip_data.get('address')}: {e}")
            return None

    def update_vm(self, vm_id: int, updates: Dict[str, Any]) -> bool:
        """
        Update VM in NetBox.

        Args:
            vm_id: VM ID
            updates: Dictionary of fields to update

        Returns:
            True if successful, False otherwise
        """
        if self.dry_run:
            logger.info(f"[DRY-RUN] Would update VM {vm_id}: {updates}")
            return True

        try:
            vm = self.nb.virtualization.virtual_machines.get(id=vm_id)
            if not vm:
                logger.error(f"VM with ID {vm_id} not found")
                return False

            # Remove disk field from updates if present (it should be calculated from virtual disks)
            if "disk" in updates:
                logger.debug(f"Removing disk field from updates for VM {vm_id} - will be calculated from virtual disks")
                updates.pop("disk")

            for key, value in updates.items():
                setattr(vm, key, value)

            vm.save()

            # Add sync tag
            tag_id = self.ensure_sync_tag()
            if tag_id:
                self._add_tag_to_object(vm, tag_id)

            logger.info(f"Updated VM {vm.name}")
            return True
        except Exception as e:
            logger.error(f"Failed to update VM {vm_id}: {e}")
            return False

    def set_vm_primary_ip(self, vm_id: int, ip_id: int, ip_version: int = 4) -> bool:
        """
        Set primary IP address for a VM.

        Args:
            vm_id: VM ID
            ip_id: IP address ID to set as primary
            ip_version: IP version (4 or 6), defaults to 4

        Returns:
            True if successful, False otherwise
        """
        if self.dry_run:
            logger.info(f"[DRY-RUN] Would set primary IPv{ip_version} (ID: {ip_id}) for VM {vm_id}")
            return True

        try:
            vm = self.nb.virtualization.virtual_machines.get(id=vm_id)
            if not vm:
                logger.error(f"VM with ID {vm_id} not found")
                return False

            # Get the IP address object
            ip = self.nb.ipam.ip_addresses.get(id=ip_id)
            if not ip:
                logger.error(f"IP address with ID {ip_id} not found")
                return False

            # Check if IP is assigned to one of this VM's interfaces
            vm_interfaces = list(self.nb.virtualization.interfaces.filter(virtual_machine_id=vm_id))
            ip_assigned_to_vm = False

            if hasattr(ip, 'assigned_object_id') and ip.assigned_object_id:
                # Check if it's assigned to one of this VM's interfaces
                for iface in vm_interfaces:
                    if ip.assigned_object_id == iface.id:
                        ip_assigned_to_vm = True
                        break

            # If not assigned to this VM, assign it to the first interface
            if not ip_assigned_to_vm:
                if not vm_interfaces:
                    logger.error(f"VM {vm.name} has no interfaces to assign IP to")
                    return False

                logger.info(f"Assigning IP {ip.address} to VM {vm.name}'s first interface before setting as primary")
                ip.assigned_object_type = "virtualization.vminterface"
                ip.assigned_object_id = vm_interfaces[0].id
                ip.save()

            # Set primary IP based on version
            if ip_version == 4:
                vm.primary_ip4 = ip_id
            elif ip_version == 6:
                vm.primary_ip6 = ip_id
            else:
                logger.error(f"Invalid IP version: {ip_version}")
                return False

            vm.save()
            logger.info(f"Set primary IPv{ip_version} {ip.address} (ID: {ip_id}) for VM {vm.name}")
            return True
        except Exception as e:
            logger.error(f"Failed to set primary IPv{ip_version} for VM {vm_id}: {e}")
            return False

    def get_vm_by_name(self, name: str) -> Optional[Record]:
        """
        Get VM by name.

        Args:
            name: VM name

        Returns:
            VM object or None
        """
        try:
            vm = self.nb.virtualization.virtual_machines.get(name=name)
            return vm
        except Exception as e:
            logger.error(f"Failed to get VM {name}: {e}")
            return None

    def get_vm_by_custom_field(self, field_name: str, field_value: str) -> Optional[Record]:
        """
        Get VM by custom field value (e.g., yc_id).

        Args:
            field_name: Custom field name
            field_value: Custom field value

        Returns:
            VM object or None
        """
        try:
            vms = self.nb.virtualization.virtual_machines.filter(**{f"cf_{field_name}": field_value})
            if vms:
                return vms[0]
            return None
        except Exception as e:
            logger.error(f"Failed to get VM by {field_name}={field_value}: {e}")
            return None

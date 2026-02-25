# Fix sync errors: clusters, phantom VM updates, primary IP instability

## Overview
- Dry-run выявил 3 системных проблемы синхронизации, которые вместе приводят к некорректному состоянию NetBox
- **Баг 1**: `ensure_cluster` ищет кластеры по новому формату `"cloud/folder"`, но в NetBox они хранятся как `"folder"` — все 43 кластера создаются заново (дубли)
- **Баг 2**: Все 289 VM помечаются как "to update" при каждом запуске — в dry-run из-за mock cluster ID=1, в реальном запуске из-за дубликатов кластеров и нестабильного comments
- **Баг 3**: 37 primary IP changes и 4 "switching" warnings — нестабильный выбор primary IP для multi-interface VM (частично исправлено в текущем diff)
- Миграция кластеров на новый формат имён с prefix `"cloud/"` для поддержки multi-cloud

## Context (from discovery)
- Файлы:
  - `src/netbox_sync/clients/netbox.py` — `ensure_cluster()` (строки 393-532): lookup по новому имени, нет fallback по старому
  - `src/netbox_sync/sync/batch.py` — `process_vm_updates()` (строки 94-391): comments comparison, cluster ID, primary IP logic
  - `src/netbox_sync/sync/infrastructure.py` — вызывает `ensure_cluster()`, строит id_mapping
- Тесты: `tests/sync/test_batch.py` (64 теста), `tests/clients/test_netbox.py`
- Текущий diff в batch.py — фиксы primary IP из предыдущего плана (pending public IP candidate)
- Данные dry-run: 289 VMs YC, 330 VMs NetBox, 43 folders, 5 zones, 49 subnets
- Orphaned clusters показывают имена БЕЗ prefix: `prod-dmz`, `tir-agora`, `tir-elma`

## Development Approach
- **Testing approach**: Regular (code first, then tests)
- Complete each task fully before moving to the next
- Make small, focused changes
- **CRITICAL: every task MUST include new/updated tests** for code changes in that task
- **CRITICAL: all tests must pass before starting next task** - no exceptions
- **CRITICAL: update this plan file when scope changes during implementation**
- Run tests after each change
- Maintain backward compatibility

## Testing Strategy
- **Unit tests**: required for every task
- No E2E/UI tests in this project
- Test command: `python3 -m pytest tests/ -v`

## Progress Tracking
- Mark completed items with `[x]` immediately when done
- Add newly discovered tasks with ➕ prefix
- Document issues/blockers with ⚠️ prefix

## Implementation Steps

### Task 1: Fix cluster lookup — add fallback by old name format

**Files:**
- Modify: `src/netbox_sync/clients/netbox.py`
- Modify: `tests/clients/test_netbox.py`

**Problem:** `ensure_cluster("prod-devops", folder_id, "grand-trade")` generates name `"grand-trade/prod-devops"` and searches by this name. Existing cluster named `"prod-devops"` is not found → creates duplicate.

**Fix:** Add fallback lookup by `folder_name` (without cloud prefix). When found, rename cluster to new format.

- [ ] In `ensure_cluster()`, after failed lookup by `cluster_name` ("cloud/folder"), add fallback lookup by `folder_name` only
- [ ] When cluster found by old name: rename it to new format (`cluster.name = cluster_name`, `cluster.slug = cluster_slug`, then `cluster.save()`)
- [ ] Log migration: `"Migrated cluster '{old_name}' → '{new_name}'"`
- [ ] Also add fallback lookup by `cluster_slug` (similar to how `ensure_site` does it) for robustness
- [ ] Write tests: cluster found by new name (no migration)
- [ ] Write tests: cluster found by old name → renamed to new format
- [ ] Write tests: cluster not found at all → created with new name
- [ ] Write tests: dry-run mode — cluster found by old name, no rename happens, returns real ID
- [ ] Run tests — must pass before next task

### Task 2: Eliminate phantom VM updates — normalize comments comparison

**Files:**
- Modify: `src/netbox_sync/sync/batch.py`
- Modify: `tests/sync/test_batch.py`

**Problem:** `process_vm_updates` compares `vm.comments != new_comments` but comments can differ due to whitespace, None vs empty string, trailing newlines. Every VM gets queued for update unnecessarily.

Additionally, in dry-run mode cluster IDs are always mock=1, causing every VM to show a cluster mismatch.

**Fix:** Normalize comments before comparison. Improve dry-run awareness for cluster comparison.

- [ ] Normalize `vm.comments` and `new_comments` before comparison: strip whitespace, handle None/empty
- [ ] Add helper function `_normalize_comments(text: str) -> str` that strips, normalizes newlines, and handles None
- [ ] In cluster comparison: skip cluster update when `cluster_id` is mock (1) in dry-run — or better, only update when actual IDs differ AND both are > 0
- [ ] Write tests: comments match after normalization → no update queued
- [ ] Write tests: comments differ → update queued
- [ ] Write tests: comments None vs empty string → no update
- [ ] Write tests: cluster mock ID (dry-run) → no cluster update queued
- [ ] Run tests — must pass before next task

### Task 3: Stabilize primary IP selection for multi-interface VMs

**Files:**
- Modify: `src/netbox_sync/sync/batch.py`
- Modify: `tests/sync/test_batch.py`

**Problem:** VMs with multiple interfaces (firewalls like fortigate1/2, yc-fw-fgt-dmz) switch primary IP between interfaces on each sync. The first private IP found on any interface becomes the candidate, but interface ordering may not be stable.

**Fix:** Prefer keeping the current primary IP if it's still valid (assigned to the VM and private).

- [ ] Before selecting a new primary IP candidate, check if `vm.primary_ip4` is still valid: exists in cache, is private, is assigned to one of the VM's interfaces
- [ ] If current primary IP is still valid: skip primary IP selection entirely (no change needed)
- [ ] Only trigger primary IP change when: current primary IP is gone, reassigned to another VM, or VM has no primary IP
- [ ] Add log: `"VM {name}: keeping current primary IP {addr} (still valid)"` at DEBUG level
- [ ] Write tests: VM has valid primary IP → no change queued
- [ ] Write tests: VM's current primary IP reassigned to another VM → new primary selected
- [ ] Write tests: VM's current primary IP no longer exists → new primary selected
- [ ] Write tests: VM with no primary IP → first private IP selected as primary
- [ ] Run tests — must pass before next task

### Task 4: Improve dry-run reporting accuracy

**Files:**
- Modify: `src/netbox_sync/sync/batch.py`

**Problem:** Dry-run reports "VMs to update: 289" which is misleading — most VMs don't actually need changes. After Tasks 1-3, the count should drop dramatically, but dry-run should also break down what's changing.

- [ ] In dry-run summary, add breakdown: how many VMs have comments changes, cluster changes, status changes, etc.
- [ ] Log per-VM details at DEBUG level showing what fields would change
- [ ] Write tests: dry-run summary shows correct breakdown of update reasons
- [ ] Run tests — must pass before next task

### Task 5: Verify acceptance criteria

- [ ] Run full test suite: `python3 -m pytest tests/ -v`
- [ ] Run dry-run: `python3 -m netbox_sync --dry-run` — verify:
  - Clusters found (not "Would create" for all 43)
  - VMs to update count significantly reduced (should be near 0 if nothing changed)
  - Primary IP changes count reduced (only real changes, not phantom switches)
- [ ] Run linter: `ruff check src/ tests/`

### Task 6: [Final] Update documentation

- [ ] Update CLAUDE.md if new patterns discovered
- [ ] Move this plan to `docs/plans/completed/`

## Technical Details

### Cluster naming migration flow
```
ensure_cluster("prod-devops", folder_id="b1gn93...", cloud_name="grand-trade")
│
├── Generate: cluster_name = "grand-trade/prod-devops"
├── Lookup 1: nb.clusters.get(name="grand-trade/prod-devops") → None
├── Lookup 2: nb.clusters.filter(name="grand-trade/prod-devops") → []
├── Fallback 3: nb.clusters.get(name="prod-devops") → Found!  ← NEW
│   ├── Rename: cluster.name = "grand-trade/prod-devops"
│   ├── Rename: cluster.slug = "grand-trade-prod-devops"
│   ├── cluster.save()
│   └── Log: "Migrated cluster 'prod-devops' → 'grand-trade/prod-devops'"
└── Return: cluster.id (real ID, not mock)
```

### Comments normalization
```python
def _normalize_comments(text: Optional[str]) -> str:
    if not text:
        return ""
    return "\n".join(line.strip() for line in text.strip().splitlines())
```

### Primary IP stability — "keep if valid" check
```python
# Before selecting new primary candidate:
if vm.primary_ip4:
    current_ip = cache.ips.get(vm.primary_ip4.id)
    if current_ip:
        current_ip_str = get_ip_without_cidr(current_ip.address)
        # Check: is it private AND assigned to one of this VM's interfaces?
        if is_private_ip(current_ip_str):
            for iface in existing_interfaces:
                if current_ip.assigned_object_id == iface.id:
                    # Current primary is valid — skip re-selection
                    return changes_made
```

### Dry-run breakdown format
```
[DRY-RUN] Would apply the following updates:
  VMs to update: 5
    - comments: 3
    - status: 1
    - cluster: 0
    - memory/cpu: 1
  IPs to update: 2
  Primary IP changes: 2
  ...
```

## Post-Completion
*Items requiring manual intervention or external systems*

**Manual verification**:
- Run `netbox-sync --dry-run` and verify cluster migration messages
- Run full sync and check NetBox UI for correctly named clusters
- Verify multi-interface VMs (fortigate1, fortigate2, yc-fw-fgt-dmz, yb-skdpu-gw-1p) have stable primary IPs across 2+ consecutive syncs
- Confirm orphaned cluster cleanup still works correctly with new naming

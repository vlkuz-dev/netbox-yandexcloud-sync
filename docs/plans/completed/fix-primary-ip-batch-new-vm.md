# Fix primary IP not set for new VMs in batch sync mode

## Overview
- When new VMs are synced via the default batch mode (`sync_vms_optimized`), IP addresses are correctly created and assigned to interfaces, but the VM's `primary_ip4` field is never set
- This means VMs in NetBox have no primary IP even though their interfaces have IPs
- Root cause appears to be in the `process_vm_updates` → `apply_batch_updates` flow for newly created VMs, combined with missing test coverage for this specific end-to-end scenario

## Context (from discovery)
- Files/components involved:
  - `src/netbox_sync/sync/batch.py` - batch sync logic (`process_vm_updates`, `apply_batch_updates`, `sync_vms_optimized`)
  - `src/netbox_sync/clients/netbox.py` - `create_ip()`, `set_vm_primary_ip()`
  - `tests/sync/test_batch.py` - batch sync tests (57 tests, all passing)
- Key finding: `test_new_vm_created` uses empty `network_interfaces: []` — never tests IP/primary flow
- The "pending" mechanism in batch mode handles deferred IP creation for new VMs:
  1. `process_vm_updates` queues `primary_ip_changes[vm_id] = "pending"`
  2. `apply_batch_updates` Step 5 creates IPs, then resolves pending → actual IP ID
  3. Step 8 sets `vm.primary_ip4 = ip_id`
- Known sub-issues:
  - `public_ip_candidate` is never set for newly created IPs (only for existing), so VMs with only public IPs can never get primary set
  - No diagnostic logging when Step 8 succeeds/fails for pending resolutions
  - No end-to-end test covers new VM → interfaces → IPs → primary IP assignment in batch mode

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
- **Unit tests**: required for every task (see Development Approach above)
- No E2E/UI tests in this project

## Progress Tracking
- Mark completed items with `[x]` immediately when done
- Add newly discovered tasks with ➕ prefix
- Document issues/blockers with ⚠️ prefix
- Update plan if implementation deviates from original scope

## Implementation Steps

### Task 1: Add diagnostic logging to batch primary IP flow
- [x] Add INFO-level log in `batch.py` `apply_batch_updates` Step 8 showing: VM name, IP ID, and whether resolution was from "pending" or direct
- [x] Add WARNING-level log when `primary_ip_changes` has "pending" that was NOT resolved (currently only in lines 505-511, ensure it includes VM name prominently)
- [x] Add DEBUG log in `process_vm_updates` showing which primary IP path was taken (private candidate, public candidate, fallback, or pending)
- [x] Write test verifying the diagnostic log messages are emitted in key scenarios (pending resolution, unresolved pending)
- [x] Run tests - must pass before next task (58 passed)

### Task 2: Fix missing public_ip_candidate for newly created IPs
- [x] In `batch.py` `process_vm_updates`, after queuing a new public primary IP for creation (line ~276), set `public_ip_candidate = "pending"` and track in `cache.pending_primary_ips` similar to private IPs
- [x] Also handle newly created NAT public IPs (line ~298): when `existing_public_ip` is None and no private candidate exists, set `public_ip_candidate = "pending"`
- [x] Update the primary IP selection logic (lines 303-368) to handle `public_ip_candidate == "pending"` correctly (queue "pending" when no private candidate and public is pending)
- [x] Write tests for new VM with only public IP as `primary_v4_address` (no private) → verify `primary_ip_changes` gets "pending"
- [x] Write tests for new VM with no `primary_v4_address` but with `primary_v4_address_one_to_one_nat` only → verify pending is queued
- [x] Run tests - 61 passed

### Task 3: Add end-to-end test for new VM batch creation with IPs
- [x] Create test `test_new_vm_created_with_interfaces_and_primary_ip` in `TestSyncVmsOptimized`
- [x] Create test `test_new_vm_primary_ip_set_for_public_only` in `TestSyncVmsOptimized`
- [x] Create test `test_multiple_new_vms_all_get_primary_ip`
- [x] Run tests - 64 passed

### Task 4: Investigate and fix root cause for private IP pending resolution
- [x] Verified pending resolution in Step 5 correctly resolves "pending" → actual IP ID (confirmed by e2e tests)
- [x] Verified CIDR normalization matching works correctly between `pending_primary_ips` and `created_ips`
- [x] Confirmed Step 7 `vm.save()` does not break Step 8 `vm.save()` — pynetbox tracks changes per-field
- [x] Root cause identified: **missing public_ip_candidate tracking for newly created IPs** (fixed in Task 2). The private IP "pending" mechanism was working correctly; the bug was that public-IP-only VMs had no candidate tracked at all
- [x] E2e tests in Task 3 confirm the fix works for both private and public IP scenarios
- [x] Diagnostic logging from Task 1 will catch any remaining edge cases in production

### Task 5: Verify acceptance criteria
- [x] Verify: new VM with private IP gets `primary_ip4` set in batch mode (test_new_vm_created_with_interfaces_and_primary_ip)
- [x] Verify: new VM with only public IP gets `primary_ip4` set in batch mode (test_new_vm_primary_ip_set_for_public_only)
- [x] Verify: multiple new VMs get primary IPs set (test_multiple_new_vms_all_get_primary_ip)
- [x] Verify: edge cases — NAT-only public IP, unresolved pending, no interfaces
- [x] Run full test suite — 283 passed
- [x] Run linter (`ruff check src/ tests/`) — all checks passed

### Task 6: [Final] Update documentation
- [x] No README changes needed (bug fix, no user-facing API changes)
- [x] No new patterns to document

*Note: ralphex automatically moves completed plans to `docs/plans/completed/`*

## Technical Details

### Batch mode flow for new VMs (current behavior)
```
sync_vms_optimized()
├── load_netbox_data()          → cache has no data for new VM
├── netbox.create_vm(vm_data)   → VM created in NetBox
├── process_vm_updates()        → queues interfaces, IPs, primary as "pending"
│   ├── All interfaces → pending_<vm_id>_eth<N>
│   ├── All IPs → ips_to_create (with pending interface refs)
│   ├── private_ip_candidate = "pending"
│   ├── public_ip_candidate = None  ← BUG: never set for new IPs
│   └── primary_ip_changes[vm_id] = "pending"
└── apply_batch_updates()
    ├── Step 3: Create interfaces → resolve pending interface IDs
    ├── Step 5: Create IPs → resolve pending primary IPs → actual IP ID
    ├── Step 7: Update VM parameters (may .save() the VM)
    └── Step 8: Set primary_ip4 → vm.primary_ip4 = ip_id; vm.save()
```

### Key data structures
- `cache.pending_primary_ips: Dict[int, str]` — VM ID → IP address string (tracks what IP should become primary)
- `cache.primary_ip_changes: Dict[int, Optional[Any]]` — VM ID → IP ID or "pending" or None
- `created_ips: Dict[str, Any]` — base IP (no CIDR) → IP record (populated during Step 5)

### Resolution matching
- `pending_primary_ips[vm_id]` stores IP with CIDR (e.g., "10.0.0.5/32")
- Resolution strips CIDR: `pending_ip.split('/')[0]` → "10.0.0.5"
- `created_ips` keyed by base IP: `ip_data['address'].split('/')[0]` → "10.0.0.5"
- Match should succeed if the IP creation succeeds

## Post-Completion
*Items requiring manual intervention or external systems - no checkboxes, informational only*

**Manual verification**:
- Run sync against a real Yandex Cloud + NetBox environment
- Create a new VM in YC, run sync, verify `primary_ip4` is set in NetBox
- Verify with `--dry-run` first, then without
- Check NetBox UI that the VM shows the correct primary IP

# NetBox Yandex Cloud Sync (v3)

Synchronize Yandex Cloud resources (VMs, networks, zones) to NetBox inventory management system.

## Features

- Synchronize VMs from Yandex Cloud to NetBox
- Map Yandex Cloud structure to NetBox hierarchy (zones -> sites, folders -> clusters)
- Automatic creation of sites, clusters, and prefixes
- Support for multiple clouds, folders, and availability zones
- Two sync modes: optimized batch (default) and standard sequential
- Dry-run mode for previewing changes
- Automatic cleanup of orphaned objects
- Automatic tagging of synced objects with `synced-from-yc`
- Docker support

## Architecture Mapping

| Yandex Cloud | NetBox | Description |
|---|---|---|
| Availability Zone | Site | Each YC zone becomes a NetBox site |
| Folder | Cluster | YC folders are mapped to NetBox clusters |
| Cloud | Cluster Group | Cloud organizations reflected in cluster naming |
| Cluster Type | `yandex-cloud` | All clusters use the unified type |
| VPC/Subnet | Prefix | Network prefixes assigned to zone sites |
| VM | Virtual Machine | VMs assigned to clusters and sites |

## Requirements

- Python 3.10+
- NetBox 3.0+
- Yandex Cloud account with OAuth token

## Installation

### From source

```bash
git clone https://github.com/vlkuz-dev/netbox-yandexcloud-sync.git
cd netbox-yandexcloud-sync
pip install .
```

### Development install

```bash
pip install -e ".[dev]"
```

### Docker

```bash
docker build -t netbox-sync .
```

## Configuration

Copy and configure environment variables:

```bash
cp .env.example .env
# Edit .env with your credentials
```

Required variables:

| Variable | Description |
|---|---|
| `YC_TOKEN` | Yandex Cloud OAuth token |
| `NETBOX_URL` | NetBox API URL (must include `/api` suffix, e.g. `https://netbox.example.com/api`) |
| `NETBOX_TOKEN` | NetBox API token with write permissions |

Optional:

| Variable | Default | Description |
|---|---|---|
| `LOG_LEVEL` | `INFO` | Logging level (DEBUG, INFO, WARNING, ERROR) |

## Usage

### CLI

```bash
# Full sync
netbox-sync

# Preview changes without applying
netbox-sync --dry-run

# Skip orphaned object cleanup
netbox-sync --no-cleanup

# Use standard (non-batch) sync mode
netbox-sync --standard

# Show version
netbox-sync --version
```

### Python module

```bash
python -m netbox_sync --help
```

### Docker

```bash
docker run --rm --env-file .env netbox-sync
docker run --rm --env-file .env netbox-sync --dry-run
```

## Project Structure

```
netbox-sync/
├── pyproject.toml
├── Dockerfile
├── .env.example
├── README.md
├── src/
│   └── netbox_sync/
│       ├── __init__.py           # Package version
│       ├── __main__.py           # python -m support
│       ├── cli.py                # CLI argument parsing
│       ├── config.py             # Configuration from env vars
│       ├── clients/
│       │   ├── yandex.py         # Yandex Cloud API client
│       │   └── netbox.py         # NetBox API wrapper
│       ├── sync/
│       │   ├── engine.py         # Top-level sync orchestrator
│       │   ├── batch.py          # Batch/optimized sync operations
│       │   ├── infrastructure.py # Sites, clusters, prefixes sync
│       │   ├── vms.py            # VM sync logic
│       │   └── cleanup.py        # Orphaned object cleanup
│       └── ip/
│           ├── classifier.py     # IP classification (private/public)
│           └── utils.py          # CIDR helpers
└── tests/
    ├── conftest.py
    ├── test_config.py
    ├── ip/
    ├── sync/
    └── clients/
```

## Development

### Running tests

```bash
pytest
```

### Debug logging

```bash
LOG_LEVEL=DEBUG netbox-sync --dry-run
```

## How It Works

1. Fetches data from Yandex Cloud API (zones, clouds, folders, subnets, VMs)
2. Creates/updates NetBox infrastructure (sites, cluster type, clusters, prefixes)
3. Syncs VMs with resources, interfaces, and IP addresses
4. Cleans up orphaned objects no longer present in Yandex Cloud

All synced objects are tagged with `synced-from-yc` for easy identification.

## License

MIT License - see [LICENSE](LICENSE) for details.

## Author

[vlkuz-dev](https://github.com/vlkuz-dev)

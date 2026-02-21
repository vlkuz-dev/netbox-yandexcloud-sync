"""CLI entry point for netbox-sync."""

import argparse
import logging
import sys

from dotenv import load_dotenv

from netbox_sync import __version__
from netbox_sync.config import Config
from netbox_sync.sync.engine import SyncEngine

logger = logging.getLogger(__name__)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command line arguments.

    Args:
        argv: Argument list to parse (defaults to sys.argv[1:]).
    """
    parser = argparse.ArgumentParser(
        prog="netbox-sync",
        description="Sync Yandex Cloud resources to NetBox",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run in dry-run mode (no changes will be made)",
    )
    parser.add_argument(
        "--no-cleanup",
        action="store_true",
        help="Skip cleanup of orphaned objects",
    )
    parser.add_argument(
        "--standard",
        action="store_true",
        help="Use standard sync instead of optimized batch operations",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    """Main entry point for netbox-sync CLI."""
    args = parse_args(argv)

    load_dotenv()

    try:
        config = Config.from_env(dry_run=args.dry_run)
    except ValueError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        sys.exit(1)

    config.setup_logging()
    logger.debug("Config: %s", config)

    engine = SyncEngine(config)

    try:
        stats = engine.run(
            use_batch=not args.standard,
            cleanup=not args.no_cleanup,
        )
        if stats:
            logger.info("Sync stats: %s", stats)
    except Exception:
        logger.exception("Sync failed")
        sys.exit(1)

"""Configuration module for Yandex Cloud to NetBox synchronization."""

import os
import logging


class Config:
    """Application configuration."""

    def __init__(
        self,
        yc_token: str,
        netbox_url: str,
        netbox_token: str,
        dry_run: bool = False,
    ):
        self.yc_token = yc_token
        self.netbox_url = netbox_url
        self.netbox_token = netbox_token
        self.dry_run = dry_run

    @classmethod
    def from_env(cls, dry_run: bool = False) -> "Config":
        """Create configuration from environment variables.

        Reads YC_TOKEN, NETBOX_URL, NETBOX_TOKEN from the environment.

        Raises:
            ValueError: If required environment variables are missing.
        """
        yc_token = os.getenv("YC_TOKEN")
        netbox_url = os.getenv("NETBOX_URL")
        netbox_token = os.getenv("NETBOX_TOKEN")

        missing = []
        if not yc_token:
            missing.append("YC_TOKEN")
        if not netbox_url:
            missing.append("NETBOX_URL")
        if not netbox_token:
            missing.append("NETBOX_TOKEN")

        if missing:
            raise ValueError(
                f"Missing required environment variables: {', '.join(missing)}\n"
                "Please set them in your environment or .env file."
            )

        return cls(
            yc_token=yc_token,
            netbox_url=netbox_url,
            netbox_token=netbox_token,
            dry_run=dry_run,
        )

    def setup_logging(self) -> None:
        """Configure logging for the application."""
        log_level = os.getenv("LOG_LEVEL", "INFO").upper()

        logging.basicConfig(
            level=getattr(logging, log_level, logging.INFO),
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        # Reduce noise from third-party libraries
        for name in ("urllib3", "requests", "httpx", "httpcore", "pynetbox"):
            logging.getLogger(name).setLevel(logging.WARNING)

    def __repr__(self) -> str:
        """Return string representation with masked tokens."""
        def _mask(value: str) -> str:
            if len(value) <= 12:
                return "***"
            return value[:4] + "***" + value[-4:]

        return (
            f"Config(netbox_url={self.netbox_url!r}, "
            f"yc_token={_mask(self.yc_token)!r}, "
            f"netbox_token={_mask(self.netbox_token)!r}, "
            f"dry_run={self.dry_run})"
        )

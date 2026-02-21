"""Tests for CLI argument parsing and main entry point."""

import subprocess
import sys
from unittest.mock import patch, MagicMock

import pytest

from netbox_sync.cli import parse_args, main


class TestParseArgs:
    """Tests for parse_args()."""

    def test_defaults(self):
        args = parse_args([])
        assert args.dry_run is False
        assert args.no_cleanup is False
        assert args.standard is False

    def test_dry_run(self):
        args = parse_args(["--dry-run"])
        assert args.dry_run is True

    def test_no_cleanup(self):
        args = parse_args(["--no-cleanup"])
        assert args.no_cleanup is True

    def test_standard(self):
        args = parse_args(["--standard"])
        assert args.standard is True

    def test_all_flags(self):
        args = parse_args(["--dry-run", "--no-cleanup", "--standard"])
        assert args.dry_run is True
        assert args.no_cleanup is True
        assert args.standard is True

    def test_version(self):
        with pytest.raises(SystemExit) as exc_info:
            parse_args(["--version"])
        assert exc_info.value.code == 0

    def test_unknown_flag_exits(self):
        with pytest.raises(SystemExit) as exc_info:
            parse_args(["--unknown"])
        assert exc_info.value.code == 2


class TestMain:
    """Tests for main() entry point."""

    @patch("netbox_sync.cli.SyncEngine")
    @patch("netbox_sync.cli.Config")
    @patch("netbox_sync.cli.load_dotenv")
    def test_main_default_flags(self, mock_dotenv, mock_config_cls, mock_engine_cls):
        mock_config = MagicMock()
        mock_config_cls.from_env.return_value = mock_config
        mock_engine = MagicMock()
        mock_engine.run.return_value = {}
        mock_engine_cls.return_value = mock_engine

        main([])

        mock_dotenv.assert_called_once()
        mock_config_cls.from_env.assert_called_once_with(dry_run=False)
        mock_config.setup_logging.assert_called_once()
        mock_engine_cls.assert_called_once_with(mock_config)
        mock_engine.run.assert_called_once_with(use_batch=True, cleanup=True)

    @patch("netbox_sync.cli.SyncEngine")
    @patch("netbox_sync.cli.Config")
    @patch("netbox_sync.cli.load_dotenv")
    def test_main_dry_run(self, mock_dotenv, mock_config_cls, mock_engine_cls):
        mock_config = MagicMock()
        mock_config_cls.from_env.return_value = mock_config
        mock_engine = MagicMock()
        mock_engine.run.return_value = {}
        mock_engine_cls.return_value = mock_engine

        main(["--dry-run"])

        mock_config_cls.from_env.assert_called_once_with(dry_run=True)

    @patch("netbox_sync.cli.SyncEngine")
    @patch("netbox_sync.cli.Config")
    @patch("netbox_sync.cli.load_dotenv")
    def test_main_standard_no_cleanup(self, mock_dotenv, mock_config_cls, mock_engine_cls):
        mock_config = MagicMock()
        mock_config_cls.from_env.return_value = mock_config
        mock_engine = MagicMock()
        mock_engine.run.return_value = {}
        mock_engine_cls.return_value = mock_engine

        main(["--standard", "--no-cleanup"])

        mock_engine.run.assert_called_once_with(use_batch=False, cleanup=False)

    @patch("netbox_sync.cli.load_dotenv")
    def test_main_missing_config_exits(self, mock_dotenv):
        with patch("netbox_sync.cli.Config") as mock_config_cls:
            mock_config_cls.from_env.side_effect = ValueError("Missing YC_TOKEN")
            with pytest.raises(SystemExit) as exc_info:
                main([])
            assert exc_info.value.code == 1

    @patch("netbox_sync.cli.SyncEngine")
    @patch("netbox_sync.cli.Config")
    @patch("netbox_sync.cli.load_dotenv")
    def test_main_sync_failure_exits(self, mock_dotenv, mock_config_cls, mock_engine_cls):
        mock_config = MagicMock()
        mock_config_cls.from_env.return_value = mock_config
        mock_engine = MagicMock()
        mock_engine.run.side_effect = RuntimeError("connection refused")
        mock_engine_cls.return_value = mock_engine

        with pytest.raises(SystemExit) as exc_info:
            main(["--dry-run"])
        assert exc_info.value.code == 1


class TestCLIIntegration:
    """Integration tests using subprocess."""

    def test_version_output(self):
        result = subprocess.run(
            [sys.executable, "-m", "netbox_sync", "--version"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert "netbox-sync 3.0.0" in result.stdout

    def test_help_output(self):
        result = subprocess.run(
            [sys.executable, "-m", "netbox_sync", "--help"],
            capture_output=True,
            text=True,
        )
        assert result.returncode == 0
        assert "--dry-run" in result.stdout
        assert "--no-cleanup" in result.stdout
        assert "--standard" in result.stdout
        assert "--version" in result.stdout

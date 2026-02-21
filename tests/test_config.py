"""Tests for netbox_sync.config module."""

import logging

import pytest

from netbox_sync.config import Config


class TestConfigInit:
    def test_stores_all_fields(self):
        cfg = Config(
            yc_token="tok-yc",
            netbox_url="https://nb.example.com",
            netbox_token="tok-nb",
            dry_run=True,
        )
        assert cfg.yc_token == "tok-yc"
        assert cfg.netbox_url == "https://nb.example.com"
        assert cfg.netbox_token == "tok-nb"
        assert cfg.dry_run is True

    def test_dry_run_defaults_false(self):
        cfg = Config(yc_token="a", netbox_url="b", netbox_token="c")
        assert cfg.dry_run is False


class TestFromEnv:
    def test_loads_all_env_vars(self, monkeypatch):
        monkeypatch.setenv("YC_TOKEN", "yc-secret")
        monkeypatch.setenv("NETBOX_URL", "https://netbox.local")
        monkeypatch.setenv("NETBOX_TOKEN", "nb-secret")

        cfg = Config.from_env()
        assert cfg.yc_token == "yc-secret"
        assert cfg.netbox_url == "https://netbox.local"
        assert cfg.netbox_token == "nb-secret"
        assert cfg.dry_run is False

    def test_passes_dry_run(self, monkeypatch):
        monkeypatch.setenv("YC_TOKEN", "a")
        monkeypatch.setenv("NETBOX_URL", "b")
        monkeypatch.setenv("NETBOX_TOKEN", "c")

        cfg = Config.from_env(dry_run=True)
        assert cfg.dry_run is True

    def test_raises_when_all_missing(self, monkeypatch):
        monkeypatch.delenv("YC_TOKEN", raising=False)
        monkeypatch.delenv("NETBOX_URL", raising=False)
        monkeypatch.delenv("NETBOX_TOKEN", raising=False)

        with pytest.raises(ValueError, match="YC_TOKEN"):
            Config.from_env()

    def test_raises_lists_all_missing(self, monkeypatch):
        monkeypatch.delenv("YC_TOKEN", raising=False)
        monkeypatch.delenv("NETBOX_URL", raising=False)
        monkeypatch.delenv("NETBOX_TOKEN", raising=False)

        with pytest.raises(ValueError) as exc_info:
            Config.from_env()

        msg = str(exc_info.value)
        assert "YC_TOKEN" in msg
        assert "NETBOX_URL" in msg
        assert "NETBOX_TOKEN" in msg

    def test_raises_when_one_missing(self, monkeypatch):
        monkeypatch.setenv("YC_TOKEN", "a")
        monkeypatch.setenv("NETBOX_URL", "b")
        monkeypatch.delenv("NETBOX_TOKEN", raising=False)

        with pytest.raises(ValueError, match="NETBOX_TOKEN"):
            Config.from_env()

    def test_empty_string_treated_as_missing(self, monkeypatch):
        monkeypatch.setenv("YC_TOKEN", "")
        monkeypatch.setenv("NETBOX_URL", "b")
        monkeypatch.setenv("NETBOX_TOKEN", "c")

        with pytest.raises(ValueError, match="YC_TOKEN"):
            Config.from_env()


class TestSetupLogging:
    def test_configures_default_info_level(self, monkeypatch):
        monkeypatch.delenv("LOG_LEVEL", raising=False)
        cfg = Config(yc_token="a", netbox_url="b", netbox_token="c")

        # Reset root logger handlers to test fresh setup
        root = logging.getLogger()
        root.handlers.clear()

        cfg.setup_logging()
        assert root.level == logging.INFO

    def test_configures_debug_level(self, monkeypatch):
        monkeypatch.setenv("LOG_LEVEL", "debug")
        cfg = Config(yc_token="a", netbox_url="b", netbox_token="c")

        root = logging.getLogger()
        root.handlers.clear()

        cfg.setup_logging()
        assert root.level == logging.DEBUG

    def test_silences_third_party_loggers(self, monkeypatch):
        monkeypatch.delenv("LOG_LEVEL", raising=False)
        cfg = Config(yc_token="a", netbox_url="b", netbox_token="c")

        root = logging.getLogger()
        root.handlers.clear()

        cfg.setup_logging()
        for name in ("httpx", "httpcore", "pynetbox", "urllib3", "requests"):
            assert logging.getLogger(name).level == logging.WARNING


class TestRepr:
    def test_masks_long_tokens(self):
        cfg = Config(
            yc_token="abcdefghijklmnop",
            netbox_url="https://nb.example.com",
            netbox_token="1234567890abcdef",
        )
        r = repr(cfg)
        assert "abcdefghijklmnop" not in r
        assert "1234567890abcdef" not in r
        assert "abcd***mnop" in r
        assert "1234***cdef" in r
        assert "https://nb.example.com" in r
        assert "dry_run=False" in r

    def test_masks_short_tokens(self):
        cfg = Config(
            yc_token="short",
            netbox_url="https://nb.example.com",
            netbox_token="tiny",
        )
        r = repr(cfg)
        assert "short" not in r
        assert "tiny" not in r
        assert "***" in r

    def test_shows_dry_run_true(self):
        cfg = Config(
            yc_token="abcdefghijklmnop",
            netbox_url="https://nb.example.com",
            netbox_token="1234567890abcdef",
            dry_run=True,
        )
        assert "dry_run=True" in repr(cfg)

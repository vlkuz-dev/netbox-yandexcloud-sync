import netbox_sync


def test_version():
    assert netbox_sync.__version__ == "3.0.0"


def test_version_is_string():
    assert isinstance(netbox_sync.__version__, str)


def test_subpackages_importable():
    from netbox_sync import clients  # noqa: F401
    from netbox_sync import ip  # noqa: F401
    from netbox_sync import sync  # noqa: F401

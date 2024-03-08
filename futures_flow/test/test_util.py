from futures_flow.core.util import get_root_directory


def test_getRootDir():
    assert isinstance(get_root_directory(), str)

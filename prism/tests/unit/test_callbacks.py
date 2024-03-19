from pathlib import Path
import pytest
import os

# Prism imports
from prism.callbacks import _PrismCallback


# Example callback
def example_callback():
    with open("callback.txt", "w") as f:
        f.write("This is the output of a callback")


def example_callback_with_args(args):
    with open("callback.txt", "w") as f:
        f.write("This is the output of a callback")


def test_good_callback():
    # Change working directory
    os.chdir(Path(__file__).parent)
    assert not (Path(__file__).parent / "callback.txt").is_file()

    # Run callback
    callback = _PrismCallback(example_callback)
    callback.run()
    assert (Path(__file__).parent / "callback.txt").is_file()
    os.unlink(Path(__file__).parent / "callback.txt")


def test_callback_with_args():
    with pytest.raises(ValueError) as cm:
        _PrismCallback(example_callback_with_args)
    expected_msg = "Callback function `example_callback_with_args` cannot have any arguments."  # noqa: E501
    assert str(cm.value) == expected_msg

import os 
import importlib
from pipelines.signals.base import BaseSignal

pkg_dir = os.path.dirname(__file__)
for file in os.listdir(pkg_dir):
    if file.endswith(".py") and file not in ["__init__.py", "base.py", "utils.py"]:
        module_name = file[:-3]
        importlib.import_module(f".{module_name}", package=__package__)


def _all_subclasses(cls):
    """Recursively get subclasses (handles multi-level inheritance)."""
    out = set()
    for sub in cls.__subclasses__():
        out.add(sub)
        out |= _all_subclasses(sub)
    return out


def discover_signals(*, instantiate: bool = True):
    """
    Returns all BaseSignal subclasses (instantiated by default).
    You can add allowlists/denylists/tags later without changing the pipeline.
    """
    signal_classes = sorted(_all_subclasses(BaseSignal), key=lambda c: c.__name__)
    if not instantiate:
        return signal_classes
    return [cls() for cls in signal_classes]
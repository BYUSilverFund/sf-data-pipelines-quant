from pipelines.signals.base import BaseSignal

def _all_subclasses(cls):
    out = set()
    for sub in cls.__subclasses__():
        out.add(sub)
        out |= _all_subclasses(sub)
    return out

def discover_signals(*, instantiate: bool = True):
    """
    Returns all BaseSignal subclasses (instantiated by default).
    """
    signal_classes = sorted(_all_subclasses(BaseSignal), key=lambda c: c.__name__)
    if not instantiate:
        return signal_classes
    return [cls() for cls in signal_classes]
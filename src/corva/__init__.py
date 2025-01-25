from .models.scheduled.scheduled import ScheduledDataTimeEvent


def __getattr__(name):
    import warnings

    if name == "ScheduledEvent":
        warnings.warn(
            "The corva.ScheduledEvent class is deprecated "
            "and will be removed from corva in the next major version. "
            "Import corva.ScheduledDataTimeEvent instead.",
            FutureWarning,
            stacklevel=2,
        )

        return ScheduledDataTimeEvent

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

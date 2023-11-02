import neptune

from src.neptune_experimental.safe_mode.safe_model import SafeModel


def initialize() -> None:
    pass
    # Monkey patching
    # neptune.Model = SafeModel

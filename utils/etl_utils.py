import inspect

from etl.etl_register import ETLBase, ETLRegistryHolder
from etl.etl_register import ETLCrawler, ETLCrawlerRegistryHolder

from etl import *

def _get_classes(register):
    """call register get_registry()

    Args:
        register (class): registry holder

    Returns:
        List[class]: registered classes
    """
    return register.get_registry()


def get_registered_etl_from_name(*names):
    """get class from name

    accept multi-parameter and list of name.

    Args:
        *names (List[string]): list of names
    Returns:
        dict: {cls_name: class}
    """
    if len(names) == 1 and isinstance(names[0], list):
        names = names[0]
    registry = _get_classes(ETLRegistryHolder)
    return {cls_name: registry[cls_name] for cls_name in names}


def get_registered_etl(class_type="base"):
    """get all class by class_type

    Args:
        class_type (str, optional): Defaults to "base".

    Returns:
        List[class]: Return registered class with respect to type
    """
    if class_type is "crawler":
        return _get_classes(ETLCrawlerRegistryHolder)
    return _get_classes(ETLRegistryHolder)
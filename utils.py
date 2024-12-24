from typing import Any, List


def filter_empty(list: List[Any]) -> List[Any]:
    return [item for item in list if item]
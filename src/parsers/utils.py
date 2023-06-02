import hashlib
import re

from src.parsers.item import get_quality_from_name, str_to_enum_dict


def form_item_key(item) -> str:
    """
    Функция для формирования из атрибутов ключа
    :param item: объект Item
    :return:
    """
    return hashlib.md5(
        f'{item.get("name")}#{item.get("quality")}#{item.get("stattrack")}'.encode()
    ).hexdigest()


def get_quality(item: dict) -> dict:
    item["quality"] = (
        get_quality_from_name(item["name"])
        if any(quality in item["name"] for quality in str_to_enum_dict.keys())
        else None
    )

    pattern = "|".join([f"\({quality}\)" for quality in str_to_enum_dict.keys()])
    item["name"] = re.sub(pattern, "", item["name"]).strip()
    return item

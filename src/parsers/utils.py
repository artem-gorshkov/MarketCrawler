import hashlib


def form_item_key(item) -> str:
    """
    Функция для формирования из атрибутов ключа
    :param item: объект Item
    :return:
    """
    return hashlib.md5(
        f'{item.get("name")}#{item.get("quality")}#{item.get("stattrack")}'.encode()
    ).hexdigest()

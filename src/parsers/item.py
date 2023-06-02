from dataclasses import dataclass
from enum import Enum


class ItemQuality(Enum):
    FactoryNew = 1
    MinimalWear = 2
    FieldTested = 3
    WellWorn = 4
    BattleScarred = 5


str_to_enum_dict = {
    "Factory New": ItemQuality.FactoryNew,
    "Minimal Wear": ItemQuality.MinimalWear,
    "Field-Tested": ItemQuality.FieldTested,
    "Well-Worn": ItemQuality.WellWorn,
    "Battle-Scarred": ItemQuality.BattleScarred
}


def str_to_enum(item):
    item = item.replace("(", "").replace(")", "")
    return item if item in str_to_enum_dict.keys() else None


def get_quality_from_name(name: str) -> str:
    for quality_name, obj in str_to_enum_dict.items():
        if name.find(quality_name) != -1:
            return quality_name


@dataclass
class Item:
    item_key: bin
    name: str
    price: float
    url: str = None
    quality: str = None
    stattrack: bool = False


@dataclass
class ItemWithCup(Item):
    market_cup: int = None

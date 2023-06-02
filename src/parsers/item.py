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
    "Battle-Scarred": ItemQuality.BattleScarred,
}


def str_to_enum(item):
    item = item.replace("(", "").replace(")", "")
    return str_to_enum_dict[item]


def get_quality_from_name(name: str) -> ItemQuality:
    for quality_name, obj in str_to_enum_dict.items():
        if name.find(quality_name) != -1:
            return obj


@dataclass
class Item:
    item_key: bin
    name: str
    price: float
    url: str = None
    quality: ItemQuality = None
    stattrack: bool = None


@dataclass
class ItemWithCup(Item):
    market_cup: int = None

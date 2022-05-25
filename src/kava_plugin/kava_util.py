import logging
import re
from decimal import Decimal, getcontext
from typing import Tuple, Union

logger = logging.getLogger(name=__name__)
logger.addHandler(logging.NullHandler())
getcontext().prec = 50


class KavaUtil:
    @classmethod
    def get_attribute_value(cls, attributes, key):
        return list(filter(lambda x: x["key"] == key, attributes))[0]["value"]

    @classmethod
    def get_attribute_values(cls, attributes, key):
        attributes = list(filter(lambda x: x["key"] == key, attributes))
        return list(map(lambda x: x["value"], attributes))

    @classmethod
    def get_event_value(cls, events, type):
        event = list(filter(lambda x: x["type"] == type, events))
        if len(event) == 0:
            return None
        else:
            return event[0]

    @classmethod
    def get_event_values(cls, events, types: list):
        event = list(filter(lambda x: x["type"] in types, events))
        if len(event) == 0:
            return None
        else:
            return event

    @classmethod
    def convert_uamount_amount(cls, uamount, token=None):
        denominator = 1000000
        if token is not None:
            if token == "busd":
                denominator = 100000000
            elif token == "bnb":
                denominator = 100000000
            elif token == "xrp":
                denominator = 100000000
        atom = Decimal(int(uamount)) / Decimal(denominator)
        return atom

    @classmethod
    def split_amount(cls, amount_token: str) -> Tuple[Union[Decimal, str], str]:
        amount = re.findall(r"\d+", amount_token)[0]
        token = amount_token[len(amount) :]
        if token == "ukava" or token == "":
            token = "kava"
        elif token == "xrpb":
            token = "xrp"
        return amount, token

    @classmethod
    def get_rewards(cls, event) -> list:
        if event is None:
            return []

        rewards = []
        amounts = KavaUtil.get_attribute_value(event["attributes"], "amount").split(",")
        for amount in amounts:
            amount, token = KavaUtil.split_amount(amount)
            amount = str(KavaUtil.convert_uamount_amount(amount))
            rewards.append({"reward_token": token, "reward_amount": amount})
        return rewards

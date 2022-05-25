import logging
from decimal import getcontext
from typing import Optional

from kava_plugin.kava_util import KavaUtil

logger = logging.getLogger(name=__name__)
logger.addHandler(logging.NullHandler())

getcontext().prec = 50

DELEGATE_ACTIONS = [
    "delegate",
    "begin_redelegate",
    "claim_delegator_reward",
    "withdraw_delegator_reward",
    "/cosmos.staking.v1beta1.MsgBeginRedelegate",
    "/cosmos.staking.v1beta1.MsgDelegate",
    "/cosmos.distribution.v1beta1.MsgWithdrawDelegatorReward",
    "/kava.incentive.v1beta1.MsgClaimDelegatorReward",
]


class Message:
    def __init__(self, logs_events, messages_events, height, chain_id):
        self.logs_events = logs_events
        self.messages_events = messages_events
        self.height = height
        self.chain_id = chain_id

    def get_action(self) -> Optional[str]:
        event = KavaUtil.get_event_value(self.logs_events, "message")
        if event is not None:
            action = KavaUtil.get_attribute_value(event["attributes"], "action")
        else:
            action = None
        return action

    def get_result(self) -> dict:
        action = self.get_action()
        logger.debug(action)
        result = {"action": None, "result": None}
        if action in DELEGATE_ACTIONS:
            result = self.__as_delegate()
        elif (
            action == "begin_unbonding"
            or action == "/cosmos.staking.v1beta1.MsgUndelegate"
        ):
            result = self.__as_begin_unbonding()
        elif action == "create_cdp":
            result = self.__as_create_cdp()
        elif action == "draw_cdp":
            result = self.__as_draw_cdp()
        elif action == "repay_cdp":
            result = self.__as_repay_cdp()
        elif action == "deposit_cdp":
            result = self.__as_deposit_cdp()
        elif action == "withdraw_cdp":
            result = self.__as_withdraw_cdp()
        elif action == "claim_usdx_minting_reward" or action == "claim_reward":
            result = self.__as_claim_usdx_minting_reward()
        elif action == "hard_deposit" or action == "harvest_deposit":
            result = self.__as_hard_deposit()
        elif action == "hard_withdraw" or action == "harvest_withdraw":
            result = self.__as_hard_withdraw()
        elif action == "hard_borrow":
            result = self.__as_hard_borrow()
        elif action == "hard_repay":
            result = self.__as_hard_repay()
        elif action in [
            "claim_hard_reward",
            "claim_harvest_reward",
            "/kava.incentive.v1beta1.MsgClaimHardReward",
        ]:
            result = self.__as_claim_hard_reward()
        elif action == "swap_exact_for_tokens" or action == "swap_for_exact_tokens":
            result = self.__as_swap_exact_for_tokens()
        elif action == "swap_deposit":
            result = self.__as_swap_deposit()
        elif action == "swap_withdraw":
            result = self.__as_swap_withdraw()
        elif action == "claim_swap_reward":
            result = self.__as_claim_swap_reward()
        elif action == "send" or action == "/cosmos.bank.v1beta1.MsgSend":
            result = self.__as_send()
        elif (
            action == "createAtomicSwap"
            or action == "/kava.bep3.v1beta1.MsgCreateAtomicSwap"
        ):
            result = self.__as_create_atomic_swap()
        elif action == "claimAtomicSwap" or action == "refundAtomicSwap":
            result = self.__as_claim_atomic_swap()
        elif action in ["vote", "committee_vote", "post_price"]:
            result = {"action": "vote", "result": None}
        else:
            logger.error(f"unknown action: {action}")

        return result

    def __as_delegate(self):
        result = {
            "action": "delegate",
            "result": {"staking_token": None, "staking_amount": None},
        }
        event = KavaUtil.get_event_value(self.logs_events, "delegate")
        if event is not None:
            amount = KavaUtil.get_attribute_value(event["attributes"], "amount")
            amount, token = KavaUtil.split_amount(amount)
            amount = str(KavaUtil.convert_uamount_amount(amount))
            result["result"]["staking_token"] = token
            result["result"]["staking_amount"] = amount

        event = KavaUtil.get_event_value(self.logs_events, "transfer")
        rewards = KavaUtil.get_rewards(event)
        result["result"]["rewards"] = rewards

        return result

    def __as_begin_unbonding(self):
        result = {
            "action": "begin_unbonding",
            "result": {"unbonding_token": None, "unbonding_amount": None},
        }
        event = KavaUtil.get_event_value(self.logs_events, "unbond")
        if event is not None:
            amount = KavaUtil.get_attribute_value(event["attributes"], "amount")
            amount, token = KavaUtil.split_amount(amount)
            amount = str(KavaUtil.convert_uamount_amount(amount))
            result["result"]["unbonding_token"] = token
            result["result"]["unbonding_amount"] = amount

        event = KavaUtil.get_event_value(self.logs_events, "transfer")
        rewards = KavaUtil.get_rewards(event)
        result["result"]["rewards"] = rewards

        return result

    def __as_create_cdp(self):
        result = {
            "action": "create_cdp",
            "result": {
                "deposit_token": None,
                "deposit_amount": None,
                "draw_token": None,
                "draw_amount": None,
            },
        }
        event = KavaUtil.get_event_value(self.logs_events, "cdp_deposit")
        if event is not None:
            amount = KavaUtil.get_attribute_value(event["attributes"], "amount")
            amount, token = KavaUtil.split_amount(amount)
            result["result"]["deposit_token"] = token
            result["result"]["deposit_amount"] = str(
                KavaUtil.convert_uamount_amount(amount, token)
            )

        event = KavaUtil.get_event_value(self.logs_events, "cdp_draw")
        if event is not None:
            amount = KavaUtil.get_attribute_value(event["attributes"], "amount")
            amount, token = KavaUtil.split_amount(amount)
            result["result"]["draw_token"] = token
            result["result"]["draw_amount"] = str(
                KavaUtil.convert_uamount_amount(amount, token)
            )

        return result

    def __as_draw_cdp(self):
        result = {
            "action": "draw_cdp",
            "result": {"draw_token": None, "draw_amount": None},
        }
        event = KavaUtil.get_event_value(self.logs_events, "cdp_draw")
        if event is not None:
            amount = KavaUtil.get_attribute_value(event["attributes"], "amount")
            amount, token = KavaUtil.split_amount(amount)
            result["result"]["draw_token"] = token
            result["result"]["draw_amount"] = str(
                KavaUtil.convert_uamount_amount(amount, token)
            )

        return result

    def __as_repay_cdp(self):
        result = {
            "action": "repay_cdp",
            "result": {
                "repay_token": None,
                "repay_amount": None,
                "withdraw_token": None,
                "withdraw_amount": None,
            },
        }
        event = KavaUtil.get_event_value(self.logs_events, "transfer")
        if event is not None:
            amounts = KavaUtil.get_attribute_values(event["attributes"], "amount")
            amount, token = KavaUtil.split_amount(amounts[0])
            result["result"]["repay_token"] = token
            result["result"]["repay_amount"] = str(
                KavaUtil.convert_uamount_amount(amount, token)
            )

            if len(amounts) == 2:
                amount, token = KavaUtil.split_amount(amounts[1])
                result["result"]["withdraw_token"] = token
                result["result"]["withdraw_amount"] = str(
                    KavaUtil.convert_uamount_amount(amount, token)
                )

        return result

    def __as_deposit_cdp(self):
        result = {
            "action": "deposit_cdp",
            "result": {"deposit_token": None, "deposit_amount": None},
        }
        event = KavaUtil.get_event_value(self.logs_events, "transfer")
        if event is not None:
            amount = KavaUtil.get_attribute_value(event["attributes"], "amount")
            amount, token = KavaUtil.split_amount(amount)
            result["result"]["deposit_token"] = token
            result["result"]["deposit_amount"] = str(
                KavaUtil.convert_uamount_amount(amount, token)
            )

        return result

    def __as_withdraw_cdp(self):
        result = {
            "action": "withdraw_cdp",
            "result": {"withdraw_token": None, "withdraw_amount": None},
        }
        event = KavaUtil.get_event_value(self.logs_events, "transfer")
        if event is not None:
            amount = KavaUtil.get_attribute_value(event["attributes"], "amount")
            amount, token = KavaUtil.split_amount(amount)
            result["result"]["withdraw_token"] = token
            result["result"]["withdraw_amount"] = str(
                KavaUtil.convert_uamount_amount(amount, token)
            )

        return result

    def __as_claim_usdx_minting_reward(self):
        result = {"action": "claim_usdx_minting_reward", "result": {"rewards": []}}
        event = KavaUtil.get_event_value(self.logs_events, "transfer")
        rewards = KavaUtil.get_rewards(event)
        result["result"]["rewards"] = rewards

        return result

    def __as_hard_withdraw(self):
        result = {
            "action": "hard_withdraw",
            "result": {"hard_withdraw_token": None, "hard_withdraw_amount": None},
        }
        event = KavaUtil.get_event_value(self.logs_events, "transfer")
        if event is not None:
            amount = KavaUtil.get_attribute_value(event["attributes"], "amount")
            amount, token = KavaUtil.split_amount(amount)
            result["result"]["hard_withdraw_token"] = token
            result["result"]["hard_withdraw_amount"] = str(
                KavaUtil.convert_uamount_amount(amount, token)
            )

        return result

    def __as_hard_deposit(self):
        result = {
            "action": "hard_deposit",
            "result": {"hard_deposit_token": None, "hard_deposit_amount": None},
        }
        event = KavaUtil.get_event_value(self.logs_events, "transfer")
        if event is not None:
            amount = KavaUtil.get_attribute_value(event["attributes"], "amount")
            amount, token = KavaUtil.split_amount(amount)
            result["result"]["hard_deposit_token"] = token
            result["result"]["hard_deposit_amount"] = str(
                KavaUtil.convert_uamount_amount(amount, token)
            )

        return result

    def __as_hard_borrow(self):
        result = {
            "action": "hard_borrow",
            "result": {"hard_borrow_token": None, "hard_borrow_amount": None},
        }
        event = KavaUtil.get_event_value(self.logs_events, "transfer")
        if event is not None:
            amount = KavaUtil.get_attribute_value(event["attributes"], "amount")
            amount, token = KavaUtil.split_amount(amount)
            result["result"]["hard_borrow_token"] = token
            result["result"]["hard_borrow_amount"] = str(
                KavaUtil.convert_uamount_amount(amount, token)
            )

        return result

    def __as_hard_repay(self):
        result = {
            "action": "hard_repay",
            "result": {"hard_repay_token": None, "hard_repay_amount": None},
        }
        event = KavaUtil.get_event_value(self.logs_events, "transfer")
        if event is not None:
            amount = KavaUtil.get_attribute_value(event["attributes"], "amount")
            amount, token = KavaUtil.split_amount(amount)
            result["result"]["hard_repay_token"] = token
            result["result"]["hard_repay_amount"] = str(
                KavaUtil.convert_uamount_amount(amount, token)
            )

        return result

    def __as_claim_hard_reward(self):
        result = {"action": "claim_hard_reward", "result": {"rewards": []}}
        event = KavaUtil.get_event_value(self.logs_events, "transfer")
        rewards = KavaUtil.get_rewards(event)
        result["result"]["rewards"] = rewards

        return result

    def __as_swap_exact_for_tokens(self):
        result = {
            "action": "swap_exact_for_tokens",
            "result": {
                "input_token": None,
                "input_amount": None,
                "output_token": None,
                "output_amount": None,
                "fee_token": None,
                "fee_amount": None,
            },
        }
        event = KavaUtil.get_event_value(self.logs_events, "swap_trade")
        if event is not None:
            input = KavaUtil.get_attribute_value(event["attributes"], "input")
            input_amount, input_token = KavaUtil.split_amount(input)
            output = KavaUtil.get_attribute_value(event["attributes"], "output")
            output_amount, output_token = KavaUtil.split_amount(output)
            fee = KavaUtil.get_attribute_value(event["attributes"], "fee")
            fee_amount, fee_token = KavaUtil.split_amount(fee)

            result["result"]["input_token"] = input_token
            result["result"]["input_amount"] = str(
                KavaUtil.convert_uamount_amount(input_amount, input_token)
            )
            result["result"]["output_token"] = output_token
            result["result"]["output_amount"] = str(
                KavaUtil.convert_uamount_amount(output_amount, output_token)
            )
            result["result"]["fee_token"] = fee_token
            result["result"]["fee_amount"] = str(
                KavaUtil.convert_uamount_amount(fee_amount, fee_token)
            )

        return result

    def __as_swap_deposit(self):
        result = {
            "action": "swap_deposit",
            "result": {"share_token": None, "share_amount": None, "inputs": None},
        }
        event = KavaUtil.get_event_value(self.logs_events, "swap_deposit")
        inputlist = []
        if event is not None:
            result["result"]["share_token"] = KavaUtil.get_attribute_value(
                event["attributes"], "pool_id"
            )
            result["result"]["share_amount"] = KavaUtil.get_attribute_value(
                event["attributes"], "shares"
            )

            inputs = KavaUtil.get_attribute_value(event["attributes"], "amount").split(
                ","
            )
            for input in inputs:
                amount, token = KavaUtil.split_amount(input)
                amount = str(KavaUtil.convert_uamount_amount(amount, token))
                inputlist.append({"input_token": token, "input_amount": amount})

            result["result"]["inputs"] = inputlist

        return result

    def __as_swap_withdraw(self):
        result = {
            "action": "swap_withdraw",
            "result": {"share_token": None, "share_amount": None, "outputs": None},
        }
        event = KavaUtil.get_event_value(self.logs_events, "swap_withdraw")
        outputlist = []
        if event is not None:
            result["result"]["share_token"] = KavaUtil.get_attribute_value(
                event["attributes"], "pool_id"
            )
            result["result"]["share_amount"] = KavaUtil.get_attribute_value(
                event["attributes"], "shares"
            )

            outputs = KavaUtil.get_attribute_value(event["attributes"], "amount").split(
                ","
            )
            for output in outputs:
                amount, token = KavaUtil.split_amount(output)
                amount = str(KavaUtil.convert_uamount_amount(amount, token))
                outputlist.append({"output_token": token, "output_amount": amount})

            result["result"]["outputs"] = outputlist

        return result

    def __as_claim_swap_reward(self):
        result = {"action": "claim_swap_reward", "result": {"rewards": []}}
        event = KavaUtil.get_event_value(self.logs_events, "transfer")
        rewards = KavaUtil.get_rewards(event)
        result["result"]["rewards"] = rewards

        return result

    def __as_send(self):
        result = {
            "action": "send",
            "result": {
                "sender": None,
                "recipient": None,
                "token": None,
                "amount": None,
            },
        }

        message_event = KavaUtil.get_event_value(self.logs_events, "message")
        if message_event is not None:
            result["result"]["sender"] = KavaUtil.get_attribute_value(
                message_event["attributes"], "sender"
            )

        transfer_event = KavaUtil.get_event_value(self.logs_events, "transfer")
        if transfer_event is not None:
            result["result"]["recipient"] = KavaUtil.get_attribute_value(
                transfer_event["attributes"], "recipient"
            )
            amount = KavaUtil.get_attribute_value(
                transfer_event["attributes"], "amount"
            )
            amount, token = KavaUtil.split_amount(amount)
            amount = str(KavaUtil.convert_uamount_amount(amount))
            result["result"]["token"] = token
            result["result"]["amount"] = amount

        return result

    def __as_create_atomic_swap(self):
        result = {
            "action": "create_atomic_swap",
            "result": {
                "sender": None,
                "recipient": None,
                "token": None,
                "amount": None,
            },
        }
        event = KavaUtil.get_event_value(self.logs_events, "create_atomic_swap")
        if event is not None:
            result["result"]["sender"] = KavaUtil.get_attribute_value(
                event["attributes"], "sender"
            )

        event = KavaUtil.get_event_value(self.logs_events, "transfer")
        if event is not None:
            result["result"]["recipient"] = KavaUtil.get_attribute_value(
                event["attributes"], "recipient"
            )
            amount = KavaUtil.get_attribute_value(event["attributes"], "amount")
            amount, token = KavaUtil.split_amount(amount)
            result["result"]["token"] = token
            result["result"]["amount"] = str(
                KavaUtil.convert_uamount_amount(amount, token)
            )

        return result

    def __as_claim_atomic_swap(self):
        result = {
            "action": "claim_atomic_swap",
            "result": {
                "sender": None,
                "recipient": None,
                "token": None,
                "amount": None,
            },
        }

        event = KavaUtil.get_event_value(self.logs_events, "transfer")
        if event is not None:
            result["result"]["sender"] = KavaUtil.get_attribute_value(
                event["attributes"], "sender"
            )
            result["result"]["recipient"] = KavaUtil.get_attribute_value(
                event["attributes"], "recipient"
            )
            amount = KavaUtil.get_attribute_value(event["attributes"], "amount")
            amount, token = KavaUtil.split_amount(amount)
            result["result"]["token"] = token
            result["result"]["amount"] = str(
                KavaUtil.convert_uamount_amount(amount, token)
            )

        return result

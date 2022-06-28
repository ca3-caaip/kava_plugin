import uuid
from decimal import Decimal
from typing import Optional

from senkalib.caaj_journal import CaajJournal
from senkalib.platform.kava.kava_transaction import KavaTransaction
from senkalib.token_original_id_table import TokenOriginalIdTable

from kava_plugin.message_factory import MessageFactory

MEGA = 10**6
EXA = 10**18


class KavaPlugin:
    platform = "kava"
    application = "kava"

    @classmethod
    def can_handle(cls, transaction: KavaTransaction) -> bool:
        platform_type = transaction.get_transaction()["header"]["chain_id"]
        return KavaPlugin.platform in platform_type

    @classmethod
    def get_caajs(
        cls,
        address: str,
        transaction: KavaTransaction,
        token_table: TokenOriginalIdTable,
    ) -> list:
        caajs = []

        messages = (
            MessageFactory.get_messages(transaction)
            if transaction.get_fail() is False
            else []
        )
        trade_uuid = KavaPlugin._get_uuid()
        for message in messages:
            try:
                result = message.get_result()
            except Exception as e:
                raise e

            if result["action"] == "delegate":
                caajs.extend(
                    KavaPlugin.__get_delegate_caajs(
                        transaction, result["result"], token_table, address, trade_uuid
                    )
                )
            elif result["action"] == "begin_redelegate":
                caajs.extend(
                    KavaPlugin.__get_delegate_caajs(
                        transaction, result["result"], token_table, address, trade_uuid
                    )
                )
            elif result["action"] == "begin_unbonding":
                caajs.extend(
                    KavaPlugin.__get_begin_unbonding_caajs(
                        transaction, result["result"], token_table, address, trade_uuid
                    )
                )
            elif result["action"] == "create_cdp":
                caajs.extend(
                    KavaPlugin.__get_create_cdp_caajs(
                        transaction, result["result"], token_table, address, trade_uuid
                    )
                )
            elif result["action"] == "draw_cdp":
                caajs.extend(
                    KavaPlugin.__get_draw_cdp_caajs(
                        transaction, result["result"], token_table, address, trade_uuid
                    )
                )
            elif result["action"] == "repay_cdp":
                caajs.extend(
                    KavaPlugin.__get_repay_cdp_caajs(
                        transaction, result["result"], token_table, address, trade_uuid
                    )
                )
            elif result["action"] == "deposit_cdp":
                caajs.extend(
                    KavaPlugin.__get_deposit_cdp_caajs(
                        transaction, result["result"], token_table, address, trade_uuid
                    )
                )
            elif result["action"] == "withdraw_cdp":
                caajs.extend(
                    KavaPlugin.__get_withdraw_cdp_caajs(
                        transaction, result["result"], token_table, address, trade_uuid
                    )
                )
            elif result["action"] == "claim_usdx_minting_reward":
                caajs.extend(
                    KavaPlugin.__get_claim_usdx_minting_reward_caajs(
                        transaction, result["result"], token_table, address, trade_uuid
                    )
                )
            elif result["action"] == "hard_withdraw":
                caajs.extend(
                    KavaPlugin.__get_hard_withdraw_caajs(
                        transaction, result["result"], token_table, address, trade_uuid
                    )
                )
            elif result["action"] == "hard_deposit":
                caajs.extend(
                    KavaPlugin.__get_hard_deposit_caajs(
                        transaction, result["result"], token_table, address, trade_uuid
                    )
                )
            elif result["action"] == "hard_borrow":
                caajs.extend(
                    KavaPlugin.__get_hard_borrow_caajs(
                        transaction, result["result"], token_table, address, trade_uuid
                    )
                )
            elif result["action"] == "hard_repay":
                caajs.extend(
                    KavaPlugin.__get_hard_repay_caajs(
                        transaction, result["result"], token_table, address, trade_uuid
                    )
                )
            elif result["action"] == "claim_hard_reward":
                caajs.extend(
                    KavaPlugin.__get_claim_hard_reward_caajs(
                        transaction, result["result"], token_table, address, trade_uuid
                    )
                )
            elif result["action"] == "swap_exact_for_tokens":
                caajs.extend(
                    KavaPlugin.__get_swap_exact_for_tokens_caajs(
                        transaction, result["result"], token_table, address, trade_uuid
                    )
                )
            elif result["action"] == "swap_deposit":
                caajs.extend(
                    KavaPlugin.__get_swap_deposit_caajs(
                        transaction, result["result"], token_table, address, trade_uuid
                    )
                )
            elif result["action"] == "swap_withdraw":
                caajs.extend(
                    KavaPlugin.__get_swap_withdraw_caajs(
                        transaction, result["result"], token_table, address, trade_uuid
                    )
                )
            elif result["action"] == "claim_swap_reward":
                caajs.extend(
                    KavaPlugin.__get_claim_swap_reward_caajs(
                        transaction, result["result"], token_table, address, trade_uuid
                    )
                )
            elif result["action"] == "send":
                caajs.extend(
                    KavaPlugin.__get_send_caajs(
                        transaction, result["result"], token_table, address, trade_uuid
                    )
                )
            elif (
                result["action"] == "create_atomic_swap"
                or result["action"] == "claim_atomic_swap"
            ):
                caajs.extend(
                    KavaPlugin.__get_create_atomic_swap_caajs(
                        transaction, result["result"], token_table, address, trade_uuid
                    )
                )
            elif result["action"] == "vote":
                pass
            else:
                raise Exception(
                    f"This type of transaction is not defined. transaction_id: {transaction.get_transaction_id()}"
                )

        transaction_fee = transaction.get_transaction_fee()
        if transaction_fee != 0:
            caaj_fee = KavaPlugin._get_caaj_fee(
                address, transaction, token_table, trade_uuid
            )
            caajs.extend(caaj_fee)

        return caajs

    @classmethod
    def __get_delegate_caajs(
        cls, transaction: KavaTransaction, result, token_table, address, trade_uuid
    ) -> list:
        caajs = []
        if result["staking_amount"] is not None and Decimal(
            result["staking_amount"]
        ) != Decimal("0"):
            token_original_id = KavaPlugin._get_token_original_id(
                result["staking_token"]
            )
            uti = token_table.get_uti(KavaPlugin.platform, token_original_id)

            caajs.append(
                CaajJournal(
                    transaction.get_timestamp(),
                    cls.platform,
                    cls.application,
                    "delegate",
                    transaction.get_transaction_id(),
                    trade_uuid,
                    "deposit",
                    result["staking_amount"],
                    uti,
                    address,
                    "kava_validator",
                    f'staking {result["staking_amount"]} {token_original_id}',
                )
            )
        # try to find delegate reward
        for reward in result["rewards"]:
            token_original_id = KavaPlugin._get_token_original_id(
                reward["reward_token"]
            )
            uti = token_table.get_uti(KavaPlugin.platform, token_original_id)
            caajs.append(
                CaajJournal(
                    transaction.get_timestamp(),
                    cls.platform,
                    cls.application,
                    "kava staking reward",
                    transaction.get_transaction_id(),
                    trade_uuid,
                    "get",
                    reward["reward_amount"],
                    uti,
                    "kava_staking_reward",
                    address,
                    f'staking reward {reward["reward_amount"]} {reward["reward_token"]}',
                )
            )
        return caajs

    @classmethod
    def __get_begin_unbonding_caajs(
        cls, transaction: KavaTransaction, result, token_table, address, trade_uuid
    ) -> list:
        caajs = []
        if result["unbonding_amount"] is not None and Decimal(
            result["unbonding_amount"]
        ) != Decimal("0"):
            token_original_id = KavaPlugin._get_token_original_id(
                result["unbonding_token"]
            )
            uti = token_table.get_uti(KavaPlugin.platform, token_original_id)

            caajs.append(
                CaajJournal(
                    transaction.get_timestamp(),
                    cls.platform,
                    cls.application,
                    "begin unbonding",
                    transaction.get_transaction_id(),
                    trade_uuid,
                    "withdraw",
                    result["unbonding_amount"],
                    uti,
                    "kava_validator",
                    address,
                    f'unstaking {result["unbonding_amount"]} {result["unbonding_token"]}',
                )
            )
        # try to find delegate reward
        for reward in result["rewards"]:
            token_original_id = KavaPlugin._get_token_original_id(
                reward["reward_token"]
            )
            uti = token_table.get_uti(KavaPlugin.platform, token_original_id)
            caajs.append(
                CaajJournal(
                    transaction.get_timestamp(),
                    cls.platform,
                    cls.application,
                    "kava staking reward",
                    transaction.get_transaction_id(),
                    trade_uuid,
                    "get",
                    reward["reward_amount"],
                    uti,
                    "kava_staking_reward",
                    address,
                    f'staking reward {reward["reward_amount"]} {reward["reward_token"]}',
                )
            )
        return caajs

    @classmethod
    def __get_create_cdp_caajs(
        cls, transaction: KavaTransaction, result, token_table, address, trade_uuid
    ) -> list:
        caajs = []

        token_original_id = KavaPlugin._get_token_original_id(result["deposit_token"])
        uti = token_table.get_uti(KavaPlugin.platform, token_original_id)
        caajs.append(
            CaajJournal(
                transaction.get_timestamp(),
                cls.platform,
                cls.application,
                "cdp deposit",
                transaction.get_transaction_id(),
                trade_uuid,
                "deposit",
                result["deposit_amount"],
                uti,
                address,
                "kava_cdp",
                f'cdp deposit {result["deposit_amount"]} {token_original_id}',
            )
        )

        token_original_id = KavaPlugin._get_token_original_id(result["draw_token"])
        uti = token_table.get_uti(KavaPlugin.platform, token_original_id)
        caajs.append(
            CaajJournal(
                transaction.get_timestamp(),
                cls.platform,
                cls.application,
                "cdp borrow",
                transaction.get_transaction_id(),
                trade_uuid,
                "borrow",
                result["draw_amount"],
                uti,
                "kava_cdp",
                address,
                f'cdp draw {result["draw_amount"]} {token_original_id}',
            )
        )
        return caajs

    @classmethod
    def __get_draw_cdp_caajs(
        cls, transaction: KavaTransaction, result, token_table, address, trade_uuid
    ) -> list:
        caajs = []

        token_original_id = KavaPlugin._get_token_original_id(result["draw_token"])
        uti = token_table.get_uti(KavaPlugin.platform, token_original_id)
        caajs.append(
            CaajJournal(
                transaction.get_timestamp(),
                cls.platform,
                cls.application,
                "cdp draw",
                transaction.get_transaction_id(),
                trade_uuid,
                "borrow",
                result["draw_amount"],
                uti,
                address,
                "kava_cdp",
                f'cdp repay {result["draw_amount"]} {result["draw_token"]}',
            )
        )

        return caajs

    @classmethod
    def __get_repay_cdp_caajs(
        cls, transaction: KavaTransaction, result, token_table, address, trade_uuid
    ) -> list:
        caajs = []

        token_original_id = KavaPlugin._get_token_original_id(result["repay_token"])
        uti = token_table.get_uti(KavaPlugin.platform, token_original_id)
        caajs.append(
            CaajJournal(
                transaction.get_timestamp(),
                cls.platform,
                cls.application,
                "cdp repay",
                transaction.get_transaction_id(),
                trade_uuid,
                "repay",
                result["repay_amount"],
                uti,
                address,
                "kava_cdp",
                f'cdp repay {result["repay_amount"]} {result["repay_token"]}',
            )
        )

        if (
            result["withdraw_token"] is not None
            and result["withdraw_amount"] is not None
        ):
            token_original_id = KavaPlugin._get_token_original_id(
                result["withdraw_token"]
            )
            uti = token_table.get_uti(KavaPlugin.platform, token_original_id)
            caajs.append(
                CaajJournal(
                    transaction.get_timestamp(),
                    cls.platform,
                    cls.application,
                    "cdp withdraw",
                    transaction.get_transaction_id(),
                    trade_uuid,
                    "withdraw",
                    result["withdraw_amount"],
                    uti,
                    "kava_cdp",
                    address,
                    f'cdp withdraw {result["withdraw_amount"]} {result["withdraw_token"]}',
                )
            )
        return caajs

    @classmethod
    def __get_deposit_cdp_caajs(
        cls, transaction: KavaTransaction, result, token_table, address, trade_uuid
    ) -> list:
        caajs = []

        token_original_id = KavaPlugin._get_token_original_id(result["deposit_token"])
        uti = token_table.get_uti(KavaPlugin.platform, token_original_id)
        caajs.append(
            CaajJournal(
                transaction.get_timestamp(),
                cls.platform,
                cls.application,
                "cdp deposit",
                transaction.get_transaction_id(),
                trade_uuid,
                "deposit",
                result["deposit_amount"],
                uti,
                address,
                "kava_cdp",
                f'cdp deposit {result["deposit_amount"]} {result["deposit_token"]}',
            )
        )

        return caajs

    @classmethod
    def __get_withdraw_cdp_caajs(
        cls, transaction: KavaTransaction, result, token_table, address, trade_uuid
    ) -> list:
        caajs = []

        token_original_id = KavaPlugin._get_token_original_id(result["withdraw_token"])
        uti = token_table.get_uti(KavaPlugin.platform, token_original_id)
        caajs.append(
            CaajJournal(
                transaction.get_timestamp(),
                cls.platform,
                cls.application,
                "cdp withdraw",
                transaction.get_transaction_id(),
                trade_uuid,
                "withdraw",
                result["withdraw_amount"],
                uti,
                "kava_cdp",
                address,
                f'cdp withdraw {result["withdraw_amount"]} {result["withdraw_token"]}',
            )
        )

        return caajs

    @classmethod
    def __get_claim_usdx_minting_reward_caajs(
        cls, transaction: KavaTransaction, result, token_table, address, trade_uuid
    ) -> list:
        caajs = []

        token_original_id = KavaPlugin._get_token_original_id(
            result["rewards"][0]["reward_token"]
        )
        uti = token_table.get_uti(KavaPlugin.platform, token_original_id)
        caajs.append(
            CaajJournal(
                transaction.get_timestamp(),
                cls.platform,
                cls.application,
                "cdp claim reward",
                transaction.get_transaction_id(),
                trade_uuid,
                "get",
                result["rewards"][0]["reward_amount"],
                uti,
                "kava_cdp",
                address,
                f'cdp reward {result["rewards"][0]["reward_amount"]} {result["rewards"][0]["reward_token"]}',
            )
        )

        return caajs

    @classmethod
    def __get_hard_withdraw_caajs(
        cls, transaction: KavaTransaction, result, token_table, address, trade_uuid
    ) -> list:
        caajs = []

        token_original_id = KavaPlugin._get_token_original_id(
            result["hard_withdraw_token"]
        )
        uti = token_table.get_uti(KavaPlugin.platform, token_original_id)
        caajs.append(
            CaajJournal(
                transaction.get_timestamp(),
                cls.platform,
                cls.application,
                "hard withdraw",
                transaction.get_transaction_id(),
                trade_uuid,
                "withdraw",
                result["hard_withdraw_amount"],
                uti,
                "hard_lending",
                address,
                f'hard withdraw {result["hard_withdraw_amount"]} {result["hard_withdraw_token"]}',
            )
        )

        return caajs

    @classmethod
    def __get_hard_deposit_caajs(
        cls, transaction: KavaTransaction, result, token_table, address, trade_uuid
    ) -> list:
        caajs = []

        token_original_id = KavaPlugin._get_token_original_id(
            result["hard_deposit_token"]
        )
        uti = token_table.get_uti(KavaPlugin.platform, token_original_id)
        caajs.append(
            CaajJournal(
                transaction.get_timestamp(),
                cls.platform,
                cls.application,
                "hard deposit",
                transaction.get_transaction_id(),
                trade_uuid,
                "deposit",
                result["hard_deposit_amount"],
                uti,
                address,
                "hard_lending",
                f'hard deposit {result["hard_deposit_amount"]} {result["hard_deposit_token"]}',
            )
        )

        return caajs

    @classmethod
    def __get_hard_borrow_caajs(
        cls, transaction: KavaTransaction, result, token_table, address, trade_uuid
    ) -> list:
        caajs = []

        token_original_id = KavaPlugin._get_token_original_id(
            result["hard_borrow_token"]
        )
        uti = token_table.get_uti(KavaPlugin.platform, token_original_id)
        caajs.append(
            CaajJournal(
                transaction.get_timestamp(),
                cls.platform,
                cls.application,
                "hard borrow",
                transaction.get_transaction_id(),
                trade_uuid,
                "borrow",
                result["hard_borrow_amount"],
                uti,
                "hard_lending",
                address,
                f'hard borrow {result["hard_borrow_amount"]} {result["hard_borrow_token"]}',
            )
        )

        return caajs

    @classmethod
    def __get_hard_repay_caajs(
        cls, transaction: KavaTransaction, result, token_table, address, trade_uuid
    ) -> list:
        caajs = []

        token_original_id = KavaPlugin._get_token_original_id(
            result["hard_repay_token"]
        )
        uti = token_table.get_uti(KavaPlugin.platform, token_original_id)
        caajs.append(
            CaajJournal(
                transaction.get_timestamp(),
                cls.platform,
                cls.application,
                "hard repay",
                transaction.get_transaction_id(),
                trade_uuid,
                "repay",
                result["hard_repay_amount"],
                uti,
                address,
                "hard_lending",
                f'hard repay {result["hard_repay_amount"]} {result["hard_repay_token"]}',
            )
        )

        return caajs

    @classmethod
    def __get_claim_hard_reward_caajs(
        cls, transaction: KavaTransaction, result, token_table, address, trade_uuid
    ) -> list:
        caajs = []

        for reward in result["rewards"]:
            token_original_id = KavaPlugin._get_token_original_id(
                reward["reward_token"]
            )
            uti = token_table.get_uti(KavaPlugin.platform, token_original_id)
            caajs.append(
                CaajJournal(
                    transaction.get_timestamp(),
                    cls.platform,
                    cls.application,
                    "claim hard reward",
                    transaction.get_transaction_id(),
                    trade_uuid,
                    "get",
                    reward["reward_amount"],
                    uti,
                    "hard_lending",
                    address,
                    f'hard lending reward receive {reward["reward_amount"]} {reward["reward_token"]}',
                )
            )

        return caajs

    @classmethod
    def __get_swap_exact_for_tokens_caajs(
        cls, transaction: KavaTransaction, result, token_table, address, trade_uuid
    ) -> list:
        caajs = []

        token_original_id = KavaPlugin._get_token_original_id(result["input_token"])
        uti = token_table.get_uti(KavaPlugin.platform, token_original_id)
        caajs.append(
            CaajJournal(
                transaction.get_timestamp(),
                cls.platform,
                cls.application,
                "swap exact for tokens",
                transaction.get_transaction_id(),
                trade_uuid,
                "lose",
                result["input_amount"],
                uti,
                address,
                "kava_swap",
                f'buy {result["output_amount"]} {result["output_token"]} sell {result["input_amount"]} {result["input_token"]}',
            )
        )

        token_original_id = KavaPlugin._get_token_original_id(result["output_token"])
        uti = token_table.get_uti(KavaPlugin.platform, token_original_id)
        caajs.append(
            CaajJournal(
                transaction.get_timestamp(),
                cls.platform,
                cls.application,
                "swap exact for tokens",
                transaction.get_transaction_id(),
                trade_uuid,
                "get",
                result["output_amount"],
                uti,
                "kava_swap",
                address,
                f'buy {result["output_amount"]} {result["output_token"]} sell {result["input_amount"]} {result["input_token"]}',
            )
        )

        token_original_id = KavaPlugin._get_token_original_id(result["fee_token"])
        uti = token_table.get_uti(KavaPlugin.platform, token_original_id)
        caajs.append(
            CaajJournal(
                transaction.get_timestamp(),
                cls.platform,
                cls.application,
                "swap exact for tokens",
                transaction.get_transaction_id(),
                trade_uuid,
                "lose",
                result["fee_amount"],
                uti,
                address,
                "kava_swap",
                f'pay {result["fee_amount"]} {result["fee_token"]} as swap fee',
            )
        )

        return caajs

    @classmethod
    def __get_swap_deposit_caajs(
        cls, transaction: KavaTransaction, result, token_table, address, trade_uuid
    ) -> list:
        caajs = []

        uti = token_table.get_uti(KavaPlugin.platform, result["share_token"])
        caajs.append(
            CaajJournal(
                transaction.get_timestamp(),
                cls.platform,
                cls.application,
                "swap deposit",
                transaction.get_transaction_id(),
                trade_uuid,
                "get_bonds",
                result["share_amount"],
                uti,
                "kava_swap",
                address,
                f'kava swap receive {result["share_amount"]} {result["share_token"]}',
            )
        )

        for input in result["inputs"]:
            token_original_id = KavaPlugin._get_token_original_id(input["input_token"])
            uti = token_table.get_uti(KavaPlugin.platform, token_original_id)
            caajs.append(
                CaajJournal(
                    transaction.get_timestamp(),
                    cls.platform,
                    cls.application,
                    "swap deposit",
                    transaction.get_transaction_id(),
                    trade_uuid,
                    "deposit",
                    input["input_amount"],
                    uti,
                    address,
                    "kava_swap",
                    f'kava swap send {input["input_amount"]} {input["input_token"]}',
                )
            )

        return caajs

    @classmethod
    def __get_swap_withdraw_caajs(
        cls, transaction: KavaTransaction, result, token_table, address, trade_uuid
    ) -> list:
        caajs = []

        uti = token_table.get_uti(KavaPlugin.platform, result["share_token"])
        caajs.append(
            CaajJournal(
                transaction.get_timestamp(),
                cls.platform,
                cls.application,
                "swap withdraw",
                transaction.get_transaction_id(),
                trade_uuid,
                "lose_bonds",
                result["share_amount"],
                uti,
                address,
                "kava_swap",
                f'kava swap send {result["share_amount"]} {result["share_token"]}',
            )
        )

        for output in result["outputs"]:
            token_original_id = KavaPlugin._get_token_original_id(
                output["output_token"]
            )
            uti = token_table.get_uti(KavaPlugin.platform, token_original_id)
            caajs.append(
                CaajJournal(
                    transaction.get_timestamp(),
                    cls.platform,
                    cls.application,
                    "swap withdraw",
                    transaction.get_transaction_id(),
                    trade_uuid,
                    "withdraw",
                    output["output_amount"],
                    uti,
                    "kava_swap",
                    address,
                    f'kava swap receive {output["output_amount"]} {output["output_token"]}',
                )
            )

        return caajs

    @classmethod
    def __get_claim_swap_reward_caajs(
        cls, transaction: KavaTransaction, result, token_table, address, trade_uuid
    ) -> list:
        caajs = []

        for reward in result["rewards"]:
            token_original_id = KavaPlugin._get_token_original_id(
                reward["reward_token"]
            )
            uti = token_table.get_uti(KavaPlugin.platform, token_original_id)
            caajs.append(
                CaajJournal(
                    transaction.get_timestamp(),
                    cls.platform,
                    cls.application,
                    "claim swap reward",
                    transaction.get_transaction_id(),
                    trade_uuid,
                    "get",
                    reward["reward_amount"],
                    uti,
                    "kava_swap",
                    address,
                    f'kava swap reward receive {reward["reward_amount"]} {reward["reward_token"]}',
                )
            )

        return caajs

    @classmethod
    def __get_send_caajs(
        cls, transaction: KavaTransaction, result, token_table, address, trade_uuid
    ) -> list:
        caajs = []

        recipient = result["recipient"]
        sender = result["sender"]
        if address in [recipient, sender]:
            if address == recipient:
                caaj_type = "receive"
                message = f'{recipient} {caaj_type} {result["amount"]} {result["token"]} from {sender}'
            else:
                caaj_type = "send"
                message = f'{sender} {caaj_type} {result["amount"]} {result["token"]} to {recipient}'

            token_original_id = KavaPlugin._get_token_original_id(result["token"])
            uti = token_table.get_uti(KavaPlugin.platform, token_original_id)
            caajs.append(
                CaajJournal(
                    transaction.get_timestamp(),
                    cls.platform,
                    cls.application,
                    "send",
                    transaction.get_transaction_id(),
                    trade_uuid,
                    caaj_type,
                    result["amount"],
                    uti,
                    sender,
                    recipient,
                    message,
                )
            )

        return caajs

    @classmethod
    def __get_create_atomic_swap_caajs(
        cls, transaction: KavaTransaction, result, token_table, address, trade_uuid
    ) -> list:
        caajs = []

        recipient = result["recipient"]
        sender = result["sender"]
        if address in [recipient, sender]:
            if address == recipient:
                caaj_type = "receive"
                message = f'{recipient} {caaj_type} {result["amount"]} {result["token"]} from kava_bc_atomic_swap'
                from_address = "kava_bc_atomic_swap"
                to_address = address
            else:
                caaj_type = "send"
                message = f'{sender} {caaj_type} {result["amount"]} {result["token"]} to kava_bc_atomic_swap'
                from_address = address
                to_address = "kava_bc_atomic_swap"

            token_original_id = KavaPlugin._get_token_original_id(result["token"])
            uti = token_table.get_uti(KavaPlugin.platform, token_original_id)
            caajs.append(
                CaajJournal(
                    transaction.get_timestamp(),
                    cls.platform,
                    cls.application,
                    "create atomic swap",
                    transaction.get_transaction_id(),
                    trade_uuid,
                    caaj_type,
                    result["amount"],
                    uti,
                    from_address,
                    to_address,
                    message,
                )
            )

        return caajs

    @classmethod
    def _get_uuid(cls) -> str:
        return str(uuid.uuid4())

    @classmethod
    def _get_token_original_id(cls, value: Optional[str]) -> Optional[str]:
        if value == "ukava":
            value = "kava"
        elif value == "":
            value = None
        return value

    @classmethod
    def _get_caaj_fee(
        cls,
        address: str,
        transaction: KavaTransaction,
        token_table: TokenOriginalIdTable,
        trade_uuid,
    ) -> list:
        caajs = []
        caajs.append(
            CaajJournal(
                transaction.get_timestamp(),
                cls.platform,
                cls.application,
                cls.platform,
                transaction.get_transaction_id(),
                trade_uuid,
                "lose",
                str(transaction.get_transaction_fee() / Decimal(MEGA)),
                "kava/kava",
                address,
                "fee",
                "",
            )
        )
        return caajs

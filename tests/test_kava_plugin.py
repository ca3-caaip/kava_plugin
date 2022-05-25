import json
import unittest
from typing import Optional
from unittest.mock import MagicMock

from senkalib.chain.kava.kava_transaction import KavaTransaction

from kava_plugin.kava_plugin import KavaPlugin


class TestKavaPlugin(unittest.TestCase):
    @classmethod
    def get_token_table_mock(cls):
        def mock_get_symbol(chain: str, token_original_id: str) -> Optional[str]:
            if chain == "kava" and token_original_id is None:
                return "kava"
            elif chain == "kava" and token_original_id == "hard":
                return "hard"
            elif chain == "kava" and token_original_id == "swp":
                return "swp"
            elif chain == "kava" and token_original_id == "busd":
                return "busd"
            elif chain == "kava" and token_original_id == "usdx":
                return "usdx"
            else:
                return None

        def mock_get_symbol_uuid(chain: str, token_original_id: str) -> str:
            return "3a2570c5-15c4-2860-52a8-bff14f27a236"

        mock = MagicMock()
        mock.get_symbol.side_effect = mock_get_symbol
        mock.get_symbol_uuid.side_effect = mock_get_symbol_uuid
        return mock

    def test_transaction_fee(self):
        test_data = TestKavaPlugin._get_test_data("delegate_v8")
        transaction = KavaTransaction(test_data)
        mock = TestKavaPlugin.get_token_table_mock()
        caajs = KavaPlugin.get_caajs(
            "kava1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8m2splc", transaction, mock
        )

        assert caajs[0].trade_uuid == caajs[1].trade_uuid
        assert caajs[0].trade_uuid == caajs[2].trade_uuid

        caaj_transaction_fee = caajs[2]
        assert caaj_transaction_fee.executed_at == "2021-10-15 01:57:03"
        assert caaj_transaction_fee.chain == "kava"
        assert caaj_transaction_fee.platform == "kava"
        assert caaj_transaction_fee.application == "kava"
        assert (
            caaj_transaction_fee.transaction_id
            == "415D5669CDDE1E89808932C3E9386169693D73B21478885238E85F19DBE04277"
        )
        assert caaj_transaction_fee.type == "lose"
        assert caaj_transaction_fee.amount == "0.0001"
        assert caaj_transaction_fee.token_symbol == "kava"
        assert caaj_transaction_fee.token_original_id is None
        assert (
            caaj_transaction_fee.caaj_from
            == "kava1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8m2splc"
        )
        assert caaj_transaction_fee.caaj_to == "fee"
        assert caaj_transaction_fee.comment == ""

    def test_delegate(self):
        test_data = TestKavaPlugin._get_test_data("delegate_v8")
        transaction = KavaTransaction(test_data)
        mock = TestKavaPlugin.get_token_table_mock()
        caajs = KavaPlugin.get_caajs(
            "kava1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8m2splc", transaction, mock
        )

        assert caajs[0].trade_uuid == caajs[1].trade_uuid
        assert caajs[0].trade_uuid == caajs[2].trade_uuid

        caaj_deposit = caajs[0]
        assert caaj_deposit.executed_at == "2021-10-15 01:57:03"
        assert caaj_deposit.chain == "kava"
        assert caaj_deposit.platform == "kava"
        assert caaj_deposit.application == "delegate"
        assert (
            caaj_deposit.transaction_id
            == "415D5669CDDE1E89808932C3E9386169693D73B21478885238E85F19DBE04277"
        )
        assert caaj_deposit.type == "deposit"
        assert caaj_deposit.amount == "0.00118"
        assert caaj_deposit.token_symbol == "kava"
        assert caaj_deposit.token_original_id is None
        assert caaj_deposit.caaj_from == "kava1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8m2splc"
        assert caaj_deposit.caaj_to == "kava_validator"
        assert caaj_deposit.comment == "staking 0.00118 kava"

        caaj_reward = caajs[1]
        assert caaj_reward.executed_at == "2021-10-15 01:57:03"
        assert caaj_reward.chain == "kava"
        assert caaj_reward.platform == "kava"
        assert caaj_reward.application == "kava staking reward"
        assert (
            caaj_reward.transaction_id
            == "415D5669CDDE1E89808932C3E9386169693D73B21478885238E85F19DBE04277"
        )
        assert caaj_reward.type == "get"
        assert caaj_reward.amount == "0.000039"
        assert caaj_reward.token_symbol == "kava"
        assert caaj_reward.token_original_id is None
        assert caaj_reward.caaj_from == "kava_staking_reward"
        assert caaj_reward.caaj_to == "kava1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8m2splc"
        assert caaj_reward.comment == "staking reward 0.000039 kava"

        test_data = TestKavaPlugin._get_test_data("begin_redelegate_v8")
        transaction = KavaTransaction(test_data)
        mock = TestKavaPlugin.get_token_table_mock()
        caajs = KavaPlugin.get_caajs(
            "kava1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8m2splc", transaction, mock
        )

        assert caajs[0].trade_uuid == caajs[1].trade_uuid

        caaj_reward = caajs[0]
        assert caaj_reward.executed_at == "2021-09-18 03:21:16"
        assert caaj_reward.chain == "kava"
        assert caaj_reward.platform == "kava"
        assert caaj_reward.application == "kava staking reward"
        assert (
            caaj_reward.transaction_id
            == "1C2C7D7C15A38B40F10CB9AE51E86F4455E4EB3969BA4EFF977FB07B4105F33E"
        )
        assert caaj_reward.type == "get"
        assert caaj_reward.amount == "3.687213"
        assert caaj_reward.token_symbol == "kava"
        assert caaj_reward.token_original_id is None
        assert caaj_reward.caaj_from == "kava_staking_reward"
        assert caaj_reward.caaj_to == "kava1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8m2splc"
        assert caaj_reward.comment == "staking reward 3.687213 kava"

        test_data = TestKavaPlugin._get_test_data("claim_delegator_reward_v8")
        transaction = KavaTransaction(test_data)
        mock = TestKavaPlugin.get_token_table_mock()
        caajs = KavaPlugin.get_caajs(
            "kava1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8m2splc", transaction, mock
        )

        caaj_reward = caajs[0]
        assert caaj_reward.executed_at == "2021-09-03 13:05:07"
        assert caaj_reward.chain == "kava"
        assert caaj_reward.platform == "kava"
        assert caaj_reward.application == "kava staking reward"
        assert (
            caaj_reward.transaction_id
            == "ABE8DF74E23D75CEDB506FA4F7A6B595ED936D60FA8ECAE84F873330AFB5B445"
        )
        assert caaj_reward.type == "get"
        assert caaj_reward.amount == "0.224049"
        assert caaj_reward.token_symbol == "hard"
        assert caaj_reward.token_original_id == "hard"
        assert caaj_reward.caaj_from == "kava_staking_reward"
        assert caaj_reward.caaj_to == "kava1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8m2splc"
        assert caaj_reward.comment == "staking reward 0.224049 hard"

        test_data = TestKavaPlugin._get_test_data("withdraw_delegator_reward_v8")
        transaction = KavaTransaction(test_data)
        mock = TestKavaPlugin.get_token_table_mock()
        caajs = KavaPlugin.get_caajs(
            "kava1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8m2splc", transaction, mock
        )

        caaj_reward = caajs[0]
        assert caaj_reward.executed_at == "2021-09-06 21:49:27"
        assert caaj_reward.chain == "kava"
        assert caaj_reward.platform == "kava"
        assert caaj_reward.application == "kava staking reward"
        assert (
            caaj_reward.transaction_id
            == "AD3AA595D579C0F3186C12FB6AD6A0D083CEC9EF97C33820CF3E230C92D65316"
        )
        assert caaj_reward.type == "get"
        assert caaj_reward.amount == "1.298035"
        assert caaj_reward.token_symbol == "kava"
        assert caaj_reward.token_original_id is None
        assert caaj_reward.caaj_from == "kava_staking_reward"
        assert caaj_reward.caaj_to == "kava1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8m2splc"
        assert caaj_reward.comment == "staking reward 1.298035 kava"

    def test_begin_unbonding(self):
        test_data = TestKavaPlugin._get_test_data("begin_unbonding_v7")
        transaction = KavaTransaction(test_data)
        mock = TestKavaPlugin.get_token_table_mock()
        caajs = KavaPlugin.get_caajs(
            "kava1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8m2splc", transaction, mock
        )

        assert caajs[0].trade_uuid == caajs[1].trade_uuid
        assert caajs[0].trade_uuid == caajs[2].trade_uuid

        caaj_withdraw = caajs[0]
        assert caaj_withdraw.executed_at == "2021-08-27 10:25:33"
        assert caaj_withdraw.chain == "kava"
        assert caaj_withdraw.platform == "kava"
        assert caaj_withdraw.application == "begin unbonding"
        assert (
            caaj_withdraw.transaction_id
            == "F802145370951CCCE74B87332F02FB5F32B9D8DB5473A7E493931DE6B7C24F52"
        )
        assert caaj_withdraw.type == "withdraw"
        assert caaj_withdraw.amount == "343.546602"
        assert caaj_withdraw.token_symbol == "kava"
        assert caaj_withdraw.token_original_id is None
        assert caaj_withdraw.caaj_from == "kava_validator"
        assert caaj_withdraw.caaj_to == "kava1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8m2splc"
        assert caaj_withdraw.comment == "unstaking 343.546602 kava"

        caaj_reward = caajs[1]
        assert caaj_reward.executed_at == "2021-08-27 10:25:33"
        assert caaj_reward.chain == "kava"
        assert caaj_reward.platform == "kava"
        assert caaj_reward.application == "kava staking reward"
        assert (
            caaj_reward.transaction_id
            == "F802145370951CCCE74B87332F02FB5F32B9D8DB5473A7E493931DE6B7C24F52"
        )
        assert caaj_reward.type == "get"
        assert caaj_reward.amount == "0.001703"
        assert caaj_reward.token_symbol == "kava"
        assert caaj_reward.token_original_id is None
        assert caaj_reward.caaj_from == "kava_staking_reward"
        assert caaj_reward.caaj_to == "kava1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8m2splc"
        assert caaj_reward.comment == "staking reward 0.001703 kava"

    def test_create_cdp(self):
        test_data = TestKavaPlugin._get_test_data("create_cdp_v7")
        transaction = KavaTransaction(test_data)
        mock = TestKavaPlugin.get_token_table_mock()
        caajs = KavaPlugin.get_caajs(
            "kava1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8m2splc", transaction, mock
        )

        assert caajs[0].trade_uuid == caajs[1].trade_uuid

        caaj_deposit = caajs[0]
        assert caaj_deposit.executed_at == "2021-06-03 14:41:06"
        assert caaj_deposit.chain == "kava"
        assert caaj_deposit.platform == "kava"
        assert caaj_deposit.application == "cdp deposit"
        assert (
            caaj_deposit.transaction_id
            == "89261DCBDB172AA1AE446F8AC7E90BE7AFA422457C0145D741A77AB07D2D7D08"
        )
        assert caaj_deposit.type == "deposit"
        assert caaj_deposit.amount == "10093.653846"
        assert caaj_deposit.token_symbol == "hard"
        assert caaj_deposit.token_original_id == "hard"
        assert caaj_deposit.caaj_from == "kava1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8m2splc"
        assert caaj_deposit.caaj_to == "kava_cdp"
        assert caaj_deposit.comment == "cdp deposit 10093.653846 hard"

        caaj_borrow = caajs[1]
        assert caaj_borrow.executed_at == "2021-06-03 14:41:06"
        assert caaj_borrow.chain == "kava"
        assert caaj_borrow.platform == "kava"
        assert caaj_borrow.application == "cdp borrow"
        assert (
            caaj_borrow.transaction_id
            == "89261DCBDB172AA1AE446F8AC7E90BE7AFA422457C0145D741A77AB07D2D7D08"
        )
        assert caaj_borrow.type == "borrow"
        assert caaj_borrow.amount == "3500"
        assert caaj_borrow.token_symbol == "usdx"
        assert caaj_borrow.token_original_id == "usdx"
        assert caaj_borrow.caaj_from == "kava_cdp"
        assert caaj_borrow.caaj_to == "kava1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8m2splc"
        assert caaj_borrow.comment == "cdp draw 3500 usdx"

    def test_draw_cdp(self):
        test_data = TestKavaPlugin._get_test_data("draw_cdp_v7")
        transaction = KavaTransaction(test_data)
        mock = TestKavaPlugin.get_token_table_mock()
        caajs = KavaPlugin.get_caajs(
            "kava1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8m2splc", transaction, mock
        )

        caaj_borrow = caajs[0]
        assert caaj_borrow.executed_at == "2021-05-31 01:41:38"
        assert caaj_borrow.chain == "kava"
        assert caaj_borrow.platform == "kava"
        assert caaj_borrow.application == "cdp draw"
        assert (
            caaj_borrow.transaction_id
            == "1CAD58F890233F986B56B9D62955B398AA1F0F31F62A7AAC440BCC234F0A0D17"
        )
        assert caaj_borrow.type == "borrow"
        assert caaj_borrow.amount == "300"
        assert caaj_borrow.token_symbol == "usdx"
        assert caaj_borrow.token_original_id == "usdx"
        assert caaj_borrow.caaj_from == "kava1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8m2splc"
        assert caaj_borrow.caaj_to == "kava_cdp"
        assert caaj_borrow.comment == "cdp repay 300 usdx"

    def test_repay_cdp(self):
        test_data = TestKavaPlugin._get_test_data("repay_cdp_v8")
        transaction = KavaTransaction(test_data)
        mock = TestKavaPlugin.get_token_table_mock()
        caajs = KavaPlugin.get_caajs(
            "kava1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8m2splc", transaction, mock
        )

        assert caajs[0].trade_uuid == caajs[1].trade_uuid

        caaj_repay = caajs[0]
        assert caaj_repay.executed_at == "2021-09-09 04:42:15"
        assert caaj_repay.chain == "kava"
        assert caaj_repay.platform == "kava"
        assert caaj_repay.application == "cdp repay"
        assert (
            caaj_repay.transaction_id
            == "158660081326903E34AD7B4CD1D95C600A743B63392F88667A388AB048E557FE"
        )
        assert caaj_repay.type == "repay"
        assert caaj_repay.amount == "10.050333"
        assert caaj_repay.token_symbol == "usdx"
        assert caaj_repay.token_original_id == "usdx"
        assert caaj_repay.caaj_from == "kava1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8m2splc"
        assert caaj_repay.caaj_to == "kava_cdp"
        assert caaj_repay.comment == "cdp repay 10.050333 usdx"

        caaj_withdraw = caajs[1]
        assert caaj_withdraw.executed_at == "2021-09-09 04:42:15"
        assert caaj_withdraw.chain == "kava"
        assert caaj_withdraw.platform == "kava"
        assert caaj_withdraw.application == "cdp withdraw"
        assert (
            caaj_withdraw.transaction_id
            == "158660081326903E34AD7B4CD1D95C600A743B63392F88667A388AB048E557FE"
        )
        assert caaj_withdraw.type == "withdraw"
        assert caaj_withdraw.amount == "0.36428994"
        assert caaj_withdraw.token_symbol == "bnb"
        assert caaj_withdraw.token_original_id == "bnb"
        assert caaj_withdraw.caaj_from == "kava_cdp"
        assert caaj_withdraw.caaj_to == "kava1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8m2splc"
        assert caaj_withdraw.comment == "cdp withdraw 0.36428994 bnb"

    def test_deposit_cdp(self):
        test_data = TestKavaPlugin._get_test_data("deposit_cdp_v8")
        transaction = KavaTransaction(test_data)
        mock = TestKavaPlugin.get_token_table_mock()
        caajs = KavaPlugin.get_caajs(
            "kava1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8m2splc", transaction, mock
        )

        caaj_deposit = caajs[0]
        assert caaj_deposit.executed_at == "2021-10-02 17:19:14"
        assert caaj_deposit.chain == "kava"
        assert caaj_deposit.platform == "kava"
        assert caaj_deposit.application == "cdp deposit"
        assert (
            caaj_deposit.transaction_id
            == "88F40ACA82A9750B1750388E076C88E45B1E000E0BC32E0E2EAFFD22FF2E8177"
        )
        assert caaj_deposit.type == "deposit"
        assert caaj_deposit.amount == "5063.76309394"
        assert caaj_deposit.token_symbol == "xrp"
        assert caaj_deposit.token_original_id == "xrp"
        assert caaj_deposit.caaj_from == "kava1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8m2splc"
        assert caaj_deposit.caaj_to == "kava_cdp"
        assert caaj_deposit.comment == "cdp deposit 5063.76309394 xrp"

    def test_withdraw_cdp(self):
        test_data = TestKavaPlugin._get_test_data("withdraw_cdp_v8")
        transaction = KavaTransaction(test_data)
        mock = TestKavaPlugin.get_token_table_mock()
        caajs = KavaPlugin.get_caajs(
            "kava1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8m2splc", transaction, mock
        )

        caaj_withdraw = caajs[0]
        assert caaj_withdraw.executed_at == "2021-09-09 04:39:11"
        assert caaj_withdraw.chain == "kava"
        assert caaj_withdraw.platform == "kava"
        assert caaj_withdraw.application == "cdp withdraw"
        assert (
            caaj_withdraw.transaction_id
            == "177C2066F3604B1E7EDE6F4CF2B355125FB58E22F026E6E82FD3556D7EE416DD"
        )
        assert caaj_withdraw.type == "withdraw"
        assert caaj_withdraw.amount == "1"
        assert caaj_withdraw.token_symbol == "bnb"
        assert caaj_withdraw.token_original_id == "bnb"
        assert caaj_withdraw.caaj_from == "kava_cdp"
        assert caaj_withdraw.caaj_to == "kava1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8m2splc"
        assert caaj_withdraw.comment == "cdp withdraw 1 bnb"

    def test_claim_usdx_minting_reward(self):
        test_data = TestKavaPlugin._get_test_data("claim_usdx_minting_reward_v7")
        transaction = KavaTransaction(test_data)
        mock = TestKavaPlugin.get_token_table_mock()
        caajs = KavaPlugin.get_caajs(
            "kava1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8m2splc", transaction, mock
        )

        caaj_reward = caajs[0]
        assert caaj_reward.executed_at == "2021-06-03 21:54:45"
        assert caaj_reward.chain == "kava"
        assert caaj_reward.platform == "kava"
        assert caaj_reward.application == "cdp claim reward"
        assert (
            caaj_reward.transaction_id
            == "B3B3CC7E9AC09231932C976D2DBBC1B97111A6A306BC74DF7C6A8E61E1FD6AA0"
        )
        assert caaj_reward.type == "get"
        assert caaj_reward.amount == "3.746212"
        assert caaj_reward.token_symbol == "kava"
        assert caaj_reward.token_original_id is None
        assert caaj_reward.caaj_from == "kava_cdp"
        assert caaj_reward.caaj_to == "kava1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8m2splc"
        assert caaj_reward.comment == "cdp reward 3.746212 kava"

        # claim_reward
        test_data = TestKavaPlugin._get_test_data("claim_reward_v6")
        transaction = KavaTransaction(test_data)
        mock = TestKavaPlugin.get_token_table_mock()
        caajs = KavaPlugin.get_caajs(
            "kava1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8m2splc", transaction, mock
        )

        caaj_reward = caajs[0]
        assert caaj_reward.executed_at == "2021-03-31 22:42:30"
        assert caaj_reward.chain == "kava"
        assert caaj_reward.platform == "kava"
        assert caaj_reward.application == "cdp claim reward"
        assert (
            caaj_reward.transaction_id
            == "EC9288B6845A67B2DC85FAE9DEE7A8BCCE27C99B10635B1BBBA679C1E052ED49"
        )
        assert caaj_reward.type == "get"
        assert caaj_reward.amount == "0.293872"
        assert caaj_reward.token_symbol == "kava"
        assert caaj_reward.token_original_id is None
        assert caaj_reward.caaj_from == "kava_cdp"
        assert caaj_reward.caaj_to == "kava1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8m2splc"
        assert caaj_reward.comment == "cdp reward 0.293872 kava"

    def test_hard_deposit(self):
        test_data = TestKavaPlugin._get_test_data("hard_deposit_v8")
        transaction = KavaTransaction(test_data)
        mock = TestKavaPlugin.get_token_table_mock()
        caajs = KavaPlugin.get_caajs(
            "kava1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8m2splc", transaction, mock
        )

        caaj_deposit = caajs[0]
        assert caaj_deposit.executed_at == "2021-10-16 13:03:13"
        assert caaj_deposit.chain == "kava"
        assert caaj_deposit.platform == "kava"
        assert caaj_deposit.application == "hard deposit"
        assert (
            caaj_deposit.transaction_id
            == "D88AEF4191B60FCCACCA126E310C3579FA30873BFC4D43C2A65FB8BBB87A7851"
        )
        assert caaj_deposit.type == "deposit"
        assert caaj_deposit.amount == "1513.591717"
        assert caaj_deposit.token_symbol == "kava"
        assert caaj_deposit.token_original_id is None
        assert caaj_deposit.caaj_from == "kava1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8m2splc"
        assert caaj_deposit.caaj_to == "hard_lending"
        assert caaj_deposit.comment == "hard deposit 1513.591717 kava"

        # harvest_deposit
        test_data = TestKavaPlugin._get_test_data("harvest_deposit_v4")
        transaction = KavaTransaction(test_data)
        mock = TestKavaPlugin.get_token_table_mock()
        caajs = KavaPlugin.get_caajs(
            "kava1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8m2splc", transaction, mock
        )

        caaj_deposit = caajs[0]
        assert caaj_deposit.executed_at == "2020-12-23 01:48:01"
        assert caaj_deposit.chain == "kava"
        assert caaj_deposit.platform == "kava"
        assert caaj_deposit.application == "hard deposit"
        assert (
            caaj_deposit.transaction_id
            == "EB1EF484C2773368F1AEA6DFD6D672A87A2C6802FD8CFA2D251BCC8A5F510275"
        )
        assert caaj_deposit.type == "deposit"
        assert caaj_deposit.amount == "3610.692343"
        assert caaj_deposit.token_symbol == "usdx"
        assert caaj_deposit.token_original_id == "usdx"
        assert caaj_deposit.caaj_from == "kava1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8m2splc"
        assert caaj_deposit.caaj_to == "hard_lending"
        assert caaj_deposit.comment == "hard deposit 3610.692343 usdx"

    def test_hard_withdraw(self):
        test_data = TestKavaPlugin._get_test_data("hard_withdraw_v8")
        transaction = KavaTransaction(test_data)
        mock = TestKavaPlugin.get_token_table_mock()
        caajs = KavaPlugin.get_caajs(
            "kava1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8m2splc", transaction, mock
        )

        caaj_withdraw = caajs[0]
        assert caaj_withdraw.executed_at == "2021-09-20 17:35:47"
        assert caaj_withdraw.chain == "kava"
        assert caaj_withdraw.platform == "kava"
        assert caaj_withdraw.application == "hard withdraw"
        assert (
            caaj_withdraw.transaction_id
            == "6D3C44020735DB474F6FA3AA1FF5CDFA2F6AF615A65E1FE8D20CF2246C1850B1"
        )
        assert caaj_withdraw.type == "withdraw"
        assert caaj_withdraw.amount == "1000"
        assert caaj_withdraw.token_symbol == "usdx"
        assert caaj_withdraw.token_original_id == "usdx"
        assert caaj_withdraw.caaj_from == "hard_lending"
        assert caaj_withdraw.caaj_to == "kava1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8m2splc"
        assert caaj_withdraw.comment == "hard withdraw 1000 usdx"

        # harvest_withdraw
        test_data = TestKavaPlugin._get_test_data("harvest_withdraw_v4")
        transaction = KavaTransaction(test_data)
        mock = TestKavaPlugin.get_token_table_mock()
        caajs = KavaPlugin.get_caajs(
            "kava1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8m2splc", transaction, mock
        )

        caaj_withdraw = caajs[0]
        assert caaj_withdraw.executed_at == "2020-12-23 01:43:10"
        assert caaj_withdraw.chain == "kava"
        assert caaj_withdraw.platform == "kava"
        assert caaj_withdraw.application == "hard withdraw"
        assert (
            caaj_withdraw.transaction_id
            == "DBA62D48CFBBF334129F879079E5BFD350D2379D5D6A9A1C9ADC4D9FD87BA665"
        )
        assert caaj_withdraw.type == "withdraw"
        assert caaj_withdraw.amount == "292.13977637"
        assert caaj_withdraw.token_symbol == "bnb"
        assert caaj_withdraw.token_original_id == "bnb"
        assert caaj_withdraw.caaj_from == "hard_lending"
        assert caaj_withdraw.caaj_to == "kava1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8m2splc"
        assert caaj_withdraw.comment == "hard withdraw 292.13977637 bnb"

    def test_hard_borrow(self):
        test_data = TestKavaPlugin._get_test_data("hard_borrow_v8")
        transaction = KavaTransaction(test_data)
        mock = TestKavaPlugin.get_token_table_mock()
        caajs = KavaPlugin.get_caajs(
            "kava1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8m2splc", transaction, mock
        )

        caaj_borrow = caajs[0]
        assert caaj_borrow.executed_at == "2021-09-21 00:51:36"
        assert caaj_borrow.chain == "kava"
        assert caaj_borrow.platform == "kava"
        assert caaj_borrow.application == "hard borrow"
        assert (
            caaj_borrow.transaction_id
            == "0CA2F41878E32422BF7E4851FCFCC0630F22F7A6AD202AC2BD6BC3171DCE5BDE"
        )
        assert caaj_borrow.type == "borrow"
        assert caaj_borrow.amount == "2637.78595858"
        assert caaj_borrow.token_symbol == "busd"
        assert caaj_borrow.token_original_id == "busd"
        assert caaj_borrow.caaj_from == "hard_lending"
        assert caaj_borrow.caaj_to == "kava1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8m2splc"
        assert caaj_borrow.comment == "hard borrow 2637.78595858 busd"

    def test_hard_repay(self):
        test_data = TestKavaPlugin._get_test_data("hard_repay_v8")
        transaction = KavaTransaction(test_data)
        mock = TestKavaPlugin.get_token_table_mock()
        caajs = KavaPlugin.get_caajs(
            "kava1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8m2splc", transaction, mock
        )

        caaj_repay = caajs[0]
        assert caaj_repay.executed_at == "2021-10-17 02:29:47"
        assert caaj_repay.chain == "kava"
        assert caaj_repay.platform == "kava"
        assert caaj_repay.application == "hard repay"
        assert (
            caaj_repay.transaction_id
            == "F396B8902DC6FBB2E781040C4C0E2DE442E49AFA94D761ACCE135865638C317A"
        )
        assert caaj_repay.type == "repay"
        assert caaj_repay.amount == "1956.12007376"
        assert caaj_repay.token_symbol == "busd"
        assert caaj_repay.token_original_id == "busd"
        assert caaj_repay.caaj_from == "kava1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8m2splc"
        assert caaj_repay.caaj_to == "hard_lending"
        assert caaj_repay.comment == "hard repay 1956.12007376 busd"

    def test_claim_hard_reward(self):
        test_data = TestKavaPlugin._get_test_data("claim_hard_reward_v7")
        transaction = KavaTransaction(test_data)
        mock = TestKavaPlugin.get_token_table_mock()
        caajs = KavaPlugin.get_caajs(
            "kava1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8m2splc", transaction, mock
        )

        caaj_reward = caajs[0]
        assert caaj_reward.executed_at == "2021-08-10 16:27:40"
        assert caaj_reward.chain == "kava"
        assert caaj_reward.platform == "kava"
        assert caaj_reward.application == "claim hard reward"
        assert (
            caaj_reward.transaction_id
            == "0776DF1721C58ACC57DD620804506F5E8E94550AE07CD0ECC4153C50C8F60AB3"
        )
        assert caaj_reward.type == "get"
        assert caaj_reward.amount == "14.418679"
        assert caaj_reward.token_symbol == "hard"
        assert caaj_reward.token_original_id == "hard"
        assert caaj_reward.caaj_from == "hard_lending"
        assert caaj_reward.caaj_to == "kava1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8m2splc"
        assert caaj_reward.comment == "hard lending reward receive 14.418679 hard"

        # claim_harvest_reward
        test_data = TestKavaPlugin._get_test_data("claim_harvest_reward_v4")
        transaction = KavaTransaction(test_data)
        mock = TestKavaPlugin.get_token_table_mock()
        caajs = KavaPlugin.get_caajs(
            "kava1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8m2splc", transaction, mock
        )

        caaj_reward = caajs[0]
        assert caaj_reward.executed_at == "2021-02-28 03:47:38"
        assert caaj_reward.chain == "kava"
        assert caaj_reward.platform == "kava"
        assert caaj_reward.application == "claim hard reward"
        assert (
            caaj_reward.transaction_id
            == "FE2235FD5CE5B962C8975E0D8E4180CD1A9A5A56C161E4940EB144B9A7752514"
        )
        assert caaj_reward.type == "get"
        assert caaj_reward.amount == "23.99439"
        assert caaj_reward.token_symbol == "hard"
        assert caaj_reward.token_original_id == "hard"
        assert caaj_reward.caaj_from == "hard_lending"
        assert caaj_reward.caaj_to == "kava1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8m2splc"
        assert caaj_reward.comment == "hard lending reward receive 23.99439 hard"

    def test_swap_exact_for_tokens(self):
        test_data = TestKavaPlugin._get_test_data("swap_exact_for_tokens_v8")
        transaction = KavaTransaction(test_data)
        mock = TestKavaPlugin.get_token_table_mock()
        caajs = KavaPlugin.get_caajs(
            "kava1mdm5595gw7n2yrfa6fjdrk2xwzn4njkj2akvq4", transaction, mock
        )

        assert caajs[0].trade_uuid == caajs[1].trade_uuid
        assert caajs[0].trade_uuid == caajs[2].trade_uuid
        assert caajs[0].trade_uuid == caajs[3].trade_uuid

        caaj_swap_input = caajs[0]
        assert caaj_swap_input.executed_at == "2021-09-09 04:41:25"
        assert caaj_swap_input.chain == "kava"
        assert caaj_swap_input.platform == "kava"
        assert caaj_swap_input.application == "swap exact for tokens"
        assert (
            caaj_swap_input.transaction_id
            == "4B226210F5E04A54E79931ECF5C1D32F2FD9F45139C0DC1C3A0481532AE60448"
        )
        assert caaj_swap_input.type == "lose"
        assert caaj_swap_input.amount == "0.03"
        assert caaj_swap_input.token_symbol == "bnb"
        assert caaj_swap_input.token_original_id == "bnb"
        assert (
            caaj_swap_input.caaj_from == "kava1mdm5595gw7n2yrfa6fjdrk2xwzn4njkj2akvq4"
        )
        assert caaj_swap_input.caaj_to == "kava_swap"
        assert caaj_swap_input.comment == "buy 12.290319 usdx sell 0.03 bnb"

        caaj_swap_output = caajs[1]
        assert caaj_swap_output.executed_at == "2021-09-09 04:41:25"
        assert caaj_swap_output.chain == "kava"
        assert caaj_swap_output.platform == "kava"
        assert caaj_swap_output.application == "swap exact for tokens"
        assert (
            caaj_swap_output.transaction_id
            == "4B226210F5E04A54E79931ECF5C1D32F2FD9F45139C0DC1C3A0481532AE60448"
        )
        assert caaj_swap_output.type == "get"
        assert caaj_swap_output.amount == "12.290319"
        assert caaj_swap_output.token_symbol == "usdx"
        assert caaj_swap_output.token_original_id == "usdx"
        assert caaj_swap_output.caaj_from == "kava_swap"
        assert caaj_swap_output.caaj_to == "kava1mdm5595gw7n2yrfa6fjdrk2xwzn4njkj2akvq4"
        assert caaj_swap_output.comment == "buy 12.290319 usdx sell 0.03 bnb"

        caaj_swap_fee = caajs[2]
        assert caaj_swap_fee.executed_at == "2021-09-09 04:41:25"
        assert caaj_swap_fee.chain == "kava"
        assert caaj_swap_fee.platform == "kava"
        assert caaj_swap_fee.application == "swap exact for tokens"
        assert (
            caaj_swap_fee.transaction_id
            == "4B226210F5E04A54E79931ECF5C1D32F2FD9F45139C0DC1C3A0481532AE60448"
        )
        assert caaj_swap_fee.type == "lose"
        assert caaj_swap_fee.amount == "0.000045"
        assert caaj_swap_fee.token_symbol == "bnb"
        assert caaj_swap_fee.token_original_id == "bnb"
        assert caaj_swap_fee.caaj_from == "kava1mdm5595gw7n2yrfa6fjdrk2xwzn4njkj2akvq4"
        assert caaj_swap_fee.caaj_to == "kava_swap"
        assert caaj_swap_fee.comment == "pay 0.000045 bnb as swap fee"

        # swap_for_exact_tokens
        test_data = TestKavaPlugin._get_test_data("swap_for_exact_tokens_v8")
        transaction = KavaTransaction(test_data)
        mock = TestKavaPlugin.get_token_table_mock()
        caajs = KavaPlugin.get_caajs(
            "kava1tnxjszq48g2k737920cchjqwccrqav053c26l0", transaction, mock
        )

        assert caajs[0].trade_uuid == caajs[1].trade_uuid
        assert caajs[0].trade_uuid == caajs[2].trade_uuid

        caaj_swap_input = caajs[0]
        assert caaj_swap_input.executed_at == "2021-09-22 07:08:13"
        assert caaj_swap_input.chain == "kava"
        assert caaj_swap_input.platform == "kava"
        assert caaj_swap_input.application == "swap exact for tokens"
        assert (
            caaj_swap_input.transaction_id
            == "8ACAFD81B28D6BE5630EBC22AD4A382DAB54DD63BF4D86F873F1DC27D9EF0039"
        )
        assert caaj_swap_input.type == "lose"
        assert caaj_swap_input.amount == "13987.92220598"
        assert caaj_swap_input.token_symbol == "busd"
        assert caaj_swap_input.token_original_id == "busd"
        assert (
            caaj_swap_input.caaj_from == "kava1tnxjszq48g2k737920cchjqwccrqav053c26l0"
        )
        assert caaj_swap_input.caaj_to == "kava_swap"
        assert caaj_swap_input.comment == "buy 14238.68 usdx sell 13987.92220598 busd"

        caaj_swap_output = caajs[1]
        assert caaj_swap_output.executed_at == "2021-09-22 07:08:13"
        assert caaj_swap_output.chain == "kava"
        assert caaj_swap_output.platform == "kava"
        assert caaj_swap_output.application == "swap exact for tokens"
        assert (
            caaj_swap_output.transaction_id
            == "8ACAFD81B28D6BE5630EBC22AD4A382DAB54DD63BF4D86F873F1DC27D9EF0039"
        )
        assert caaj_swap_output.type == "get"
        assert caaj_swap_output.amount == "14238.68"
        assert caaj_swap_output.token_symbol == "usdx"
        assert caaj_swap_output.token_original_id == "usdx"
        assert caaj_swap_output.caaj_from == "kava_swap"
        assert caaj_swap_output.caaj_to == "kava1tnxjszq48g2k737920cchjqwccrqav053c26l0"
        assert caaj_swap_output.comment == "buy 14238.68 usdx sell 13987.92220598 busd"

        caaj_swap_fee = caajs[2]
        assert caaj_swap_fee.executed_at == "2021-09-22 07:08:13"
        assert caaj_swap_fee.chain == "kava"
        assert caaj_swap_fee.platform == "kava"
        assert caaj_swap_fee.application == "swap exact for tokens"
        assert (
            caaj_swap_fee.transaction_id
            == "8ACAFD81B28D6BE5630EBC22AD4A382DAB54DD63BF4D86F873F1DC27D9EF0039"
        )
        assert caaj_swap_fee.type == "lose"
        assert caaj_swap_fee.amount == "20.98188331"
        assert caaj_swap_fee.token_symbol == "busd"
        assert caaj_swap_fee.token_original_id == "busd"
        assert caaj_swap_fee.caaj_from == "kava1tnxjszq48g2k737920cchjqwccrqav053c26l0"
        assert caaj_swap_fee.caaj_to == "kava_swap"
        assert caaj_swap_fee.comment == "pay 20.98188331 busd as swap fee"

    def test_swap_deposit(self):
        test_data = TestKavaPlugin._get_test_data("swap_deposit_v8")
        transaction = KavaTransaction(test_data)
        mock = TestKavaPlugin.get_token_table_mock()
        caajs = KavaPlugin.get_caajs(
            "kava1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8m2splc", transaction, mock
        )

        assert caajs[0].trade_uuid == caajs[1].trade_uuid
        assert caajs[0].trade_uuid == caajs[2].trade_uuid

        caaj_get_bonds = caajs[0]
        assert caaj_get_bonds.executed_at == "2021-08-30 17:54:42"
        assert caaj_get_bonds.chain == "kava"
        assert caaj_get_bonds.platform == "kava"
        assert caaj_get_bonds.application == "swap deposit"
        assert (
            caaj_get_bonds.transaction_id
            == "425BE10B2E9A6358CE30251C51FE910E3A4CA45F2928F3AC26F0A5C1C57576C8"
        )
        assert caaj_get_bonds.type == "get_bonds"
        assert caaj_get_bonds.amount == "19155352120"
        assert caaj_get_bonds.token_symbol == "busd:usdx"
        assert caaj_get_bonds.token_original_id == "busd:usdx"
        assert caaj_get_bonds.caaj_from == "kava_swap"
        assert caaj_get_bonds.caaj_to == "kava1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8m2splc"
        assert caaj_get_bonds.comment == "kava swap receive 19155352120 busd:usdx"

        caaj_deposit = caajs[1]
        assert caaj_deposit.executed_at == "2021-08-30 17:54:42"
        assert caaj_deposit.chain == "kava"
        assert caaj_deposit.platform == "kava"
        assert caaj_deposit.application == "swap deposit"
        assert (
            caaj_deposit.transaction_id
            == "425BE10B2E9A6358CE30251C51FE910E3A4CA45F2928F3AC26F0A5C1C57576C8"
        )
        assert caaj_deposit.type == "deposit"
        assert caaj_deposit.amount == "1914.40274498"
        assert caaj_deposit.token_symbol == "busd"
        assert caaj_deposit.token_original_id == "busd"
        assert caaj_deposit.caaj_from == "kava1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8m2splc"
        assert caaj_deposit.caaj_to == "kava_swap"
        assert caaj_deposit.comment == "kava swap send 1914.40274498 busd"

        caaj_deposit = caajs[2]
        assert caaj_deposit.executed_at == "2021-08-30 17:54:42"
        assert caaj_deposit.chain == "kava"
        assert caaj_deposit.platform == "kava"
        assert caaj_deposit.application == "swap deposit"
        assert (
            caaj_deposit.transaction_id
            == "425BE10B2E9A6358CE30251C51FE910E3A4CA45F2928F3AC26F0A5C1C57576C8"
        )
        assert caaj_deposit.type == "deposit"
        assert caaj_deposit.amount == "1918.51883"
        assert caaj_deposit.token_symbol == "usdx"
        assert caaj_deposit.token_original_id == "usdx"
        assert caaj_deposit.caaj_from == "kava1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8m2splc"
        assert caaj_deposit.caaj_to == "kava_swap"
        assert caaj_deposit.comment == "kava swap send 1918.51883 usdx"

    def test_swap_withdraw(self):
        test_data = TestKavaPlugin._get_test_data("swap_withdraw_v8")
        transaction = KavaTransaction(test_data)
        mock = TestKavaPlugin.get_token_table_mock()
        caajs = KavaPlugin.get_caajs(
            "kava1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8m2splc", transaction, mock
        )

        assert caajs[0].trade_uuid == caajs[1].trade_uuid
        assert caajs[0].trade_uuid == caajs[2].trade_uuid

        caaj_get_bonds = caajs[0]
        assert caaj_get_bonds.executed_at == "2021-08-30 17:52:20"
        assert caaj_get_bonds.chain == "kava"
        assert caaj_get_bonds.platform == "kava"
        assert caaj_get_bonds.application == "swap withdraw"
        assert (
            caaj_get_bonds.transaction_id
            == "AF18E27F1426740A1CA0704C13BC65C1B8AF21A739B570C7B6EC1EA8380453DD"
        )
        assert caaj_get_bonds.type == "lose_bonds"
        assert caaj_get_bonds.amount == "655345546"
        assert caaj_get_bonds.token_symbol == "swp:usdx"
        assert caaj_get_bonds.token_original_id == "swp:usdx"
        assert caaj_get_bonds.caaj_from == "kava1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8m2splc"
        assert caaj_get_bonds.caaj_to == "kava_swap"
        assert caaj_get_bonds.comment == "kava swap send 655345546 swp:usdx"

        caaj_withdraw = caajs[1]
        assert caaj_withdraw.executed_at == "2021-08-30 17:52:20"
        assert caaj_withdraw.chain == "kava"
        assert caaj_withdraw.platform == "kava"
        assert caaj_withdraw.application == "swap withdraw"
        assert (
            caaj_withdraw.transaction_id
            == "AF18E27F1426740A1CA0704C13BC65C1B8AF21A739B570C7B6EC1EA8380453DD"
        )
        assert caaj_withdraw.type == "withdraw"
        assert caaj_withdraw.amount == "510.54504"
        assert caaj_withdraw.token_symbol == "swp"
        assert caaj_withdraw.token_original_id == "swp"
        assert caaj_withdraw.caaj_from == "kava_swap"
        assert caaj_withdraw.caaj_to == "kava1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8m2splc"
        assert caaj_withdraw.comment == "kava swap receive 510.54504 swp"

        caaj_withdraw = caajs[2]
        assert caaj_withdraw.executed_at == "2021-08-30 17:52:20"
        assert caaj_withdraw.chain == "kava"
        assert caaj_withdraw.platform == "kava"
        assert caaj_withdraw.application == "swap withdraw"
        assert (
            caaj_withdraw.transaction_id
            == "AF18E27F1426740A1CA0704C13BC65C1B8AF21A739B570C7B6EC1EA8380453DD"
        )
        assert caaj_withdraw.type == "withdraw"
        assert caaj_withdraw.amount == "844.628983"
        assert caaj_withdraw.token_symbol == "usdx"
        assert caaj_withdraw.token_original_id == "usdx"
        assert caaj_withdraw.caaj_from == "kava_swap"
        assert caaj_withdraw.caaj_to == "kava1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8m2splc"
        assert caaj_withdraw.comment == "kava swap receive 844.628983 usdx"

    def test_claim_swap_reward(self):
        test_data = TestKavaPlugin._get_test_data("claim_swap_reward_v8")
        transaction = KavaTransaction(test_data)
        mock = TestKavaPlugin.get_token_table_mock()
        caajs = KavaPlugin.get_caajs(
            "kava1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8m2splc", transaction, mock
        )

        caaj_reward = caajs[0]
        assert caaj_reward.executed_at == "2021-09-15 13:05:53"
        assert caaj_reward.chain == "kava"
        assert caaj_reward.platform == "kava"
        assert caaj_reward.application == "claim swap reward"
        assert (
            caaj_reward.transaction_id
            == "C2D830944FCD1B98DF6D6939BA6367047C8B76D8DC40888332D5C658DFFAA06E"
        )
        assert caaj_reward.type == "get"
        assert caaj_reward.amount == "830.379251"
        assert caaj_reward.token_symbol == "swp"
        assert caaj_reward.token_original_id == "swp"
        assert caaj_reward.caaj_from == "kava_swap"
        assert caaj_reward.caaj_to == "kava1jv65s3grqf6v6jl3dp4t6c9t9rk99cd8m2splc"
        assert caaj_reward.comment == "kava swap reward receive 830.379251 swp"

    def test_send(self):
        # recipient
        test_data = TestKavaPlugin._get_test_data("send_v2")
        transaction = KavaTransaction(test_data)
        mock = TestKavaPlugin.get_token_table_mock()
        caajs = KavaPlugin.get_caajs(
            "kava1nzq60hrphyr8anvkw6fv93mhafew7ez4tq9ahv", transaction, mock
        )

        caaj_send = caajs[0]
        assert caaj_send.executed_at == "2020-04-01 07:40:01"
        assert caaj_send.chain == "kava"
        assert caaj_send.platform == "kava"
        assert caaj_send.application == "send"
        assert (
            caaj_send.transaction_id
            == "A7C892540487D8E488338F84F9C48A6FAC60870A0E609CB6E5F5907B6DF78636"
        )
        assert caaj_send.type == "receive"
        assert caaj_send.amount == "13.5"
        assert caaj_send.token_symbol == "kava"
        assert caaj_send.token_original_id is None
        assert caaj_send.caaj_from == "kava1k760ypy9tzhp6l2rmg06sq4n74z0d3relc549c"
        assert caaj_send.caaj_to == "kava1nzq60hrphyr8anvkw6fv93mhafew7ez4tq9ahv"
        assert (
            caaj_send.comment
            == "kava1nzq60hrphyr8anvkw6fv93mhafew7ez4tq9ahv receive 13.5 kava from kava1k760ypy9tzhp6l2rmg06sq4n74z0d3relc549c"
        )

        # sender
        test_data = TestKavaPlugin._get_test_data("send_v8")
        transaction = KavaTransaction(test_data)
        mock = TestKavaPlugin.get_token_table_mock()
        caajs = KavaPlugin.get_caajs(
            "kava1dlezgt8undlpvdp0esmzyvxzvc59gkd56vkmea", transaction, mock
        )

        caaj_send = caajs[0]
        assert caaj_send.executed_at == "2021-10-16 01:26:47"
        assert caaj_send.chain == "kava"
        assert caaj_send.platform == "kava"
        assert caaj_send.application == "send"
        assert (
            caaj_send.transaction_id
            == "C1973EA66796DEFBA950B2A79E9E8118EB0DC4E68E16EFD76E507FC18B1E0DDF"
        )
        assert caaj_send.type == "send"
        assert caaj_send.amount == "2.17"
        assert caaj_send.token_symbol == "kava"
        assert caaj_send.token_original_id is None
        assert caaj_send.caaj_from == "kava1dlezgt8undlpvdp0esmzyvxzvc59gkd56vkmea"
        assert caaj_send.caaj_to == "kava1ys70jvnajkv88529ys6urjcyle3k2j9r24g6a7"
        assert (
            caaj_send.comment
            == "kava1dlezgt8undlpvdp0esmzyvxzvc59gkd56vkmea send 2.17 kava to kava1ys70jvnajkv88529ys6urjcyle3k2j9r24g6a7"
        )

    def test_create_atomic_swap(self):
        # recipient
        test_data = TestKavaPlugin._get_test_data("createAtomicSwap_v3")
        transaction = KavaTransaction(test_data)
        mock = TestKavaPlugin.get_token_table_mock()
        caajs = KavaPlugin.get_caajs(
            "kava1eyugkwc74zejgwdwl7mvm7pad4hzdnka4wmdmu", transaction, mock
        )

        caaj_create_atomic_swap = caajs[0]
        assert caaj_create_atomic_swap.executed_at == "2020-09-08 05:34:28"
        assert caaj_create_atomic_swap.chain == "kava"
        assert caaj_create_atomic_swap.platform == "kava"
        assert caaj_create_atomic_swap.application == "create atomic swap"
        assert (
            caaj_create_atomic_swap.transaction_id
            == "39E4B07236B3A54208D57EF643F4343B1C1B5ACB5326719B4BB4DE3B505EA2F7"
        )
        assert caaj_create_atomic_swap.type == "receive"
        assert caaj_create_atomic_swap.amount == "0.1995"
        assert caaj_create_atomic_swap.token_symbol == "bnb"
        assert caaj_create_atomic_swap.token_original_id == "bnb"
        assert caaj_create_atomic_swap.caaj_from == "kava_bc_atomic_swap"
        assert (
            caaj_create_atomic_swap.caaj_to
            == "kava1eyugkwc74zejgwdwl7mvm7pad4hzdnka4wmdmu"
        )
        assert (
            caaj_create_atomic_swap.comment
            == "kava1eyugkwc74zejgwdwl7mvm7pad4hzdnka4wmdmu receive 0.1995 bnb from kava_bc_atomic_swap"
        )

        # sender
        test_data = TestKavaPlugin._get_test_data("createAtomicSwap_v8")
        transaction = KavaTransaction(test_data)
        mock = TestKavaPlugin.get_token_table_mock()
        caajs = KavaPlugin.get_caajs(
            "kava1mdm5595gw7n2yrfa6fjdrk2xwzn4njkj2akvq4", transaction, mock
        )

        caaj_create_atomic_swap = caajs[0]
        assert caaj_create_atomic_swap.executed_at == "2021-09-26 12:28:30"
        assert caaj_create_atomic_swap.chain == "kava"
        assert caaj_create_atomic_swap.platform == "kava"
        assert caaj_create_atomic_swap.application == "create atomic swap"
        assert (
            caaj_create_atomic_swap.transaction_id
            == "4CB6BA6CA99CFB051C1DC1BB9EFBD23D0B8DB4DF884CE687161B7CF1CF46BB7B"
        )
        assert caaj_create_atomic_swap.type == "send"
        assert caaj_create_atomic_swap.amount == "1.33428994"
        assert caaj_create_atomic_swap.token_symbol == "bnb"
        assert caaj_create_atomic_swap.token_original_id == "bnb"
        assert (
            caaj_create_atomic_swap.caaj_from
            == "kava1mdm5595gw7n2yrfa6fjdrk2xwzn4njkj2akvq4"
        )
        assert caaj_create_atomic_swap.caaj_to == "kava_bc_atomic_swap"
        assert (
            caaj_create_atomic_swap.comment
            == "kava1mdm5595gw7n2yrfa6fjdrk2xwzn4njkj2akvq4 send 1.33428994 bnb to kava_bc_atomic_swap"
        )

        # v9
        test_data = TestKavaPlugin._get_test_data("createAtomicSwap_v9")
        transaction = KavaTransaction(test_data)
        mock = TestKavaPlugin.get_token_table_mock()
        caajs = KavaPlugin.get_caajs(
            "kava1af7lm2qv9zp526gjd3cdxrpr9zeangjlyhjqjx", transaction, mock
        )

        caaj_create_atomic_swap = caajs[0]
        assert caaj_create_atomic_swap.executed_at == "2022-01-27 02:23:59"
        assert caaj_create_atomic_swap.chain == "kava"
        assert caaj_create_atomic_swap.platform == "kava"
        assert caaj_create_atomic_swap.application == "create atomic swap"
        assert (
            caaj_create_atomic_swap.transaction_id
            == "9AA55DD057F91432FC1F5ABCF34D009BA680F05014FED09DD08B14E67E25D982"
        )
        assert caaj_create_atomic_swap.type == "send"
        assert caaj_create_atomic_swap.amount == "310113.74719552"
        assert caaj_create_atomic_swap.token_symbol == "busd"
        assert caaj_create_atomic_swap.token_original_id == "busd"
        assert (
            caaj_create_atomic_swap.caaj_from
            == "kava1af7lm2qv9zp526gjd3cdxrpr9zeangjlyhjqjx"
        )
        assert caaj_create_atomic_swap.caaj_to == "kava_bc_atomic_swap"
        assert (
            caaj_create_atomic_swap.comment
            == "kava1af7lm2qv9zp526gjd3cdxrpr9zeangjlyhjqjx send 310113.74719552 busd to kava_bc_atomic_swap"
        )

        # claimAtomicSwap
        test_data = TestKavaPlugin._get_test_data("claimAtomicSwap_v4")
        transaction = KavaTransaction(test_data)
        mock = TestKavaPlugin.get_token_table_mock()
        caajs = KavaPlugin.get_caajs(
            "kava1nzq60hrphyr8anvkw6fv93mhafew7ez4tq9ahv", transaction, mock
        )

        caaj_create_atomic_swap = caajs[0]
        assert caaj_create_atomic_swap.executed_at == "2020-11-10 13:42:52"
        assert caaj_create_atomic_swap.chain == "kava"
        assert caaj_create_atomic_swap.platform == "kava"
        assert caaj_create_atomic_swap.application == "create atomic swap"
        assert (
            caaj_create_atomic_swap.transaction_id
            == "CCAC9B017C7704383CBA089C071798BE38233DEE59C7D75BF8AB2432F5F42363"
        )
        assert caaj_create_atomic_swap.type == "receive"
        assert caaj_create_atomic_swap.amount == "99.889"
        assert caaj_create_atomic_swap.token_symbol == "xrp"
        assert caaj_create_atomic_swap.token_original_id == "xrp"
        assert caaj_create_atomic_swap.caaj_from == "kava_bc_atomic_swap"
        assert (
            caaj_create_atomic_swap.caaj_to
            == "kava1nzq60hrphyr8anvkw6fv93mhafew7ez4tq9ahv"
        )
        assert (
            caaj_create_atomic_swap.comment
            == "kava1nzq60hrphyr8anvkw6fv93mhafew7ez4tq9ahv receive 99.889 xrp from kava_bc_atomic_swap"
        )

        # refundAtomicSwap
        test_data = TestKavaPlugin._get_test_data("refundAtomicSwap_v6")
        transaction = KavaTransaction(test_data)
        mock = TestKavaPlugin.get_token_table_mock()
        caajs = KavaPlugin.get_caajs(
            "kava1eyugkwc74zejgwdwl7mvm7pad4hzdnka4wmdmu", transaction, mock
        )

        caaj_create_atomic_swap = caajs[0]
        assert caaj_create_atomic_swap.executed_at == "2021-04-06 23:00:17"
        assert caaj_create_atomic_swap.chain == "kava"
        assert caaj_create_atomic_swap.platform == "kava"
        assert caaj_create_atomic_swap.application == "create atomic swap"
        assert (
            caaj_create_atomic_swap.transaction_id
            == "C027244153F6ECB58D2786C307EB32514ACDEA1729F13B1310BA2E6A1D15BFB"
        )
        assert caaj_create_atomic_swap.type == "send"
        assert caaj_create_atomic_swap.amount == "500"
        assert caaj_create_atomic_swap.token_symbol == "bnb"
        assert caaj_create_atomic_swap.token_original_id == "bnb"
        assert (
            caaj_create_atomic_swap.caaj_from
            == "kava1eyugkwc74zejgwdwl7mvm7pad4hzdnka4wmdmu"
        )
        assert caaj_create_atomic_swap.caaj_to == "kava_bc_atomic_swap"
        assert (
            caaj_create_atomic_swap.comment
            == "kava1eyugkwc74zejgwdwl7mvm7pad4hzdnka4wmdmu send 500 bnb to kava_bc_atomic_swap"
        )

    def test_vote(self):
        test_data = TestKavaPlugin._get_test_data("vote_v8")
        transaction = KavaTransaction(test_data)
        mock = TestKavaPlugin.get_token_table_mock()
        caajs = KavaPlugin.get_caajs(
            "kava179ahnk902wgm7qzr66t5ga0a8euc28ce703jy3", transaction, mock
        )

        caaj_transaction_fee = caajs[0]
        assert caaj_transaction_fee.executed_at == "2021-10-02 01:35:37"
        assert caaj_transaction_fee.chain == "kava"
        assert caaj_transaction_fee.platform == "kava"
        assert caaj_transaction_fee.application == "kava"
        assert (
            caaj_transaction_fee.transaction_id
            == "C1FC796FCD2901530F87B5FA2BBA3FCBA8CF16D7C22226C8E910C240CF3BA930"
        )
        assert caaj_transaction_fee.type == "lose"
        assert caaj_transaction_fee.amount == "0.00075"
        assert caaj_transaction_fee.token_symbol == "kava"
        assert caaj_transaction_fee.token_original_id is None
        assert (
            caaj_transaction_fee.caaj_from
            == "kava179ahnk902wgm7qzr66t5ga0a8euc28ce703jy3"
        )
        assert caaj_transaction_fee.caaj_to == "fee"
        assert caaj_transaction_fee.comment == ""

    def test_fail(self):
        test_data = TestKavaPlugin._get_test_data("fail_v8")
        transaction = KavaTransaction(test_data)
        mock = TestKavaPlugin.get_token_table_mock()
        caajs = KavaPlugin.get_caajs(
            "kava179ahnk902wgm7qzr66t5ga0a8euc28ce703jy3", transaction, mock
        )

        caaj_transaction_fee = caajs[0]
        assert caaj_transaction_fee.executed_at == "2021-10-08 02:25:45"
        assert caaj_transaction_fee.chain == "kava"
        assert caaj_transaction_fee.platform == "kava"
        assert caaj_transaction_fee.application == "kava"
        assert (
            caaj_transaction_fee.transaction_id
            == "F2FC1DF24185A9909DF3E3BC87726AAF1440F94FE2BE1633F0A7BAA2E09B069F"
        )
        assert caaj_transaction_fee.type == "lose"
        assert caaj_transaction_fee.amount == "0.01"
        assert caaj_transaction_fee.token_symbol == "kava"
        assert caaj_transaction_fee.token_original_id is None
        assert (
            caaj_transaction_fee.caaj_from
            == "kava179ahnk902wgm7qzr66t5ga0a8euc28ce703jy3"
        )
        assert caaj_transaction_fee.caaj_to == "fee"
        assert caaj_transaction_fee.comment == ""

    @classmethod
    def _get_test_data(cls, filename):
        with open(f"tests/data/{filename}.json", encoding="utf-8") as jsonfile_local:
            test_data = json.load(jsonfile_local)
        return test_data

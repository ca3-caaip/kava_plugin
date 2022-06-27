import json
import unittest

from senkalib.platform.kava.kava_transaction import KavaTransaction

from kava_plugin.message_factory import MessageFactory


class TestMessage(unittest.TestCase):
    """verify get_caaj works fine"""

    def test_get_result(self):
        for action_dict in [{"action": "delegate", "version": "v8", "msg_index": 0}]:
            action = action_dict["action"]
            version = action_dict["version"]
            result = TestMessage._get_test_data_messages_result(f"{action}_{version}")
            self.assertEqual(result["action"], action)

    def test_as_withdraw_delegator_reward(self):
        result = TestMessage._get_test_data_messages_result(
            "withdraw_delegator_reward_v8"
        )
        self.assertEqual(result["action"], "delegate")
        self.assertEqual(
            result["result"]["rewards"][0],
            {"reward_token": "kava", "reward_amount": "1.298035"},
        )

    def test_as_delegate(self):
        result = TestMessage._get_test_data_messages_result("delegate_v8")
        self.assertEqual(result["action"], "delegate")
        self.assertEqual(result["result"]["staking_token"], "kava")
        self.assertEqual(result["result"]["staking_amount"], "0.00118")
        self.assertEqual(
            result["result"]["rewards"][0],
            {"reward_token": "kava", "reward_amount": "0.000039"},
        )

        result = TestMessage._get_test_data_messages_result("begin_redelegate_v8")
        self.assertEqual(result["action"], "delegate")
        self.assertEqual(result["result"]["staking_token"], None)
        self.assertEqual(result["result"]["staking_amount"], None)
        self.assertEqual(
            result["result"]["rewards"][0],
            {"reward_token": "kava", "reward_amount": "3.687213"},
        )

    def test_begin_unbonding(self):
        result = TestMessage._get_test_data_messages_result("begin_unbonding_v7")
        self.assertEqual(result["action"], "begin_unbonding")
        self.assertEqual(result["result"]["unbonding_token"], "kava")
        self.assertEqual(result["result"]["unbonding_amount"], "343.546602")
        self.assertEqual(
            result["result"]["rewards"][0],
            {"reward_token": "kava", "reward_amount": "0.001703"},
        )

    def test_hard_deposit(self):
        result = TestMessage._get_test_data_messages_result("hard_deposit_v8")
        self.assertEqual(result["action"], "hard_deposit")
        self.assertEqual(result["result"]["hard_deposit_token"], "kava")
        self.assertEqual(result["result"]["hard_deposit_amount"], "1513.591717")

        result = TestMessage._get_test_data_messages_result("harvest_deposit_v4")
        self.assertEqual(result["action"], "hard_deposit")
        self.assertEqual(result["result"]["hard_deposit_token"], "usdx")
        self.assertEqual(result["result"]["hard_deposit_amount"], "3610.692343")

    def test_hard_withdraw(self):
        result = TestMessage._get_test_data_messages_result("hard_withdraw_v8")
        self.assertEqual(result["action"], "hard_withdraw")
        self.assertEqual(result["result"]["hard_withdraw_token"], "usdx")
        self.assertEqual(result["result"]["hard_withdraw_amount"], "1000")

        result = TestMessage._get_test_data_messages_result("harvest_withdraw_v4")
        self.assertEqual(result["action"], "hard_withdraw")
        self.assertEqual(result["result"]["hard_withdraw_token"], "bnb")
        self.assertEqual(result["result"]["hard_withdraw_amount"], "292.13977637")

    def test_hard_repay(self):
        result = TestMessage._get_test_data_messages_result("hard_repay_v8")
        self.assertEqual(result["action"], "hard_repay")
        self.assertEqual(result["result"]["hard_repay_token"], "busd")
        self.assertEqual(result["result"]["hard_repay_amount"], "1956.12007376")

    def test_hard_borrow(self):
        result = TestMessage._get_test_data_messages_result("hard_borrow_v8")
        self.assertEqual(result["action"], "hard_borrow")
        self.assertEqual(result["result"]["hard_borrow_token"], "busd")
        self.assertEqual(result["result"]["hard_borrow_amount"], "2637.78595858")

    def test_claim_hard_reward(self):
        result = TestMessage._get_test_data_messages_result("claim_hard_reward_v7")
        self.assertEqual(result["action"], "claim_hard_reward")
        self.assertEqual(
            result["result"]["rewards"][0],
            {"reward_token": "hard", "reward_amount": "14.418679"},
        )
        self.assertEqual(
            result["result"]["rewards"][1],
            {"reward_token": "kava", "reward_amount": "24.675275"},
        )

        result = TestMessage._get_test_data_messages_result("claim_harvest_reward_v4")
        self.assertEqual(result["action"], "claim_hard_reward")
        self.assertEqual(
            result["result"]["rewards"][0],
            {"reward_token": "hard", "reward_amount": "23.99439"},
        )

    def test_create_cdp(self):
        result = TestMessage._get_test_data_messages_result("create_cdp_v7")
        self.assertEqual(result["action"], "create_cdp")
        self.assertEqual(
            result["result"],
            {
                "deposit_token": "hard",
                "deposit_amount": "10093.653846",
                "draw_token": "usdx",
                "draw_amount": "3500",
            },
        )

    def test_draw_cdp(self):
        result = TestMessage._get_test_data_messages_result("draw_cdp_v7")
        self.assertEqual(result["action"], "draw_cdp")
        self.assertEqual(result["result"], {"draw_token": "usdx", "draw_amount": "300"})

    def test_repay_cdp(self):
        result = TestMessage._get_test_data_messages_result("repay_cdp_v8")
        self.assertEqual(result["action"], "repay_cdp")
        self.assertEqual(
            result["result"],
            {
                "repay_token": "usdx",
                "repay_amount": "10.050333",
                "withdraw_token": "bnb",
                "withdraw_amount": "0.36428994",
            },
        )

    def test_deposit_cdp(self):
        result = TestMessage._get_test_data_messages_result("deposit_cdp_v8")
        self.assertEqual(result["action"], "deposit_cdp")
        self.assertEqual(
            result["result"],
            {"deposit_token": "xrp", "deposit_amount": "5063.76309394"},
        )

    def test_withdraw_cdp(self):
        result = TestMessage._get_test_data_messages_result("withdraw_cdp_v8")
        self.assertEqual(result["action"], "withdraw_cdp")
        self.assertEqual(
            result["result"], {"withdraw_token": "bnb", "withdraw_amount": "1"}
        )

    def test_as_claim_usdx_minting_reward(self):
        result = TestMessage._get_test_data_messages_result(
            "claim_usdx_minting_reward_v7"
        )
        self.assertEqual(result["action"], "claim_usdx_minting_reward")
        self.assertEqual(
            result["result"]["rewards"][0],
            {"reward_token": "kava", "reward_amount": "3.746212"},
        )

        result = TestMessage._get_test_data_messages_result("claim_reward_v6")
        self.assertEqual(result["action"], "claim_usdx_minting_reward")
        self.assertEqual(
            result["result"]["rewards"][0],
            {"reward_token": "kava", "reward_amount": "0.293872"},
        )

    def test_createAtomicSwap(self):
        result = TestMessage._get_test_data_messages_result("createAtomicSwap_v8")
        self.assertEqual(result["action"], "create_atomic_swap")
        self.assertEqual(
            result["result"],
            {
                "sender": "kava1mdm5595gw7n2yrfa6fjdrk2xwzn4njkj2akvq4",
                "recipient": "kava1eyugkwc74zejgwdwl7mvm7pad4hzdnka4wmdmu",
                "token": "bnb",
                "amount": "1.33428994",
            },
        )

        result = TestMessage._get_test_data_messages_result("createAtomicSwap_v3")
        self.assertEqual(result["action"], "create_atomic_swap")
        self.assertEqual(
            result["result"],
            {
                "sender": "kava1x8cy4tfcxzywqwenttjswlv6x8swhc6hz2xfxq",
                "recipient": "kava1eyugkwc74zejgwdwl7mvm7pad4hzdnka4wmdmu",
                "token": "bnb",
                "amount": "0.1995",
            },
        )

        result = TestMessage._get_test_data_messages_result("createAtomicSwap_v9")
        self.assertEqual(result["action"], "create_atomic_swap")
        self.assertEqual(
            result["result"],
            {
                "sender": "kava1af7lm2qv9zp526gjd3cdxrpr9zeangjlyhjqjx",
                "recipient": "kava1eyugkwc74zejgwdwl7mvm7pad4hzdnka4wmdmu",
                "token": "busd",
                "amount": "310113.74719552",
            },
        )

    def test_claimAtomicSwap(self):
        result = TestMessage._get_test_data_messages_result("claimAtomicSwap_v4")
        self.assertEqual(result["action"], "claim_atomic_swap")
        self.assertEqual(
            result["result"],
            {
                "sender": "kava1eyugkwc74zejgwdwl7mvm7pad4hzdnka4wmdmu",
                "recipient": "kava1nzq60hrphyr8anvkw6fv93mhafew7ez4tq9ahv",
                "token": "xrp",
                "amount": "99.889",
            },
        )

        result = TestMessage._get_test_data_messages_result("refundAtomicSwap_v6")
        self.assertEqual(result["action"], "claim_atomic_swap")
        self.assertEqual(
            result["result"],
            {
                "sender": "kava1eyugkwc74zejgwdwl7mvm7pad4hzdnka4wmdmu",
                "recipient": "kava1dfg9r2n12m9abhet34k3xtju9vbndkuieqlojg",
                "token": "bnb",
                "amount": "500",
            },
        )

    def test_swap_exact_for_tokens(self):
        result = TestMessage._get_test_data_messages_result("swap_exact_for_tokens_v8")
        self.assertEqual(result["action"], "swap_exact_for_tokens")
        self.assertEqual(
            result["result"],
            {
                "input_token": "bnb",
                "input_amount": "0.03",
                "output_token": "usdx",
                "output_amount": "12.290319",
                "fee_token": "bnb",
                "fee_amount": "0.000045",
            },
        )

        result = TestMessage._get_test_data_messages_result("swap_for_exact_tokens_v8")
        self.assertEqual(result["action"], "swap_exact_for_tokens")
        self.assertEqual(
            result["result"],
            {
                "input_token": "busd",
                "input_amount": "13987.92220598",
                "output_token": "usdx",
                "output_amount": "14238.68",
                "fee_token": "busd",
                "fee_amount": "20.98188331",
            },
        )

    def test_swap_deposit(self):
        result = TestMessage._get_test_data_messages_result("swap_deposit_v8")
        self.assertEqual(result["action"], "swap_deposit")
        self.assertEqual(result["result"]["share_token"], "busd:usdx")
        self.assertEqual(result["result"]["share_amount"], "19155352120")
        self.assertEqual(
            result["result"]["inputs"][0],
            {"input_token": "busd", "input_amount": "1914.40274498"},
        )
        self.assertEqual(
            result["result"]["inputs"][1],
            {"input_token": "usdx", "input_amount": "1918.51883"},
        )

    def test_swap_withdraw(self):
        result = TestMessage._get_test_data_messages_result("swap_withdraw_v8")
        self.assertEqual(result["action"], "swap_withdraw")
        self.assertEqual(result["result"]["share_token"], "swp:usdx")
        self.assertEqual(result["result"]["share_amount"], "655345546")
        self.assertEqual(
            result["result"]["outputs"][0],
            {"output_token": "swp", "output_amount": "510.54504"},
        )
        self.assertEqual(
            result["result"]["outputs"][1],
            {"output_token": "usdx", "output_amount": "844.628983"},
        )

    def test_claim_swap_reward(self):
        result = TestMessage._get_test_data_messages_result("claim_swap_reward_v8")
        self.assertEqual(result["action"], "claim_swap_reward")
        self.assertEqual(
            result["result"]["rewards"][0],
            {"reward_token": "swp", "reward_amount": "830.379251"},
        )

    def test_send(self):
        result = TestMessage._get_test_data_messages_result("send_v8")
        self.assertEqual(result["action"], "send")
        self.assertEqual(
            result["result"],
            {
                "sender": "kava1dlezgt8undlpvdp0esmzyvxzvc59gkd56vkmea",
                "recipient": "kava1ys70jvnajkv88529ys6urjcyle3k2j9r24g6a7",
                "token": "kava",
                "amount": "2.17",
            },
        )

        result = TestMessage._get_test_data_messages_result("send_v2")
        self.assertEqual(result["action"], "send")
        self.assertEqual(
            result["result"],
            {
                "sender": "kava1k760ypy9tzhp6l2rmg06sq4n74z0d3relc549c",
                "recipient": "kava1nzq60hrphyr8anvkw6fv93mhafew7ez4tq9ahv",
                "token": "kava",
                "amount": "13.5",
            },
        )

    @classmethod
    def _get_test_data_messages_result(cls, filename) -> dict:
        with open(f"tests/data/{filename}.json", encoding="utf-8") as jsonfile_local:
            transaction = KavaTransaction(json.load(jsonfile_local))
            message = MessageFactory.get_messages(transaction)[0]

        return message.get_result()


if __name__ == "__main__":
    unittest.main()

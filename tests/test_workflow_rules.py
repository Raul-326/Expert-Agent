import unittest

import pandas as pd

from workflow_feishu import (
    build_panel_snapshot,
    build_dataframe_from_values,
    build_name_alias_index,
    calculate_back_to_back_annotator_stats,
    detect_back_to_back_schema,
    intelligent_column_mapping,
    resolve_full_name,
)


class WorkflowRulesTests(unittest.TestCase):
    def test_header_auto_detect_second_row(self):
        values = [
            ["", "", "", ""],
            ["Annotator", "QA", "CC Verdict", "POC Verdict"],
            ["a", "b", "pass", "fail"],
            ["c", "d", "fail", "pass"],
        ]
        df = build_dataframe_from_values(values)
        self.assertEqual(df.attrs.get("header_row"), 2)
        self.assertEqual(df.columns.tolist(), ["Annotator", "QA", "CC Verdict", "POC Verdict"])

    def test_mapping_prefers_poc_verdict_not_validation(self):
        df = pd.DataFrame(
            {
                "Annotator": ["u1", "u2"],
                "QA": ["q1", "q2"],
                "CC Verdict": ["pass", "fail"],
                "POC Name": ["p1", "p2"],
                "POC Verdict": ["pass", "fail"],
                "Validation": ["x", "y"],
            }
        )
        mapping = intelligent_column_mapping(df.columns.tolist(), df=df)
        self.assertEqual(mapping.get("CC Verdict"), "质检结果")
        self.assertEqual(mapping.get("POC Verdict"), "抽检结果")
        self.assertNotEqual(mapping.get("Validation"), "抽检结果")

    def test_name_mapping_keep_xiaoxia(self):
        roster = ["王乙琀", "刘茜"]
        alias_index = build_name_alias_index(roster)
        self.assertEqual(resolve_full_name("yihan", alias_index), "王乙琀")
        self.assertEqual(resolve_full_name("xiaoxia", alias_index), "xiaoxia")

    def test_snapshot_contains_group_and_sheet_titles(self):
        class Args:
            operator = "tester"
            sheet = "s1"
            header_row = 1
            sop_url = ""
            result_sheet = "结果"
            no_write_back = True

        annotators = pd.DataFrame(
            [
                {
                    "初标人": "张三",
                    "初标总产量": 10,
                    "被质检数": 10,
                    "质检通过数": 8,
                    "初标准确率": "80.00%",
                    "加权初标准确率": "80.00%",
                }
            ]
        )

        snapshot = build_panel_snapshot(
            spreadsheet_token="tokA",
            sheet_ref="s1",
            sheet_title="Sheet Alpha",
            spreadsheet_title="项目A",
            result_spreadsheet_token="tokR",
            result_sheet_ref="结果",
            project_display_name="",
            annotators=annotators,
            qas=pd.DataFrame(),
            pocs=pd.DataFrame(),
            difficulty=1.0,
            args=Args(),
            mapping={},
        )
        meta = snapshot["project_meta"]
        self.assertEqual(meta["project_group_id"], "tokA")
        self.assertEqual(meta["project_group_name"], "项目A")
        self.assertEqual(meta["sheet_title"], "Sheet Alpha")

    def test_back_to_back_without_discussion_columns(self):
        # 该结构没有 __3 商讨列，需仍能识别为背靠背并按“结果一致则双方通过”统计。
        df = pd.DataFrame(
            [
                {
                    "prompt": "p1",
                    "response": "r1",
                    "Name": "A",
                    "If DCG<3": "x",
                    "PT": "p",
                    "first_label": "l1",
                    "sec_label": "s1",
                    "Name__2": "B",
                    "If DCG<3__2": "y",
                    "PT__2": "p",
                    "first_label__2": "l1",
                    "sec_label__2": "s1",
                },
                {
                    "prompt": "p2",
                    "response": "r2",
                    "Name": "A",
                    "If DCG<3": "x",
                    "PT": "p",
                    "first_label": "l2",
                    "sec_label": "s2",
                    "Name__2": "B",
                    "If DCG<3__2": "x",
                    "PT__2": "p2",
                    "first_label__2": "l3",
                    "sec_label__2": "s2",
                },
            ]
        )

        self.assertTrue(detect_back_to_back_schema(df))
        annotators, _, _ = calculate_back_to_back_annotator_stats(df, debug=False)
        self.assertEqual(set(annotators["初标人"].tolist()), {"A", "B"})

        row_a = annotators[annotators["初标人"] == "A"].iloc[0].to_dict()
        row_b = annotators[annotators["初标人"] == "B"].iloc[0].to_dict()

        self.assertEqual(row_a["初标总产量"], 2)
        self.assertEqual(row_b["初标总产量"], 2)
        self.assertEqual(row_a["被质检数"], 2)
        self.assertEqual(row_b["被质检数"], 2)
        self.assertEqual(row_a["质检通过数"], 1)
        self.assertEqual(row_b["质检通过数"], 1)


if __name__ == "__main__":
    unittest.main()

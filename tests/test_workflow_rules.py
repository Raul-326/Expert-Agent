import unittest
from unittest.mock import patch

import pandas as pd

from workflow_feishu import (
    WorkflowComputeRequest,
    build_panel_snapshot,
    build_dataframe_from_values,
    build_name_alias_index,
    calculate_accuracy_workflow,
    calculate_back_to_back_annotator_stats,
    compute_workflow,
    detect_back_to_back_schema,
    infer_reference_pairs_with_ark,
    intelligent_column_mapping,
    ensure_minimum_actor_mapping,
    persist_workflow_result,
    rebalance_actor_role_mapping,
    _normalize_sheet_refs_for_source,
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

    def test_rebalance_name_from_poc_to_annotator_without_poc_context(self):
        df = pd.DataFrame({"Name": ["Tom", "Jerry"]})
        mapping = {"Name": "POC 姓名"}
        mapping = rebalance_actor_role_mapping(mapping, df)
        self.assertEqual(mapping.get("Name"), "初标人")

    def test_rebalance_name_from_qa_to_annotator_without_qa_context(self):
        df = pd.DataFrame({"Name": ["Tom", "Jerry"], "Badcase": ["yes", "no"]})
        mapping = {"Name": "质检人"}
        mapping = rebalance_actor_role_mapping(mapping, df)
        self.assertEqual(mapping.get("Name"), "初标人")

    def test_ensure_minimum_actor_mapping_pick_name_column(self):
        df = pd.DataFrame(
            {
                "Name": ["Tom", "Jerry", "Alice"],
                "Badcase": ["yes", "no", "yes"],
            }
        )
        mapping = ensure_minimum_actor_mapping({}, df)
        self.assertEqual(mapping.get("Name"), "初标人")

    def test_ensure_minimum_actor_mapping_skip_non_name_columns(self):
        df = pd.DataFrame(
            {
                "message_intention_level1": ["写作", "对话", "知识"],
                "label": ["a", "b", "c"],
            }
        )
        mapping = ensure_minimum_actor_mapping({}, df)
        self.assertEqual(mapping, {})

    def test_actor_column_should_not_use_yes_no_label_column(self):
        df = pd.DataFrame(
            {
                "SP Name": ["Tom", "Jerry", "Alice", "Bob"],
                "NM any forgetting of history?": ["yes", "no", "yes", "no"],
                "CC Verdict": ["pass", "fail", "pass", "fail"],
            }
        )

        def fake_ark(prompt: str, *args, **kwargs):
            if "列名: SP Name" in prompt:
                return '{"is_person_name_column": true, "reason": "person names"}'
            return '{"is_person_name_column": false, "reason": "binary labels"}'

        with patch("workflow_feishu.call_modelark_text", side_effect=fake_ark):
            mapping = intelligent_column_mapping(df.columns.tolist(), df=df)

        self.assertEqual(mapping.get("SP Name"), "初标人")
        self.assertNotEqual(mapping.get("NM any forgetting of history?"), "质检人")

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

    def test_snapshot_prefers_project_display_name_for_group_name(self):
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
            spreadsheet_token="tokB",
            sheet_ref="s1",
            sheet_title="Sheet Beta",
            spreadsheet_title="原始表名",
            result_spreadsheet_token="tokR",
            result_sheet_ref="结果",
            project_display_name="Dola-027-多语种投放VLM/LLM 001",
            annotators=annotators,
            qas=pd.DataFrame(),
            pocs=pd.DataFrame(),
            difficulty=1.0,
            args=Args(),
            mapping={},
        )
        meta = snapshot["project_meta"]
        self.assertEqual(meta["project_group_name"], "Dola-027-多语种投放VLM/LLM 001")

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

    def test_normalize_sheet_refs_support_sheet_urls(self):
        refs = _normalize_sheet_refs_for_source(
            source_url="https://bytedance.larkoffice.com/sheets/AAA111?sheet=s1",
            spreadsheet_token="AAA111",
            sheet_refs=[
                "s2",
                "https://bytedance.larkoffice.com/sheets/AAA111?sheet=s3",
                "s2",
            ],
        )
        self.assertEqual(refs, ["s2", "s3"])

    def test_compute_workflow_returns_preview_structure(self):
        df = pd.DataFrame(
            {
                "Annotator": ["a1", "a2"],
                "QA": ["q1", "q2"],
                "CC Verdict": ["pass", "fail"],
                "POC Name": ["p1", "p2"],
                "POC Verdict": ["pass", "pass"],
            }
        )
        df.attrs["sheet_id"] = "s1"
        df.attrs["sheet_title"] = "Sheet1"
        df.attrs["spreadsheet_title"] = "项目1"
        df.attrs["header_row"] = 1

        annotators = pd.DataFrame(
            [
                {
                    "初标人": "a1",
                    "初标总产量": 2,
                    "被质检数": 2,
                    "质检通过数": 1,
                    "初标准确率": "50.00%",
                }
            ]
        )

        with patch("workflow_feishu.resolve_feishu_access_token", return_value="user_token"), patch(
            "workflow_feishu._resolve_source_spreadsheet",
            return_value=("AAA111", "项目1", "https://bytedance.larkoffice.com/sheets/AAA111?sheet=s1"),
        ), patch("workflow_feishu.read_feishu_sheet", return_value=df), patch(
            "workflow_feishu.detect_back_to_back_schema", return_value=False
        ), patch(
            "workflow_feishu.intelligent_column_mapping",
            return_value={
                "Annotator": "初标人",
                "QA": "质检人",
                "CC Verdict": "质检结果",
                "POC Name": "POC 姓名",
                "POC Verdict": "抽检结果",
            },
        ), patch(
            "workflow_feishu.calculate_accuracy_workflow",
            return_value=(annotators, pd.DataFrame(), pd.DataFrame()),
        ), patch("workflow_feishu.load_name_roster", return_value=[]):
            result = compute_workflow(
                WorkflowComputeRequest(
                    source_url="https://bytedance.larkoffice.com/sheets/AAA111?sheet=s1",
                    sheet_refs=[],
                    auth_mode="user",
                    user_access_token="u-token",
                    project_display_name="项目显示名A",
                    poc_owner="负责人A",
                    evaluate_poc_score=False,
                )
            )

        self.assertEqual(result.spreadsheet_token, "AAA111")
        self.assertEqual(result.project_display_name, "项目显示名A")
        self.assertEqual(result.poc_owner, "负责人A")
        self.assertEqual(result.sheet_refs, ["s1"])
        self.assertEqual(len(result.sheets), 1)
        self.assertEqual(len(result.snapshots), 1)
        self.assertEqual(len(result.errors), 0)
        self.assertIn("project_metrics", result.project_aggregate_preview)
        self.assertEqual(result.snapshots[0]["project_meta"]["display_name"], "项目显示名A")
        self.assertEqual(result.snapshots[0]["run_meta"]["args"]["poc_owner"], "负责人A")

    def test_multiline_header_group_leaf_metadata(self):
        values = [
            ["Round1", "", "QC", ""],
            ["Badcase", "Question_Type", "Badcase", "Question_Type"],
            ["x", "t1", "x", "t1"],
        ]
        df = build_dataframe_from_values(values, header_row=2, header_depth="auto")
        self.assertEqual(df.attrs.get("header_depth"), 2)
        self.assertEqual(df.columns.tolist(), ["Round1::Badcase", "Round1::Question_Type", "QC::Badcase", "QC::Question_Type"])
        meta = df.attrs.get("column_metadata") or []
        self.assertEqual(meta[0]["group_name"], "Round1")
        self.assertEqual(meta[0]["leaf_name"], "Badcase")
        self.assertEqual(meta[0]["full_name"], "Round1::Badcase")

    def test_reference_keywords_prompt_contains_cc_qc_qa(self):
        df = pd.DataFrame({"A::Badcase": ["x"], "QC::Badcase": ["x"]})
        df.attrs["column_metadata"] = [
            {"index": 0, "group_name": "A", "leaf_name": "Badcase", "full_name": "A::Badcase", "column_name": "A::Badcase"},
            {"index": 1, "group_name": "QC", "leaf_name": "Badcase", "full_name": "QC::Badcase", "column_name": "QC::Badcase"},
        ]
        with patch(
            "workflow_feishu._call_modelark_json",
            return_value={
                "candidates": [
                    {
                        "source_group": "A",
                        "reference_group": "QC",
                        "confidence": 0.8,
                        "pairs": [{"source_col": "A::Badcase", "reference_col": "QC::Badcase", "type": "objective"}],
                    }
                ],
                "recommended_index": 0,
            },
        ) as mock_call:
            infer_reference_pairs_with_ark(df)
        prompt = mock_call.call_args[0][0]
        self.assertIn("qc", prompt.lower())
        self.assertIn("cc", prompt.lower())
        self.assertIn("qa", prompt.lower())

    def test_non_gt_naming_still_can_use_ark_reference_pairs(self):
        df = pd.DataFrame(
            {
                "SP Name": ["Tom", "Tom", "Jerry"],
                "Round1::Badcase": ["x", "x", "z"],
                "Round1::Question_Type": ["t1", "t2", "t1"],
                "Review::Badcase": ["x", "x", "z"],
                "Review::Question_Type": ["t1", "DIFF", "t1"],
            }
        )
        mapping = {"SP Name": "初标人"}
        with patch(
            "workflow_feishu.infer_reference_pairs_with_ark",
            return_value={
                "candidates": [
                    {
                        "source_group": "Round1",
                        "reference_group": "Review",
                        "confidence": 0.92,
                        "pairs": [
                            {"source_col": "Round1::Badcase", "reference_col": "Review::Badcase", "type": "objective"},
                            {"source_col": "Round1::Question_Type", "reference_col": "Review::Question_Type", "type": "objective"},
                        ],
                    }
                ],
                "selected": {
                    "source_group": "Round1",
                    "reference_group": "Review",
                    "confidence": 0.92,
                    "pairs": [
                        {"source_col": "Round1::Badcase", "reference_col": "Review::Badcase", "type": "objective"},
                        {"source_col": "Round1::Question_Type", "reference_col": "Review::Question_Type", "type": "objective"},
                    ],
                },
            },
        ), patch(
            "workflow_feishu.build_reference_result_by_ark",
            return_value=pd.Series(["通过", "不通过", "通过"], dtype=object),
        ):
            annotators, qas, pocs = calculate_accuracy_workflow(df, mapping)

        self.assertTrue(qas.empty)
        self.assertTrue(pocs.empty)
        row_a = annotators[annotators["初标人"] == "Tom"].iloc[0].to_dict()
        row_b = annotators[annotators["初标人"] == "Jerry"].iloc[0].to_dict()
        self.assertEqual(row_a["被质检数"], 2)
        self.assertEqual(row_a["质检通过数"], 1)
        self.assertEqual(row_b["被质检数"], 1)
        self.assertEqual(row_b["质检通过数"], 1)

    def test_multi_candidates_choose_highest_confidence(self):
        df = pd.DataFrame(
            {
                "S::A": ["x"],
                "R1::A": ["x"],
                "R2::A": ["x"],
            }
        )
        with patch(
            "workflow_feishu._call_modelark_json",
            return_value={
                "candidates": [
                    {"source_group": "S", "reference_group": "R1", "confidence": 0.4, "pairs": [{"source_col": "S::A", "reference_col": "R1::A", "type": "objective"}]},
                    {"source_group": "S", "reference_group": "R2", "confidence": 0.8, "pairs": [{"source_col": "S::A", "reference_col": "R2::A", "type": "objective"}]},
                ],
                "recommended_index": 99,
            },
        ):
            plan = infer_reference_pairs_with_ark(df)
        self.assertEqual(plan["selected"]["reference_group"], "R2")

    def test_subjective_columns_do_not_affect_pass(self):
        df = pd.DataFrame(
            {
                "SP Name": ["Tom", "Tom", "Jerry"],
                "A::Badcase": ["x", "x", "z"],
                "R::Badcase": ["x", "y", "z"],
                "A::Question_Description": ["主观1", "主观2", "主观3"],
                "R::Question_Description": ["完全不同", "也不同", "另一个不同"],
            }
        )
        mapping = {"SP Name": "初标人"}
        with patch(
            "workflow_feishu.infer_reference_pairs_with_ark",
            return_value={
                "candidates": [
                    {
                        "source_group": "A",
                        "reference_group": "R",
                        "confidence": 0.95,
                        "pairs": [
                            {"source_col": "A::Badcase", "reference_col": "R::Badcase", "type": "objective"},
                            {
                                "source_col": "A::Question_Description",
                                "reference_col": "R::Question_Description",
                                "type": "subjective",
                            },
                        ],
                    }
                ],
                "selected": {
                    "source_group": "A",
                    "reference_group": "R",
                    "confidence": 0.95,
                    "pairs": [
                        {"source_col": "A::Badcase", "reference_col": "R::Badcase", "type": "objective"},
                        {"source_col": "A::Question_Description", "reference_col": "R::Question_Description", "type": "subjective"},
                    ],
                },
            },
        ), patch(
            "workflow_feishu.build_reference_result_by_ark",
            return_value=pd.Series(["通过", "不通过", "通过"], dtype=object),
        ):
            annotators, _, _ = calculate_accuracy_workflow(df, mapping)

        tom = annotators[annotators["初标人"] == "Tom"].iloc[0].to_dict()
        self.assertEqual(tom["被质检数"], 2)
        self.assertEqual(tom["质检通过数"], 1)

    def test_comparable_false_rows_not_counted(self):
        df = pd.DataFrame(
            {
                "SP Name": ["Tom", "Tom", "Jerry"],
                "A::Badcase": ["x", "", "z"],
                "R::Badcase": ["x", "", "z"],
            }
        )
        mapping = {"SP Name": "初标人"}
        with patch(
            "workflow_feishu.infer_reference_pairs_with_ark",
            return_value={
                "candidates": [
                    {
                        "source_group": "A",
                        "reference_group": "R",
                        "confidence": 0.9,
                        "pairs": [{"source_col": "A::Badcase", "reference_col": "R::Badcase", "type": "objective"}],
                    }
                ],
                "selected": {
                    "source_group": "A",
                    "reference_group": "R",
                    "confidence": 0.9,
                    "pairs": [{"source_col": "A::Badcase", "reference_col": "R::Badcase", "type": "objective"}],
                },
            },
        ), patch(
            "workflow_feishu.build_reference_result_by_ark",
            return_value=pd.Series(["通过", None, "通过"], dtype=object),
        ):
            annotators, _, _ = calculate_accuracy_workflow(df, mapping)
        tom = annotators[annotators["初标人"] == "Tom"].iloc[0].to_dict()
        self.assertEqual(tom["被质检数"], 1)
        self.assertEqual(tom["质检通过数"], 1)

    def test_low_confidence_fallback_to_existing_result_column(self):
        df = pd.DataFrame(
            {
                "SP Name": ["Tom", "Jerry", "Jerry"],
                "CC Verdict": ["pass", "fail", "pass"],
                "A::Badcase": ["x", "y", "z"],
                "R::Badcase": ["x", "x", "z"],
            }
        )
        mapping = {"SP Name": "初标人"}
        with patch(
            "workflow_feishu.infer_reference_pairs_with_ark",
            return_value={
                "candidates": [
                    {
                        "source_group": "A",
                        "reference_group": "R",
                        "confidence": 0.2,
                        "pairs": [{"source_col": "A::Badcase", "reference_col": "R::Badcase", "type": "objective"}],
                    }
                ],
                "selected": {
                    "source_group": "A",
                    "reference_group": "R",
                    "confidence": 0.2,
                    "pairs": [{"source_col": "A::Badcase", "reference_col": "R::Badcase", "type": "objective"}],
                },
            },
        ), patch("workflow_feishu.build_reference_result_by_ark") as mock_build:
            annotators, _, _ = calculate_accuracy_workflow(df, mapping, ark_reference_confidence_threshold=0.6)
        self.assertFalse(mock_build.called)
        tom = annotators[annotators["初标人"] == "Tom"].iloc[0].to_dict()
        jerry = annotators[annotators["初标人"] == "Jerry"].iloc[0].to_dict()
        self.assertEqual(tom["质检通过数"], 1)
        self.assertEqual(jerry["质检通过数"], 1)

    def test_low_confidence_without_result_column_keeps_no_qc_denominator(self):
        df = pd.DataFrame(
            {
                "SP Name": ["Tom", "Jerry", "Jerry"],
                "A::Badcase": ["x", "y", "z"],
                "R::Badcase": ["x", "x", "z"],
            }
        )
        mapping = {"SP Name": "初标人"}
        with patch(
            "workflow_feishu.infer_reference_pairs_with_ark",
            return_value={
                "candidates": [
                    {
                        "source_group": "A",
                        "reference_group": "R",
                        "confidence": 0.2,
                        "pairs": [{"source_col": "A::Badcase", "reference_col": "R::Badcase", "type": "objective"}],
                    }
                ],
                "selected": {
                    "source_group": "A",
                    "reference_group": "R",
                    "confidence": 0.2,
                    "pairs": [{"source_col": "A::Badcase", "reference_col": "R::Badcase", "type": "objective"}],
                },
            },
        ):
            annotators, _, _ = calculate_accuracy_workflow(df, mapping, ark_reference_confidence_threshold=0.6)
        tom = annotators[annotators["初标人"] == "Tom"].iloc[0].to_dict()
        self.assertEqual(tom["被质检数"], 0)

    def test_existing_result_column_priority_over_ark(self):
        df = pd.DataFrame(
            {
                "SP Name": ["Tom", "Jerry", "Jerry"],
                "质检结果": ["通过", "不通过", "通过"],
                "A::Badcase": ["x", "y", "z"],
                "R::Badcase": ["x", "x", "z"],
            }
        )
        mapping = {"SP Name": "初标人"}
        with patch("workflow_feishu.infer_reference_pairs_with_ark") as mock_infer, patch(
            "workflow_feishu.build_reference_result_by_ark"
        ) as mock_build:
            annotators, _, _ = calculate_accuracy_workflow(df, mapping)
        self.assertFalse(mock_infer.called)
        self.assertFalse(mock_build.called)
        tom = annotators[annotators["初标人"] == "Tom"].iloc[0].to_dict()
        self.assertEqual(tom["质检通过数"], 1)

    def test_persist_workflow_result_returns_run_ids(self):
        class DummyResult:
            snapshots = [{"run_id": "r1"}, {"run_id": "r2"}]

        with patch("workflow_feishu.save_run_snapshot", side_effect=["r1", "r2"]):
            run_ids = persist_workflow_result(DummyResult(), db_path="/tmp/x.db")
        self.assertEqual(run_ids, ["r1", "r2"])


if __name__ == "__main__":
    unittest.main()

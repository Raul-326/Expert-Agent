import tempfile
import unittest

from panel_db import (
    apply_override,
    get_person_project_series,
    get_project_detail,
    get_project_group_detail,
    get_sheet_detail,
    list_audit_logs,
    list_project_groups,
    save_run_snapshot,
)
from panel_metrics import compute_effective_person_overall, compute_effective_project_metrics


class PanelMetricsAndDBTests(unittest.TestCase):
    @staticmethod
    def _make_snapshot(
        project_id: str,
        group_id: str,
        group_name: str,
        sheet_ref: str,
        sheet_title: str,
        run_id: str,
        run_at: str,
        person_rows: list,
        difficulty: float = 1.0,
    ) -> dict:
        token = group_id
        return {
            "project_id": project_id,
            "run_id": run_id,
            "run_at": run_at,
            "difficulty_coef": difficulty,
            "project_meta": {
                "project_id": project_id,
                "project_group_id": group_id,
                "project_group_name": group_name,
                "spreadsheet_token": token,
                "sheet_ref": sheet_ref,
                "sheet_title": sheet_title,
                "display_name": sheet_title,
                "result_spreadsheet_token": token,
                "result_sheet_ref": "结果",
            },
            "run_meta": {
                "run_id": run_id,
                "run_at": run_at,
                "source_type": "workflow_feishu",
                "difficulty_coef": difficulty,
            },
            "person_metrics_base": person_rows,
        }

    def test_override_recompute_accuracy_and_weighted(self):
        base_rows = [
            {
                "project_id": "p1",
                "person_name": "张三",
                "role": "初标",
                "volume": 20,
                "inspected_count": 10,
                "pass_count": 8,
                "accuracy": 0.8,
                "difficulty_coef": 1.2,
            }
        ]
        overrides = [
            {
                "project_id": "p1",
                "person_name": "张三",
                "role": "初标",
                "metric_key": "pass_count",
                "override_value": "9",
                "is_active": 1,
            },
            {
                "project_id": "p1",
                "person_name": None,
                "role": None,
                "metric_key": "difficulty_coef",
                "override_value": "1.3",
                "is_active": 1,
            },
        ]

        result = compute_effective_project_metrics(base_rows, overrides)
        row = result["person_metrics"][0]

        self.assertAlmostEqual(row["accuracy"], 0.9, places=6)
        self.assertAlmostEqual(row["weighted_accuracy"], 1.17, places=6)

    def test_person_overall_weighted_by_inspected_count(self):
        rows = [
            {
                "project_id": "p1",
                "person_name": "李四",
                "role": "初标",
                "inspected_count": 10,
                "pass_count": 8,
                "weighted_accuracy": 0.96,
            },
            {
                "project_id": "p2",
                "person_name": "李四",
                "role": "初标",
                "inspected_count": 20,
                "pass_count": 10,
                "weighted_accuracy": 0.65,
            },
        ]
        overall = compute_effective_person_overall(rows)
        self.assertEqual(len(overall), 1)
        item = overall[0]

        self.assertAlmostEqual(item["overall_accuracy"], 0.6, places=6)
        self.assertAlmostEqual(item["overall_weighted_accuracy"], (0.96 * 10 + 0.65 * 20) / 30, places=6)

    def test_audit_log_written_for_override_changes(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = f"{tmp}/panel.db"
            snapshot = {
                "project_id": "tok:Sheet1",
                "run_id": "run-1",
                "run_at": "2026-03-03T00:00:00+00:00",
                "difficulty_coef": 1.2,
                "project_meta": {
                    "project_id": "tok:Sheet1",
                    "spreadsheet_token": "tok",
                    "sheet_ref": "Sheet1",
                    "display_name": "demo",
                    "result_spreadsheet_token": "tok",
                    "result_sheet_ref": "结果",
                },
                "run_meta": {
                    "run_id": "run-1",
                    "run_at": "2026-03-03T00:00:00+00:00",
                    "source_type": "workflow_feishu",
                },
                "person_metrics_base": [
                    {
                        "project_id": "tok:Sheet1",
                        "person_name": "张三",
                        "role": "初标",
                        "volume": 10,
                        "inspected_count": 5,
                        "pass_count": 4,
                        "accuracy": 0.8,
                        "weighted_accuracy": 0.96,
                        "difficulty_coef": 1.2,
                    }
                ],
            }
            save_run_snapshot(snapshot, db_path=db_path)

            apply_override(
                db_path=db_path,
                project_id="tok:Sheet1",
                person_name="张三",
                role="初标",
                metric_key="pass_count",
                override_value=5,
                updated_by="tester",
                reason="fix1",
            )
            apply_override(
                db_path=db_path,
                project_id="tok:Sheet1",
                person_name="张三",
                role="初标",
                metric_key="pass_count",
                override_value=4,
                updated_by="tester",
                reason="fix2",
            )

            logs = list_audit_logs(db_path=db_path, project_id="tok:Sheet1", updated_by="tester", limit=20)
            self.assertGreaterEqual(len(logs), 2)
            actions = {x["action"] for x in logs}
            self.assertIn("create_override", actions)
            self.assertIn("update_override", actions)

            detail = get_project_detail(db_path, "tok:Sheet1")
            self.assertTrue(detail.get("overrides"))

    def test_project_group_aggregation_and_sheet_count(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = f"{tmp}/panel.db"
            g = "tok_g1"

            s1 = self._make_snapshot(
                project_id=f"{g}:s1",
                group_id=g,
                group_name="项目G1",
                sheet_ref="s1",
                sheet_title="Sheet A",
                run_id="run-s1",
                run_at="2026-03-03T00:00:00+00:00",
                person_rows=[
                    {
                        "project_id": f"{g}:s1",
                        "person_name": "张三",
                        "role": "初标",
                        "volume": 10,
                        "inspected_count": 10,
                        "pass_count": 8,
                        "accuracy": 0.8,
                        "weighted_accuracy": 0.8,
                        "difficulty_coef": 1.0,
                    }
                ],
            )
            s2 = self._make_snapshot(
                project_id=f"{g}:s2",
                group_id=g,
                group_name="项目G1",
                sheet_ref="s2",
                sheet_title="Sheet B",
                run_id="run-s2",
                run_at="2026-03-04T00:00:00+00:00",
                person_rows=[
                    {
                        "project_id": f"{g}:s2",
                        "person_name": "张三",
                        "role": "初标",
                        "volume": 20,
                        "inspected_count": 20,
                        "pass_count": 12,
                        "accuracy": 0.6,
                        "weighted_accuracy": 0.6,
                        "difficulty_coef": 1.0,
                    }
                ],
            )
            save_run_snapshot(s1, db_path=db_path)
            save_run_snapshot(s2, db_path=db_path)

            groups = list_project_groups(db_path=db_path)
            self.assertEqual(len(groups), 1)
            self.assertEqual(groups[0]["sheet_count"], 2)
            self.assertAlmostEqual(groups[0]["project_accuracy"], (8 + 12) / (10 + 20), places=6)

            detail = get_project_group_detail(db_path=db_path, project_group_id=g)
            self.assertEqual(detail["sheet_count"], 2)
            overall = next(x for x in detail["project_metrics"] if x["metric_group"] == "整体")
            self.assertAlmostEqual(overall["accuracy"], (8 + 12) / (10 + 20), places=6)

    def test_person_series_supports_project_and_sheet_granularity(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = f"{tmp}/panel.db"
            g = "tok_g2"

            save_run_snapshot(
                self._make_snapshot(
                    project_id=f"{g}:s1",
                    group_id=g,
                    group_name="项目G2",
                    sheet_ref="s1",
                    sheet_title="S1",
                    run_id="run1",
                    run_at="2026-03-03T00:00:00+00:00",
                    person_rows=[
                        {
                            "project_id": f"{g}:s1",
                            "person_name": "李四",
                            "role": "初标",
                            "volume": 10,
                            "inspected_count": 10,
                            "pass_count": 9,
                            "accuracy": 0.9,
                            "weighted_accuracy": 0.9,
                            "difficulty_coef": 1.0,
                        }
                    ],
                ),
                db_path=db_path,
            )
            save_run_snapshot(
                self._make_snapshot(
                    project_id=f"{g}:s2",
                    group_id=g,
                    group_name="项目G2",
                    sheet_ref="s2",
                    sheet_title="S2",
                    run_id="run2",
                    run_at="2026-03-04T00:00:00+00:00",
                    person_rows=[
                        {
                            "project_id": f"{g}:s2",
                            "person_name": "李四",
                            "role": "初标",
                            "volume": 30,
                            "inspected_count": 30,
                            "pass_count": 15,
                            "accuracy": 0.5,
                            "weighted_accuracy": 0.5,
                            "difficulty_coef": 1.0,
                        }
                    ],
                ),
                db_path=db_path,
            )

            project_series = get_person_project_series(db_path=db_path, person_name="李四", role="初标", granularity="project")
            sheet_series = get_person_project_series(db_path=db_path, person_name="李四", role="初标", granularity="sheet")

            self.assertEqual(len(project_series), 1)
            self.assertEqual(len(sheet_series), 2)
            self.assertAlmostEqual(project_series[0]["accuracy"], (9 + 15) / (10 + 30), places=6)

    def test_override_default_scope_is_sheet_level(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = f"{tmp}/panel.db"
            g = "tok_g3"
            p1 = f"{g}:s1"
            p2 = f"{g}:s2"

            save_run_snapshot(
                self._make_snapshot(
                    project_id=p1,
                    group_id=g,
                    group_name="项目G3",
                    sheet_ref="s1",
                    sheet_title="S1",
                    run_id="run1",
                    run_at="2026-03-03T00:00:00+00:00",
                    person_rows=[
                        {
                            "project_id": p1,
                            "person_name": "王五",
                            "role": "初标",
                            "volume": 10,
                            "inspected_count": 10,
                            "pass_count": 8,
                            "accuracy": 0.8,
                            "weighted_accuracy": 0.8,
                            "difficulty_coef": 1.0,
                        }
                    ],
                ),
                db_path=db_path,
            )
            save_run_snapshot(
                self._make_snapshot(
                    project_id=p2,
                    group_id=g,
                    group_name="项目G3",
                    sheet_ref="s2",
                    sheet_title="S2",
                    run_id="run2",
                    run_at="2026-03-03T00:10:00+00:00",
                    person_rows=[
                        {
                            "project_id": p2,
                            "person_name": "王五",
                            "role": "初标",
                            "volume": 20,
                            "inspected_count": 20,
                            "pass_count": 10,
                            "accuracy": 0.5,
                            "weighted_accuracy": 0.5,
                            "difficulty_coef": 1.0,
                        }
                    ],
                ),
                db_path=db_path,
            )

            apply_override(
                db_path=db_path,
                project_id=p1,
                person_name="王五",
                role="初标",
                metric_key="pass_count",
                override_value=10,
                updated_by="tester",
                reason="sheet-only",
            )

            d1 = get_sheet_detail(db_path=db_path, project_id=p1)
            d2 = get_sheet_detail(db_path=db_path, project_id=p2)
            r1 = next(x for x in d1["person_metrics"] if x["person_name"] == "王五" and x["role"] == "初标")
            r2 = next(x for x in d2["person_metrics"] if x["person_name"] == "王五" and x["role"] == "初标")

            self.assertAlmostEqual(r1["accuracy"], 1.0, places=6)
            self.assertAlmostEqual(r2["accuracy"], 0.5, places=6)


if __name__ == "__main__":
    unittest.main()

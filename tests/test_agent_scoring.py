import tempfile
import unittest
from unittest.mock import patch

import pandas as pd

from agent.orchestrator import run_task
from agent.skills import poc_score_aggregate_skill, sop_quality_skill
from agent.types import AgentTaskRequest
from panel_db import (
    apply_poc_score_override,
    get_latest_project_poc_score,
    list_audit_logs,
    save_project_poc_score,
)


class AgentScoringTests(unittest.TestCase):
    def test_manual_sop_score_required_when_no_sop(self):
        with self.assertRaises(ValueError):
            sop_quality_skill(sop_url="", token="", manual_sop_score=None)

    def test_manual_sop_score_clamped(self):
        result = sop_quality_skill(sop_url="", token="", manual_sop_score=120)
        self.assertEqual(result["sop_score"], 100.0)
        self.assertEqual(result["source_type"], "manual")

    def test_poc_aggregate_formula(self):
        result = poc_score_aggregate_skill(sop_score=90, sheet_score=70, project_owner="张三")
        self.assertEqual(result["poc_total_score"], 80.0)
        self.assertEqual(result["grade"], "B")

    def test_score_override_and_audit(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = f"{tmp}/panel.db"
            score_id = save_project_poc_score(
                db_path=db_path,
                job_id="job-1",
                project_group_id="tok_1",
                project_owner="负责人A",
                sop_score=80,
                sheet_score=70,
                total_score=75,
                grade="B",
                sop_source_type="llm",
                model_name="m1",
                prompt_version="v1",
                sop_reason="r1",
                sop_evidence=["e1"],
                sop_raw_output={"a": 1},
                sheet_reason="r2",
                sheet_evidence=["e2"],
                sheet_raw_output={"b": 2},
            )

            override_id = apply_poc_score_override(
                db_path=db_path,
                score_id=score_id,
                updated_by="tester",
                reason="人工修正",
                override_fields={"total_score": 88},
            )
            self.assertGreater(override_id, 0)

            latest = get_latest_project_poc_score(db_path=db_path, project_group_id="tok_1")
            score = latest.get("score") or {}
            self.assertEqual(score.get("total_score"), 88.0)
            self.assertEqual(score.get("grade"), "A")

            logs = list_audit_logs(db_path=db_path, updated_by="tester", limit=20)
            actions = {x.get("action") for x in logs}
            self.assertIn("poc_score_override", actions)

    def test_run_task_dry_run_no_db_write(self):
        df = pd.DataFrame({"x": [1]})
        skill_outputs = [
            {
                "df": df,
                "spreadsheet_token": "tok1",
                "token": "u-token",
                "sheet_ref": "s1",
                "sheet_title": "Sheet1",
                "spreadsheet_title": "项目1",
            },
            {"schema_type": "normal", "mapping": {}},
            {"annotators": pd.DataFrame(), "qas": pd.DataFrame(), "pocs": pd.DataFrame()},
            {"annotators": pd.DataFrame(), "qas": pd.DataFrame(), "pocs": pd.DataFrame()},
            {
                "sop_score": 80,
                "sop_reason": "",
                "sop_evidence": [],
                "source_type": "manual",
                "model_name": "manual",
                "prompt_version": "poc_sop_v1",
                "raw_model_output": "",
                "raw_payload": {},
            },
            {
                "sheet_score": 70,
                "sheet_reason": "",
                "sheet_evidence": [],
                "model_name": "model",
                "prompt_version": "poc_sheet_v1",
                "raw_model_output": "",
                "raw_payload": {},
            },
            {"project_owner": "负责人A", "sop_score": 80, "sheet_score": 70, "poc_total_score": 75, "grade": "B"},
        ]

        def fake_invoke(*args, **kwargs):
            return skill_outputs.pop(0)

        with patch("agent.orchestrator._register_default_skills"), patch(
            "agent.orchestrator._resolve_source_url", return_value="https://bytedance.larkoffice.com/sheets/tok1?sheet=s1"
        ), patch("agent.orchestrator._extract_sheet_refs", return_value=["s1"]), patch(
            "agent.orchestrator._invoke_skill", side_effect=fake_invoke
        ), patch(
            "agent.orchestrator.wf.build_panel_snapshot",
            return_value={"run_id": "run-1", "person_metrics_base": [], "project_metrics_base": []},
        ), patch("agent.orchestrator.detect_project_owner", return_value="负责人A"), patch(
            "agent.orchestrator.create_agent_job"
        ) as m_create_job, patch(
            "agent.orchestrator.update_agent_job_status"
        ) as m_update_job, patch(
            "agent.orchestrator.save_run_snapshot"
        ) as m_save_run, patch(
            "agent.orchestrator.save_project_poc_score"
        ) as m_save_score:
            req = AgentTaskRequest(
                source_url="https://bytedance.larkoffice.com/sheets/tok1?sheet=s1",
                manual_sop_score=80,
                user_access_token="u-token",
                flags={"dry_run": True},
            )
            result = run_task(req)

        self.assertEqual(result.run_ids, ["run-1"])
        self.assertIsNone(result.poc_score_id)
        self.assertEqual(result.score_card.get("poc_total_score"), 75)
        self.assertFalse(m_create_job.called)
        self.assertFalse(m_update_job.called)
        self.assertFalse(m_save_run.called)
        self.assertFalse(m_save_score.called)


if __name__ == "__main__":
    unittest.main()

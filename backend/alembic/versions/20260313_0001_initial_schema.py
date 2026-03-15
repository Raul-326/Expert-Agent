"""initial schema

Revision ID: 20260313_0001
Revises:
Create Date: 2026-03-13 18:20:00
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision = "20260313_0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "audit_logs",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("table_name", sa.String(), nullable=False),
        sa.Column("record_id", sa.Integer(), nullable=False),
        sa.Column("action", sa.String(), nullable=False),
        sa.Column("operator", sa.String(), nullable=False),
        sa.Column("old_value", sa.String(), nullable=True),
        sa.Column("new_value", sa.String(), nullable=True),
        sa.Column("reason", sa.String(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "person_metrics_base",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("run_id", sa.Integer(), nullable=False),
        sa.Column("person_name", sa.String(), nullable=False),
        sa.Column("role", sa.String(), nullable=False),
        sa.Column("volume", sa.Integer(), nullable=False),
        sa.Column("inspected_count", sa.Integer(), nullable=False),
        sa.Column("pass_count", sa.Integer(), nullable=False),
        sa.Column("accuracy", sa.Float(), nullable=True),
        sa.Column("weighted_accuracy", sa.Float(), nullable=True),
        sa.Column("difficulty_coef", sa.Float(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "poc_scores",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("project_group_id", sa.Integer(), nullable=False),
        sa.Column("run_id", sa.Integer(), nullable=True),
        sa.Column("sop_source_type", sa.String(), nullable=True),
        sa.Column("model_name", sa.String(), nullable=True),
        sa.Column("total_score", sa.Float(), nullable=False),
        sa.Column("grade", sa.String(), nullable=True),
        sa.Column("sop_score", sa.Float(), nullable=False),
        sa.Column("sheet_score", sa.Float(), nullable=False),
        sa.Column("project_owner", sa.String(), nullable=True),
        sa.Column("details_json", sa.String(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "project_group_overrides",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("project_group_id", sa.Integer(), nullable=False),
        sa.Column("person_name", sa.String(), nullable=False),
        sa.Column("role", sa.String(), nullable=False),
        sa.Column("metric_key", sa.String(), nullable=False),
        sa.Column("override_value", sa.Float(), nullable=False),
        sa.Column("is_active", sa.Boolean(), nullable=False),
        sa.Column("updated_by", sa.String(), nullable=False),
        sa.Column("reason", sa.String(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "project_groups",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("project_group_name", sa.String(), nullable=False),
        sa.Column("spreadsheet_token", sa.String(), nullable=False),
        sa.Column("poc_name", sa.String(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "project_metrics_base",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("run_id", sa.Integer(), nullable=False),
        sa.Column("metric_group", sa.String(), nullable=False),
        sa.Column("volume_total", sa.Integer(), nullable=False),
        sa.Column("inspected_total", sa.Integer(), nullable=False),
        sa.Column("pass_total", sa.Integer(), nullable=False),
        sa.Column("accuracy", sa.Float(), nullable=True),
        sa.Column("weighted_accuracy", sa.Float(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "project_sheets",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("run_id", sa.Integer(), nullable=False),
        sa.Column("sheet_ref", sa.String(), nullable=False),
        sa.Column("sheet_title", sa.String(), nullable=True),
        sa.Column("schema_type", sa.String(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_table(
        "runs",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("project_group_id", sa.Integer(), nullable=False),
        sa.Column("batch_project_name", sa.String(), nullable=True),
        sa.Column("batch_no", sa.String(), nullable=True),
        sa.Column("run_at", sa.DateTime(), nullable=False),
        sa.Column("status", sa.String(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )


def downgrade() -> None:
    op.drop_table("runs")
    op.drop_table("project_sheets")
    op.drop_table("project_metrics_base")
    op.drop_table("project_groups")
    op.drop_table("project_group_overrides")
    op.drop_table("poc_scores")
    op.drop_table("person_metrics_base")
    op.drop_table("audit_logs")

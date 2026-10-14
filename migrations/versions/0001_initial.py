"""Initial durable transaction processing schema."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.mysql import DATETIME

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def timestamp():
    return sa.DateTime().with_variant(DATETIME(fsp=6), "mysql")


def upgrade():
    op.create_table(
        "transactions",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("customer_id", sa.String(128), nullable=False),
        sa.Column("value", sa.Numeric(18, 2), nullable=False),
        sa.Column("status", sa.String(20), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("generation", sa.Integer(), nullable=False),
        sa.Column("correlation_id", sa.String(36), nullable=False),
        sa.Column("created_at", timestamp(), nullable=False),
        sa.Column("updated_at", timestamp(), nullable=False),
        sa.Column(
            "idempotency_key",
            sa.String(128).with_variant(
                sa.String(128, collation="utf8mb4_bin"),
                "mysql",
            ),
        ),
        sa.Column("fingerprint", sa.String(64)),
        sa.UniqueConstraint("idempotency_key", name="uq_transactions_idempotency_key"),
        mysql_engine="InnoDB",
        mysql_charset="utf8mb4",
    )
    op.create_table(
        "processing_jobs",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("event_id", sa.String(36), nullable=False),
        sa.Column(
            "transaction_id", sa.String(36), sa.ForeignKey("transactions.id"), nullable=False
        ),
        sa.Column("generation", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(20), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False),
        sa.Column("next_attempt_at", timestamp(), nullable=False),
        sa.Column("created_at", timestamp(), nullable=False),
        sa.Column("updated_at", timestamp(), nullable=False),
        sa.Column("lease_token", sa.String(36)),
        sa.Column("lease_until", timestamp()),
        sa.Column("last_error", sa.String(500)),
        sa.UniqueConstraint("event_id", name="uq_processing_jobs_event_id"),
        sa.UniqueConstraint("transaction_id", "generation", name="uq_job_generation"),
        mysql_engine="InnoDB",
        mysql_charset="utf8mb4",
    )
    op.create_index("ix_jobs_ready", "processing_jobs", ["status", "next_attempt_at"])
    op.create_index("ix_jobs_expired", "processing_jobs", ["status", "lease_until"])
    op.create_table(
        "outbox_events",
        sa.Column(
            "id",
            sa.BigInteger().with_variant(sa.Integer(), "sqlite"),
            primary_key=True,
            autoincrement=True,
        ),
        sa.Column("event_id", sa.String(36), nullable=False),
        sa.Column("transaction_id", sa.String(36), sa.ForeignKey("transactions.id")),
        sa.Column("topic", sa.String(128), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("created_at", timestamp(), nullable=False),
        sa.Column("published_at", timestamp()),
        sa.UniqueConstraint("event_id", name="uq_outbox_events_event_id"),
        mysql_engine="InnoDB",
        mysql_charset="utf8mb4",
    )
    op.create_index("ix_outbox_pending", "outbox_events", ["published_at", "id"])
    op.create_table(
        "failed_messages",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("transaction_id", sa.String(36), sa.ForeignKey("transactions.id")),
        sa.Column("generation", sa.Integer()),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("created_at", timestamp(), nullable=False),
        sa.Column("source_topic", sa.String(128)),
        sa.Column("source_partition", sa.Integer()),
        sa.Column("source_offset", sa.BigInteger()),
        sa.Column("replayed_at", timestamp()),
        sa.Column("replay_event_id", sa.String(36)),
        sa.UniqueConstraint(
            "source_topic", "source_partition", "source_offset", name="uq_failure_source"
        ),
        mysql_engine="InnoDB",
        mysql_charset="utf8mb4",
    )
    op.create_index("ix_failures_transaction", "failed_messages", ["transaction_id", "generation"])
    op.create_index("ix_failures_created", "failed_messages", ["created_at"])


def downgrade():
    for table in ("failed_messages", "outbox_events", "processing_jobs", "transactions"):
        op.drop_table(table)

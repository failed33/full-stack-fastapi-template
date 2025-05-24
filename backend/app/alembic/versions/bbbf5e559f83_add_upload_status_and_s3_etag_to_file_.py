"""add_upload_status_and_s3_etag_to_file_model

Revision ID: bbbf5e559f83
Revises: 7ba1975450dd
Create Date: 2025-05-24 19:41:48.088339

"""
from alembic import op
import sqlalchemy as sa
import sqlmodel.sql.sqltypes


# revision identifiers, used by Alembic.
revision = 'bbbf5e559f83'
down_revision = '7ba1975450dd'
branch_labels = None
depends_on = None


def upgrade():
    # Add the new columns to the file table
    op.add_column('file', sa.Column('upload_status', sa.String(50), nullable=False, server_default='PENDING'))
    op.add_column('file', sa.Column('s3_etag', sa.String(255), nullable=True))
    op.create_index(op.f('ix_file_upload_status'), 'file', ['upload_status'], unique=False)

    # Remove the default before converting to enum type, then convert and re-add default
    op.alter_column('file', 'upload_status', server_default=None)
    op.execute("ALTER TABLE file ALTER COLUMN upload_status TYPE processstatus USING upload_status::processstatus")
    op.execute("ALTER TABLE file ALTER COLUMN upload_status SET DEFAULT 'PENDING'::processstatus")


def downgrade():
    op.drop_index(op.f('ix_file_upload_status'), table_name='file')
    op.drop_column('file', 's3_etag')
    op.drop_column('file', 'upload_status')

"""your_migration_description

Revision ID: 7ba1975450dd
Revises: ca2f89d387b4
Create Date: 2025-05-23 23:17:35.010705

"""
from alembic import op
import sqlalchemy as sa
import sqlmodel.sql.sqltypes


# revision identifiers, used by Alembic.
revision = '7ba1975450dd'
down_revision = 'ca2f89d387b4'
branch_labels = None
depends_on = None


def upgrade():
    pass


def downgrade():
    pass

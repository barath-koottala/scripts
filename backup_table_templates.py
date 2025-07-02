"""
Backup table creation SQL templates for client deletion operations.
These templates get populated with actual table names and timestamps during execution.
"""

# Template for creating account backup table
ACCOUNT_BACKUP_TABLE_SQL = """CREATE TABLE IF NOT EXISTS account.{account_backup_table} (
    LIKE account.virtual_account_holder INCLUDING ALL
);
ALTER TABLE account.{account_backup_table} 
ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ DEFAULT NOW(),
ADD COLUMN IF NOT EXISTS deletion_reason TEXT DEFAULT 'Missing email cleanup',
ADD COLUMN IF NOT EXISTS deletion_batch_id TEXT DEFAULT '{timestamp}';"""

# Template for creating entity backup table
ENTITY_BACKUP_TABLE_SQL = """CREATE TABLE IF NOT EXISTS entity.{entity_backup_table} (
    LIKE entity.entity INCLUDING ALL
);
ALTER TABLE entity.{entity_backup_table} 
ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ DEFAULT NOW(),
ADD COLUMN IF NOT EXISTS deletion_reason TEXT DEFAULT 'Missing email cleanup',
ADD COLUMN IF NOT EXISTS deletion_batch_id TEXT DEFAULT '{timestamp}';"""

# Combined template for dry run display
DRY_RUN_BACKUP_TABLES_SQL = """-- Account backup table
{account_sql}

-- Entity backup table
{entity_sql}"""

# Template for backup records SQL (dry run display)
BACKUP_RECORDS_SQL = """-- Backup virtual_account_holder records
INSERT INTO account.{account_backup_table} 
SELECT *, NOW(), 'Missing email cleanup', '{timestamp}'
FROM account.virtual_account_holder
WHERE person_id IN ({sample_ids}...);

-- Backup entity records
INSERT INTO entity.{entity_backup_table} 
SELECT *, NOW(), 'Missing email cleanup', '{timestamp}'
FROM entity.entity
WHERE entity_id IN ({sample_ids}...);"""
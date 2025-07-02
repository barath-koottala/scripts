"""
Constants and templates for the missing email client cleanup script.
Contains all SQL templates and configuration constants used throughout the application.
"""

# Rollback script template
ROLLBACK_SCRIPT_TEMPLATE = """-- Rollback script for deletion batch: {batch_id}
-- Generated on: {timestamp}
-- 
-- INSTRUCTIONS:
-- 1. Review the records in the backup tables first:
--    SELECT * FROM account.{account_backup_table} ORDER BY person_id;
--    SELECT * FROM entity.{entity_backup_table} ORDER BY entity_id;
-- 2. If you need to restore records, run the INSERT statements below
-- 3. After successful restore, you can drop the backup tables

-- Restore deleted virtual_account_holder records
INSERT INTO account.virtual_account_holder
SELECT person_id, account_id
FROM account.{account_backup_table}
WHERE person_id NOT IN (
    SELECT person_id FROM account.virtual_account_holder WHERE person_id IS NOT NULL
);

-- Restore deleted entity records
INSERT INTO entity.entity
SELECT entity_id, entity_type, entity_subtype, created_at, updated_at, deleted_at
FROM entity.{entity_backup_table}
WHERE entity_id NOT IN (
    SELECT entity_id FROM entity.entity WHERE entity_id IS NOT NULL
);

-- Verify restore (run this to check)
-- SELECT COUNT(*) as restored_account_count FROM account.virtual_account_holder v
-- JOIN account.{account_backup_table} b ON v.person_id = b.person_id;
-- SELECT COUNT(*) as restored_entity_count FROM entity.entity e
-- JOIN entity.{entity_backup_table} b ON e.entity_id = b.entity_id;

-- After successful restore, clean up backup tables
-- DROP TABLE account.{account_backup_table};
-- DROP TABLE entity.{entity_backup_table};
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
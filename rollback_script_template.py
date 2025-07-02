"""
Rollback script template for client deletion operations.
This template gets populated with actual backup table names during execution.
"""

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
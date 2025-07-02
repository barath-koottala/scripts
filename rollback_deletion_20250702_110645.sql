-- Rollback script for deletion batch: 20250702_110645
-- Generated on: 2025-07-02T11:06:45.288246
-- 
-- INSTRUCTIONS:
-- 1. Review the records in the backup tables first:
--    SELECT * FROM account.virtual_account_holder_deletion_backup_20250702_110645 ORDER BY person_id;
--    SELECT * FROM entity.entity_deletion_backup_20250702_110645 ORDER BY entity_id;
-- 2. If you need to restore records, run the INSERT statements below
-- 3. After successful restore, you can drop the backup tables

-- Restore deleted virtual_account_holder records
INSERT INTO account.virtual_account_holder
SELECT person_id, account_id
FROM account.virtual_account_holder_deletion_backup_20250702_110645
WHERE person_id NOT IN (
    SELECT person_id FROM account.virtual_account_holder WHERE person_id IS NOT NULL
);

-- Restore deleted entity records
INSERT INTO entity.entity
SELECT entity_id, entity_type, entity_subtype, created_at, updated_at, deleted_at
FROM entity.entity_deletion_backup_20250702_110645
WHERE entity_id NOT IN (
    SELECT entity_id FROM entity.entity WHERE entity_id IS NOT NULL
);

-- Verify restore (run this to check)
-- SELECT COUNT(*) as restored_account_count FROM account.virtual_account_holder v
-- JOIN account.virtual_account_holder_deletion_backup_20250702_110645 b ON v.person_id = b.person_id;
-- SELECT COUNT(*) as restored_entity_count FROM entity.entity e
-- JOIN entity.entity_deletion_backup_20250702_110645 b ON e.entity_id = b.entity_id;

-- After successful restore, clean up backup tables
-- DROP TABLE account.virtual_account_holder_deletion_backup_20250702_110645;
-- DROP TABLE entity.entity_deletion_backup_20250702_110645;

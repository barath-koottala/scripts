-- Account backup table
CREATE TABLE IF NOT EXISTS account.virtual_account_holder_deletion_backup_20250702_114959 (
    LIKE account.virtual_account_holder INCLUDING ALL
);
ALTER TABLE account.virtual_account_holder_deletion_backup_20250702_114959 
ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ DEFAULT NOW(),
ADD COLUMN IF NOT EXISTS deletion_reason TEXT DEFAULT 'Missing email cleanup',
ADD COLUMN IF NOT EXISTS deletion_batch_id TEXT DEFAULT '20250702_114959';

-- Entity backup table
CREATE TABLE IF NOT EXISTS entity.entity_deletion_backup_20250702_114959 (
    LIKE entity.entity INCLUDING ALL
);
ALTER TABLE entity.entity_deletion_backup_20250702_114959 
ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ DEFAULT NOW(),
ADD COLUMN IF NOT EXISTS deletion_reason TEXT DEFAULT 'Missing email cleanup',
ADD COLUMN IF NOT EXISTS deletion_batch_id TEXT DEFAULT '20250702_114959';
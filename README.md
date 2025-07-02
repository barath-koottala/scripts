# Missing Email Client Cleanup Script

A comprehensive Python script for analyzing and safely deleting client records with missing email addresses from the Farther database. The script handles cleanup across multiple tables with full backup and rollback capabilities.

## Overview

This script processes a CSV file containing client IDs of clients with missing email addresses and provides two main operations:

1. **Analysis Mode** - Analyzes which clients have accounts or entity records
2. **Deletion Mode** - Safely deletes client records from database tables with full backup

## Features

- ✅ **Safe deletion** with automatic backup table creation
- ✅ **Dry run mode** for testing before actual execution  
- ✅ **Multi-table cleanup** (account.virtual_account_holder and entity.entity)
- ✅ **Rollback script generation** for easy recovery
- ✅ **Comprehensive logging** with detailed progress tracking
- ✅ **CSV output files** for analysis results
- ✅ **Transaction safety** with proper error handling

## Prerequisites

### Required Files

1. **Database Configuration**
   ```python
   # db_config.py
   DATABASE_URL = "postgresql://username:password@host:port/database?sslmode=require"
   ```

2. **Input CSV File**
   ```csv
   # missing_emails.csv
   Client ID
   123456789
   987654321
   ```

### Dependencies

Install required packages:
```bash
pip install -r requirements.txt
```

Required packages:
- `psycopg2-binary` - PostgreSQL database adapter
- Standard library modules (csv, logging, datetime, etc.)

## Usage

The script supports two main modes with different execution options:

### 1. Analysis Mode (Default)

Analyzes the client IDs without making any database changes.

```bash
python find_accounts_with_missing_emails.py analyze
# or simply
python find_accounts_with_missing_emails.py
```

**What it does:**
- Loads client IDs from `missing_emails.csv`
- Checks which clients have accounts in `account.virtual_account_holder`
- Checks which clients exist in `entity.entity` table
- Generates analysis reports and CSV files
- **No database modifications**

**Output Files:**
- `clients_with_accounts_{timestamp}.csv` - Clients that have accounts (⚠️ DO NOT DELETE)
- `clients_safe_to_delete_{timestamp}.txt` - Clients safe to delete (no accounts)
- `clients_in_entity_table_{timestamp}.csv` - Clients found in entity.entity table
- **No rollback script generated** (no deletion operations planned)

### 2. Deletion Mode

#### Dry Run (Safe Testing)

Test the deletion process without making actual changes:

```bash
python find_accounts_with_missing_emails.py delete
```

**What it does:**
- Performs all safety checks
- Shows exactly what would be deleted
- Displays SQL commands that would execute
- Shows expected backup tables and rollback script
- **Generates actual rollback script file** for review
- **No database modifications**

**Output Files:**
- All analysis files (same as analysis mode)
- `rollback_deletion_{timestamp}.sql` - Recovery script for review

**Sample Output:**
```
🔍 DRY RUN MODE - No actual deletion will occur
✓ Confirmed: All 344 clients have no accounts - safe to delete
✓ Found 156 clients in entity.entity table - will be deleted
📜 DRY RUN: Would create backup tables:
  - account.virtual_account_holder_deletion_backup_20250702_143045
  - entity.entity_deletion_backup_20250702_143045
```

#### Actual Execution (DANGER ZONE)

Perform the actual deletion with full safety measures.

The difference in the command here is the pressence of the **--execute** flag:

```bash
python find_accounts_with_missing_emails.py delete --execute
```

**What it does:**
- Creates backup tables with full audit trails
- Backs up all records before deletion
- Deletes records from both tables:
  - `account.virtual_account_holder` (where person_id matches client IDs)
  - `entity.entity` (where entity_id matches client IDs)
- Generates rollback script for recovery
- Verifies deletion success

**Safety Checks:**
- ❌ **ABORTS** if any client has accounts in virtual_account_holder
- ✅ **Proceeds** only if all clients have zero accounts
- 📝 **Logs** everything with timestamps and counts

## Database Tables Affected

### Tables Modified
1. **`account.virtual_account_holder`**
   - Deletes where `person_id` matches client IDs from CSV
   - These are account holding relationships

2. **`entity.entity`**
   - Deletes where `entity_id` matches client IDs from CSV  
   - These are core entity records

### Backup Tables Created
1. **`account.virtual_account_holder_deletion_backup_{timestamp}`**
2. **`entity.entity_deletion_backup_{timestamp}`**

Each backup table includes original columns plus audit columns:
- `deleted_at` - Timestamp of deletion
- `deletion_reason` - "Missing email cleanup"
- `deletion_batch_id` - Batch identifier for tracking

## Safety Features

### Pre-Deletion Validation
- ✅ Verifies CSV file exists and is readable
- ✅ Confirms database connectivity  
- ✅ Checks that NO client has accounts (aborts if any found)
- ✅ Shows count of entity records that will be deleted

### During Execution
- 🔄 Creates backup tables before any deletion
- 🔄 Backs up all records before deletion
- 🔄 Deletes in single transaction (all-or-nothing)
- 🔄 Verifies backup and deletion counts match

### Post-Deletion Recovery
- 📄 Generates rollback script with restore SQL for complete recovery
- 🔍 Verifies no target records remain in database
- 📊 Provides detailed success/failure reporting
- ✅ **Deletions are reversible** through backup tables and rollback scripts

## Output Files and Logs

### Log Files
- `fix_duplicate_emails_{timestamp}.log` - Detailed execution log
- Console output with progress indicators and results

### Analysis Files
```
clients_with_accounts_20250702_143045.csv      # Clients with accounts (don't delete)
clients_safe_to_delete_20250702_143045.txt     # Safe to delete (no accounts)  
clients_in_entity_table_20250702_143045.csv    # Found in entity table
```

### Deletion Files
```
rollback_deletion_20250702_143045.sql                    # Recovery script (created in both dry run and execute modes)
```

### Database Objects (only created during actual execution)
```
virtual_account_holder_deletion_backup_20250702_143045    # Backup table in DB
entity_deletion_backup_20250702_143045                   # Backup table in DB
```

## Recovery Process

If you need to restore deleted records:

1. **Review the backup tables:**
   ```sql
   SELECT * FROM account.virtual_account_holder_deletion_backup_20250702_143045;
   SELECT * FROM entity.entity_deletion_backup_20250702_143045;
   ```

2. **Run the generated rollback script:**
   ```bash
   psql -f rollback_deletion_20250702_143045.sql
   ```

3. **Verify restoration:**
   ```sql
   -- Check restored counts match backup counts
   SELECT COUNT(*) FROM account.virtual_account_holder 
   WHERE person_id IN (SELECT person_id FROM account.virtual_account_holder_deletion_backup_20250702_143045);
   ```

## Error Handling

### Common Errors and Solutions

**"No client IDs loaded from CSV"**
- Check that `missing_emails.csv` exists and has proper format
- Ensure CSV has header row "Client ID"

**"Found X clients with accounts!"**
- SAFETY FEATURE: Script prevents deletion of clients with accounts
- Review the `clients_with_accounts_{timestamp}.csv` file
- Remove those clients from input CSV or resolve accounts first

**"Database connection failed"**
- Verify `db_config.py` contains correct DATABASE_URL
- Check database server connectivity and credentials

**"Backup count mismatch"**
- Database state changed during execution
- Review logs and verify data integrity before proceeding

## Examples

### Typical Workflow

1. **First, analyze the data:**
   ```bash
   python find_accounts_with_missing_emails.py analyze
   ```
   
2. **Review the generated CSV files** to understand impact

3. **Test with dry run:**
   ```bash
   python find_accounts_with_missing_emails.py delete
   ```
   
4. **Review the dry run output** and SQL commands

5. **Execute actual deletion** (when confident):
   ```bash
   python find_accounts_with_missing_emails.py delete --execute
   ```

6. **Verify results** and keep rollback script safe

### Sample Console Output

**Analysis Mode:**
```
INFO - Connected to CockroachDB
INFO - Total accounts in virtual_account_holder table: 1,234,567
INFO - Loaded 344 client IDs from CSV
INFO - Found 0 client IDs from CSV that have accounts
INFO - Found 156 entity records from CSV client IDs in entity.entity table
INFO - Analysis Results:
INFO -   Total clients from CSV: 344
INFO -   Clients WITH accounts: 0
INFO -   Clients WITHOUT accounts: 344
INFO -   Clients in entity.entity table: 156
```

**Deletion Mode (Actual Execution):**
```
INFO - 🚨 EXECUTING ACTUAL DELETION - This will modify the database!
INFO - ✓ Created backup tables: account.virtual_account_holder_deletion_backup_20250702_143045, entity.entity_deletion_backup_20250702_143045
INFO - ✓ Backed up 0 virtual_account_holder records to virtual_account_holder_deletion_backup_20250702_143045
INFO - ✓ Backed up 156 entity records to entity_deletion_backup_20250702_143045
INFO - ✓ Deleted 0 records from account.virtual_account_holder
INFO - ✓ Deleted 156 records from entity.entity
INFO - ✓ Generated rollback script: rollback_deletion_20250702_143045.sql
INFO - ✅ SUCCESS: All clients successfully deleted
```

## Architecture

The script uses a class-based approach with the following key components:

- **`MissingEmailAnalyzer`** - Main class handling all operations
- **Template files** - External SQL templates for maintainability:
  - `rollback_script_template.py` - Rollback SQL generation
  - `backup_table_templates.py` - Backup table creation SQL

## Security Considerations

- ⚠️  **ALWAYS test with dry run first**
- ⚠️  **Keep rollback scripts** until certain deletion was successful  
- ⚠️  **Backup database** before running large deletions
- ⚠️  **Review client lists** to ensure no important accounts are deleted
- ⚠️  **Run during maintenance windows** for production systems

## Support

For issues or questions:
1. Check the generated log files for detailed error information
2. Review the CSV output files to understand data relationships
3. Use dry run mode extensively before actual execution
4. Keep rollback scripts for recovery if needed

---

**⚠️ WARNING: This script performs database deletions with full backup and rollback capabilities. While deletions can be reversed using the generated rollback scripts, always use dry run mode first and maintain proper database backups.**
#!/usr/bin/env python3
"""
Script to fix duplicate email addresses in CockroachDB person table.
This script identifies and resolves violations of unique email constraints.
"""

import psycopg2
from psycopg2.extras import RealDictCursor
import logging
from typing import List, Dict, Any
from datetime import datetime
import sys
import os
import csv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'fix_duplicate_emails_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

logger = logging.getLogger(__name__)


class MissingEmailAnalyzer:
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self.conn = None
        
    def connect(self):
        """Establish database connection"""
        try:
            self.conn = psycopg2.connect(
                self.connection_string,
                cursor_factory=RealDictCursor
            )
            self.conn.autocommit = False
            logger.info("Connected to CockroachDB")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise

    def disconnect(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            logger.info("Disconnected from database")

    def load_client_ids_from_csv(self, csv_file_path: str) -> List[int]:
        """Load client IDs from the missing_emails.csv file"""
        client_ids = []
        
        try:
            with open(csv_file_path, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    client_id = int(row['Client ID'])
                    client_ids.append(client_id)
            
            logger.info(f"Loaded {len(client_ids)} client IDs from CSV")
            return client_ids
            
        except Exception as e:
            logger.error(f"Error loading CSV file: {e}")
            raise

    def check_clients_with_accounts(self, client_ids: List[int]) -> List[Dict[str, Any]]:
        """Check which client IDs from the CSV have accounts in virtual_account_holder table"""
        if not client_ids:
            logger.warning("No client IDs provided")
            return []
        
        # Create placeholders for the IN clause
        placeholders = ','.join(['%s'] * len(client_ids))
        
        query = f"""
        SELECT 
            vah.person_id as client_id,
            COUNT(vah.account_id) as account_count,
            ARRAY_AGG(vah.account_id) as account_ids
        FROM account.virtual_account_holder vah
        WHERE vah.person_id IN ({placeholders})
        GROUP BY vah.person_id
        ORDER BY account_count DESC, vah.person_id
        """
        
        with self.conn.cursor() as cur:
            cur.execute(query, client_ids)
            results = cur.fetchall()
            
        logger.info(f"Found {len(results)} client IDs from CSV that have accounts")
        return results

    def check_clients_in_entity_table(self, client_ids: List[int]) -> List[Dict[str, Any]]:
        """Check which client IDs from the CSV exist in entity.entity table"""
        if not client_ids:
            logger.warning("No client IDs provided for entity table check")
            return []
        
        # Create placeholders for the IN clause
        placeholders = ','.join(['%s'] * len(client_ids))
        
        query = f"""
        SELECT *
        FROM entity.entity
        WHERE entity_id IN ({placeholders})
        ORDER BY entity_id
        """
        
        with self.conn.cursor() as cur:
            cur.execute(query, client_ids)
            results = cur.fetchall()
            
        logger.info(f"Found {len(results)} entity records from CSV client IDs in entity.entity table")
        return results

    def get_total_accounts_count(self) -> int:
        """Get total count of accounts in the virtual_account_holder table"""
        query = "SELECT COUNT(*) as total_accounts FROM account.virtual_account_holder"
        
        with self.conn.cursor() as cur:
            cur.execute(query)
            result = cur.fetchone()
            
        total_accounts = result['total_accounts']
        logger.info(f"Total accounts in virtual_account_holder table: {total_accounts:,}")
        return total_accounts

    def analyze_missing_email_accounts(self, csv_file_path: str):
        """Main analysis function to check which missing email clients have accounts"""
        try:
            self.connect()
            
            # Get total accounts count for context
            total_accounts = self.get_total_accounts_count()
            
            # Load client IDs from CSV
            client_ids = self.load_client_ids_from_csv(csv_file_path)
            
            # Check which ones have accounts
            clients_with_accounts = self.check_clients_with_accounts(client_ids)
            
            # Check entity table as well during analysis
            clients_in_entity = self.check_clients_in_entity_table(client_ids)
            
            # Find which client IDs from CSV have accounts
            client_ids_with_accounts = set(c['client_id'] for c in clients_with_accounts)
            client_ids_without_accounts = set(client_ids) - client_ids_with_accounts
            
            # Calculate total accounts owned by CSV clients
            total_accounts_from_csv = sum(c['account_count'] for c in clients_with_accounts)
            
            logger.info(f"Analysis Results:")
            logger.info(f"  Total accounts in database: {total_accounts:,}")
            logger.info(f"  Total clients from CSV: {len(client_ids)}")
            logger.info(f"  Clients WITH accounts: {len(client_ids_with_accounts)}")
            logger.info(f"  Clients WITHOUT accounts: {len(client_ids_without_accounts)}")
            logger.info(f"  Clients in entity.entity table: {len(clients_in_entity)}")
            logger.info(f"  Total accounts owned by CSV clients: {total_accounts_from_csv}")
            logger.info(f"  Percentage of total accounts: {(total_accounts_from_csv/total_accounts*100):.2f}%" if total_accounts > 0 else "  Percentage: N/A")
            
            # Detailed breakdown
            if clients_with_accounts:
                logger.info(f"\nClients WITH accounts (⚠️ DO NOT DELETE):")
                for client in clients_with_accounts:
                    client_id = client['client_id']
                    account_count = client['account_count']
                    account_ids = client['account_ids']
                    logger.info(f"  ⚠️  Client ID: {client_id} - {account_count} accounts - Account IDs: {account_ids}")
            
            if client_ids_without_accounts:
                logger.info(f"\nClients WITHOUT accounts (✓ Safe to delete):")
                for client_id in sorted(client_ids_without_accounts):
                    logger.info(f"  ✓  Client ID: {client_id} - No accounts found")
            
            # Write results to files
            self.write_results_to_files(clients_with_accounts, client_ids_without_accounts, clients_in_entity)
            
            return clients_with_accounts
            
        except Exception as e:
            logger.error(f"Error during analysis: {e}")
            raise
        finally:
            self.disconnect()

    def create_backup_tables(self, table_suffix: str = None) -> tuple:
        """Create backup tables for safe deletion with rollback capability"""
        if not table_suffix:
            table_suffix = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        account_backup_table = f"virtual_account_holder_deletion_backup_{table_suffix}"
        entity_backup_table = f"entity_deletion_backup_{table_suffix}"
        
        try:
            with self.conn.cursor() as cur:
                # Create account backup table
                create_account_backup_sql = f"""
                CREATE TABLE IF NOT EXISTS account.{account_backup_table} (
                    LIKE account.virtual_account_holder INCLUDING ALL
                );
                """
                
                cur.execute(create_account_backup_sql)
                
                # Add metadata columns for account backup
                alter_account_backup_sql = f"""
                ALTER TABLE account.{account_backup_table} 
                ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ DEFAULT NOW(),
                ADD COLUMN IF NOT EXISTS deletion_reason TEXT DEFAULT 'Missing email cleanup',
                ADD COLUMN IF NOT EXISTS deletion_batch_id TEXT DEFAULT '{table_suffix}';
                """
                
                cur.execute(alter_account_backup_sql)
                
                # Create entity backup table
                create_entity_backup_sql = f"""
                CREATE TABLE IF NOT EXISTS entity.{entity_backup_table} (
                    LIKE entity.entity INCLUDING ALL
                );
                """
                
                cur.execute(create_entity_backup_sql)
                
                # Add metadata columns for entity backup
                alter_entity_backup_sql = f"""
                ALTER TABLE entity.{entity_backup_table} 
                ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ DEFAULT NOW(),
                ADD COLUMN IF NOT EXISTS deletion_reason TEXT DEFAULT 'Missing email cleanup',
                ADD COLUMN IF NOT EXISTS deletion_batch_id TEXT DEFAULT '{table_suffix}';
                """
                
                cur.execute(alter_entity_backup_sql)
                self.conn.commit()
                
                logger.info(f"✓ Created backup tables: account.{account_backup_table}, entity.{entity_backup_table}")
                return account_backup_table, entity_backup_table
                
        except Exception as e:
            logger.error(f"Error creating backup tables: {e}")
            self.conn.rollback()
            raise

    def backup_records_before_deletion(self, client_ids: List[int], account_backup_table: str, entity_backup_table: str) -> tuple:
        """Backup records to deletion backup tables before deleting them"""
        if not client_ids:
            logger.warning("No client IDs provided for backup")
            return 0, 0
        
        placeholders = ','.join(['%s'] * len(client_ids))
        batch_id = account_backup_table.split('_')[-1]
        
        try:
            with self.conn.cursor() as cur:
                # Backup virtual_account_holder records
                account_backup_sql = f"""
                INSERT INTO account.{account_backup_table} 
                SELECT *, NOW(), 'Missing email cleanup', '{batch_id}'
                FROM account.virtual_account_holder
                WHERE person_id IN ({placeholders})
                """
                
                cur.execute(account_backup_sql, client_ids)
                account_backed_up_count = cur.rowcount
                
                # Backup entity records
                entity_backup_sql = f"""
                INSERT INTO entity.{entity_backup_table} 
                SELECT *, NOW(), 'Missing email cleanup', '{batch_id}'
                FROM entity.entity
                WHERE entity_id IN ({placeholders})
                """
                
                cur.execute(entity_backup_sql, client_ids)
                entity_backed_up_count = cur.rowcount
                
                self.conn.commit()
                
                logger.info(f"✓ Backed up {account_backed_up_count} virtual_account_holder records to {account_backup_table}")
                logger.info(f"✓ Backed up {entity_backed_up_count} entity records to {entity_backup_table}")
                return account_backed_up_count, entity_backed_up_count
                
        except Exception as e:
            logger.error(f"Error backing up records: {e}")
            self.conn.rollback()
            raise

    def delete_client_records(self, client_ids: List[int]) -> tuple:
        """Delete client records from account.virtual_account_holder and entity.entity tables"""
        if not client_ids:
            logger.warning("No client IDs provided for deletion")
            return 0, 0
        
        placeholders = ','.join(['%s'] * len(client_ids))
        
        try:
            with self.conn.cursor() as cur:
                # Delete from virtual_account_holder
                delete_account_sql = f"""
                DELETE FROM account.virtual_account_holder
                WHERE person_id IN ({placeholders})
                """
                
                cur.execute(delete_account_sql, client_ids)
                account_deleted_count = cur.rowcount
                
                # Delete from entity.entity
                delete_entity_sql = f"""
                DELETE FROM entity.entity
                WHERE entity_id IN ({placeholders})
                """
                
                cur.execute(delete_entity_sql, client_ids)
                entity_deleted_count = cur.rowcount
                
                self.conn.commit()
                
                logger.info(f"✓ Deleted {account_deleted_count} records from account.virtual_account_holder")
                logger.info(f"✓ Deleted {entity_deleted_count} records from entity.entity")
                return account_deleted_count, entity_deleted_count
                
        except Exception as e:
            logger.error(f"Error deleting records: {e}")
            self.conn.rollback()
            raise

    def generate_rollback_script(self, account_backup_table: str, entity_backup_table: str, rollback_file_path: str):
        """Generate SQL rollback script to restore deleted records"""
        try:
            rollback_sql = f"""-- Rollback script for deletion batch: {account_backup_table.split('_')[-1]}
-- Generated on: {datetime.now().isoformat()}
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
-- DROP TABLE entity.{entity_backup_table};"""
            
            with open(rollback_file_path, 'w', encoding='utf-8') as f:
                f.write(rollback_sql)
            
            logger.info(f"✓ Generated rollback script: {rollback_file_path}")
            
        except Exception as e:
            logger.error(f"Error generating rollback script: {e}")
            raise

    def get_sample_records_for_logging(self, client_ids: List[int], sample_size: int = 5) -> List[Dict[str, Any]]:
        """Get sample records for dry run logging"""
        if not client_ids:
            return []
        
        # Get a sample of records to show in dry run
        sample_ids = client_ids[:sample_size]
        placeholders = ','.join(['%s'] * len(sample_ids))
        
        query = f"""
        SELECT person_id, first, last, email 
        FROM person.person 
        WHERE person_id IN ({placeholders})
        ORDER BY person_id
        """
        
        with self.conn.cursor() as cur:
            cur.execute(query, sample_ids)
            results = cur.fetchall()
        
        return results

    def safe_delete_clients(self, csv_file_path: str, dry_run: bool = True):
        """Safely delete clients with missing emails using backup and rollback strategy"""
        try:
            self.connect()
            
            # Load client IDs from CSV
            client_ids = self.load_client_ids_from_csv(csv_file_path)
            
            if not client_ids:
                logger.warning("No client IDs loaded from CSV")
                return
            
            # Double-check these clients don't have accounts
            clients_with_accounts = self.check_clients_with_accounts(client_ids)
            if clients_with_accounts:
                logger.error(f"⚠️ ABORTING: Found {len(clients_with_accounts)} clients with accounts!")
                for client in clients_with_accounts:
                    logger.error(f"   Client {client['client_id']} has {client['account_count']} accounts")
                return
            
            # Check if clients exist in entity.entity table
            clients_in_entity = self.check_clients_in_entity_table(client_ids)
            
            logger.info(f"✓ Confirmed: All {len(client_ids)} clients have no accounts - safe to delete")
            if clients_in_entity:
                logger.info(f"✓ Found {len(clients_in_entity)} clients in entity.entity table - will be deleted")
            else:
                logger.info(f"✓ No clients found in entity.entity table")
            
            # Generate timestamp for this operation
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            account_backup_table = f"virtual_account_holder_deletion_backup_{timestamp}"
            entity_backup_table = f"entity_deletion_backup_{timestamp}"
            rollback_file = f'/Users/barath/Farther/scripts/rollback_deletion_{timestamp}.sql'
            
            if dry_run:
                logger.info("🔍 DRY RUN MODE - No actual deletion will occur")
                logger.info(f"🔍 DRY RUN: Complete process simulation:")
                logger.info(f"")
                
                # Show what backup tables would be created
                logger.info(f"🔍 DRY RUN: Would create backup tables:")
                logger.info(f"  - account.{account_backup_table}")
                logger.info(f"  - entity.{entity_backup_table}")
                logger.info(f"🔍 DRY RUN: Backup tables would include audit columns: deleted_at, deletion_reason, deletion_batch_id")
                
                # Show sample records that would be backed up
                sample_records = self.get_sample_records_for_logging(client_ids, 5)
                logger.info(f"🔍 DRY RUN: Would backup {len(client_ids)} records. Sample records:")
                for record in sample_records:
                    email_display = record['email'] if record['email'] else 'NULL'
                    first_name = record['first'] if record['first'] else ''
                    last_name = record['last'] if record['last'] else ''
                    logger.info(f"  📋 Client ID: {record['person_id']} ({first_name} {last_name}) - Email: {email_display}")
                if len(client_ids) > 5:
                    logger.info(f"  📋 ... and {len(client_ids) - 5} more records")
                
                # Show what files would be created and process steps
                logger.info(f"🔍 DRY RUN: Would generate rollback script: {rollback_file}")
                logger.info(f"🔍 DRY RUN: Process steps that would execute:")
                logger.info(f"  ✅ Create backup tables for both account and entity records")
                logger.info(f"  ✅ Delete records from account.virtual_account_holder (person_id)")
                logger.info(f"  ✅ Delete records from entity.entity (entity_id)")
                logger.info(f"  ✅ Generate rollback script with restoration SQL")
                
                # Show SQL commands that would be generated
                logger.info(f"\n📜 DRY RUN: This is what you would see if you ran it in --execute mode:\n")
                
                # Show backup table creation SQL
                logger.info(f"📜 DRY RUN: Backup tables creation SQL:")
                backup_create_sql = f"""-- Account backup table
CREATE TABLE IF NOT EXISTS account.{account_backup_table} (
    LIKE account.virtual_account_holder INCLUDING ALL
);
ALTER TABLE account.{account_backup_table} 
ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ DEFAULT NOW(),
ADD COLUMN IF NOT EXISTS deletion_reason TEXT DEFAULT 'Missing email cleanup',
ADD COLUMN IF NOT EXISTS deletion_batch_id TEXT DEFAULT '{timestamp}';

-- Entity backup table
CREATE TABLE IF NOT EXISTS entity.{entity_backup_table} (
    LIKE entity.entity INCLUDING ALL
);
ALTER TABLE entity.{entity_backup_table} 
ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ DEFAULT NOW(),
ADD COLUMN IF NOT EXISTS deletion_reason TEXT DEFAULT 'Missing email cleanup',
ADD COLUMN IF NOT EXISTS deletion_batch_id TEXT DEFAULT '{timestamp}';"""
                logger.info(f"```sql\n{backup_create_sql}\n```")
                
                # Show backup table schema details
                logger.info(f"\n📜 DRY RUN: Backup tables schema:")
                logger.info(f"```")
                logger.info(f"account.{account_backup_table}:")
                logger.info(f"  - person_id, account_id (from virtual_account_holder)")
                logger.info(f"")
                logger.info(f"entity.{entity_backup_table}:")
                logger.info(f"  - entity_id, entity_type, entity_subtype")
                logger.info(f"  - created_at, updated_at, deleted_at (from entity.entity)")
                logger.info(f"")
                logger.info(f"Additional audit columns for both:")
                logger.info(f"  - deleted_at (TIMESTAMPTZ) - When record was deleted")
                logger.info(f"  - deletion_reason (TEXT) - 'Missing email cleanup'")
                logger.info(f"  - deletion_batch_id (TEXT) - '{timestamp}'")
                logger.info(f"```")
                
                # Show backup records SQL
                logger.info(f"\n📜 DRY RUN: Backup records SQL (first 5 client IDs):")
                sample_ids_str = ','.join(map(str, client_ids[:5]))
                backup_insert_sql = f"""-- Backup virtual_account_holder records
INSERT INTO account.{account_backup_table} 
SELECT *, NOW(), 'Missing email cleanup', '{timestamp}'
FROM account.virtual_account_holder
WHERE person_id IN ({sample_ids_str}...);

-- Backup entity records
INSERT INTO entity.{entity_backup_table} 
SELECT *, NOW(), 'Missing email cleanup', '{timestamp}'
FROM entity.entity
WHERE entity_id IN ({sample_ids_str}...);"""
                logger.info(f"```sql\n{backup_insert_sql}\n```")
                
                # Show deletion SQL
                logger.info(f"\n📜 DRY RUN: Deletion SQL (first 5 client IDs):")
                delete_sql = f"""-- Delete from virtual_account_holder
DELETE FROM account.virtual_account_holder
WHERE person_id IN ({sample_ids_str}...);

-- Delete from entity.entity
DELETE FROM entity.entity
WHERE entity_id IN ({sample_ids_str}...);"""
                logger.info(f"```sql\n{delete_sql}\n```")
                
                # Generate and show rollback script content
                logger.info(f"\n📜 DRY RUN: Rollback script content that would be written to {rollback_file}:")
                rollback_content = f"""-- Rollback script for deletion batch: {timestamp}
-- Generated on: {datetime.now().isoformat()}

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
);"""
                logger.info(f"```sql\n{rollback_content}\n```")
                
                # Show expected log output
                logger.info(f"\n📜 DRY RUN: Expected execution log output:")
                logger.info(f"  ✓ Created backup tables: account.{account_backup_table}, entity.{entity_backup_table}")
                logger.info(f"  ✓ Backed up records from both tables")
                logger.info(f"  ✓ Deleted records from account.virtual_account_holder and entity.entity")
                logger.info(f"  ✓ Generated rollback script: {rollback_file}")
                logger.info(f"  ✅ SUCCESS: All client records successfully deleted from both tables")
                logger.info(f"  ✅ Files: {account_backup_table}, {entity_backup_table}, {rollback_file}")
                
                logger.info(f"\n🔍 DRY RUN: No database modifications performed")
                logger.info(f"🔍 DRY RUN: To execute actual deletion, run with --execute flag")
                return
            
            # ACTUAL EXECUTION MODE
            logger.info("🚨 EXECUTING ACTUAL DELETION - This will modify the database!")
            
            # Create backup tables
            account_backup_table, entity_backup_table = self.create_backup_tables(timestamp)
            
            # Backup records before deletion
            account_backed_up, entity_backed_up = self.backup_records_before_deletion(
                client_ids, account_backup_table, entity_backup_table
            )
            
            # Perform deletion
            account_deleted, entity_deleted = self.delete_client_records(client_ids)
            
            # Generate rollback script
            self.generate_rollback_script(account_backup_table, entity_backup_table, rollback_file)
            
            # Final verification - check both tables
            remaining_account_clients = self.check_clients_with_accounts(client_ids)
            remaining_entity_clients = self.check_clients_in_entity_table(client_ids)
            
            if remaining_account_clients or remaining_entity_clients:
                logger.error(f"⚠️ ERROR: Some clients still exist after deletion!")
                if remaining_account_clients:
                    logger.error(f"   {len(remaining_account_clients)} clients still in virtual_account_holder")
                if remaining_entity_clients:
                    logger.error(f"   {len(remaining_entity_clients)} clients still in entity.entity")
            else:
                logger.info(f"✅ SUCCESS: All clients successfully deleted")
                logger.info(f"   Account records deleted: {account_deleted}")
                logger.info(f"   Entity records deleted: {entity_deleted}")
                logger.info(f"✅ Backup tables: {account_backup_table}, {entity_backup_table}")
                logger.info(f"✅ Rollback script: {rollback_file}")
            
        except Exception as e:
            logger.error(f"Error during safe deletion: {e}")
            raise
        finally:
            self.disconnect()

    def write_results_to_files(self, clients_with_accounts: List[Dict[str, Any]], client_ids_without_accounts: set, clients_in_entity: List[Dict[str, Any]] = None):
        """Write analysis results to CSV files"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Write clients WITH accounts to CSV
        accounts_file = f'/Users/barath/Farther/scripts/clients_with_accounts_{timestamp}.csv'
        try:
            with open(accounts_file, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['client_id', 'account_count', 'account_ids']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                
                writer.writeheader()
                for client in clients_with_accounts:
                    writer.writerow({
                        'client_id': client['client_id'],
                        'account_count': client['account_count'],
                        'account_ids': ','.join(map(str, client['account_ids'])) if client['account_ids'] else ''
                    })
            
            logger.info(f"✓ Wrote {len(clients_with_accounts)} clients WITH accounts to: {accounts_file}")
        except Exception as e:
            logger.error(f"Error writing accounts file: {e}")
        
        # Write clients WITHOUT accounts to text file
        safe_delete_file = f'/Users/barath/Farther/scripts/clients_safe_to_delete_{timestamp}.txt'
        try:
            with open(safe_delete_file, 'w', encoding='utf-8') as txtfile:
                txtfile.write(f"# Clients from missing_emails.csv that have NO accounts\n")
                txtfile.write(f"# Generated on {datetime.now().isoformat()}\n")
                txtfile.write(f"# Total: {len(client_ids_without_accounts)} clients\n\n")
                
                for client_id in sorted(client_ids_without_accounts):
                    txtfile.write(f"{client_id}\n")
            
            logger.info(f"✓ Wrote {len(client_ids_without_accounts)} clients safe to delete to: {safe_delete_file}")
        except Exception as e:
            logger.error(f"Error writing safe delete file: {e}")
        
        # Write clients found in entity table to separate file
        if clients_in_entity:
            entity_file = f'/Users/barath/Farther/scripts/clients_in_entity_table_{timestamp}.csv'
            try:
                with open(entity_file, 'w', newline='', encoding='utf-8') as csvfile:
                    # Get all column names from the first entity record
                    fieldnames = list(clients_in_entity[0].keys()) if clients_in_entity else []
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    
                    writer.writeheader()
                    for client in clients_in_entity:
                        writer.writerow(client)
                
                logger.info(f"✓ Wrote {len(clients_in_entity)} clients found in entity.entity to: {entity_file}")
            except Exception as e:
                logger.error(f"Error writing entity file: {e}")
        else:
            logger.info("No clients found in entity.entity table - no entity file written")


def main():
    # Import database configuration from separate file
    try:
        from db_config import DATABASE_URL as CONNECTION_STRING
    except ImportError:
        print("Error: db_config.py not found. Please create it with your DATABASE_URL.")
        print("See db_config.py.example for the required format.")
        sys.exit(1)

    
    if not CONNECTION_STRING:
        print("Error: DATABASE_URL is empty in db_config.py")
        print("Please update db_config.py with your actual database connection string.")
        sys.exit(1)

    # Path to the CSV file
    csv_file_path = '/Users/barath/Farther/scripts/missing_emails.csv'
    
    # Check command line arguments for mode
    mode = 'analyze'  # Default mode
    dry_run = True    # Default to dry run for safety
    
    if len(sys.argv) > 1:
        if sys.argv[1] == 'delete':
            mode = 'delete'
            if len(sys.argv) > 2 and sys.argv[2] == '--execute':
                dry_run = False
                logger.warning("🚨 EXECUTE MODE - This will perform actual deletion!")
            else:
                logger.info("🔍 DRY RUN MODE - Use '--execute' flag to perform actual deletion")
        elif sys.argv[1] == 'analyze':
            mode = 'analyze'
        else:
            print("Usage:")
            print("  python find_accounts_with_missing_emails.py analyze           # Run analysis only")
            print("  python find_accounts_with_missing_emails.py delete            # Dry run deletion")
            print("  python find_accounts_with_missing_emails.py delete --execute  # Execute actual deletion")
            sys.exit(1)
    
    # Create analyzer
    analyzer = MissingEmailAnalyzer(CONNECTION_STRING)
    
    if mode == 'analyze':
        logger.info(f"Starting analysis of clients with missing emails at {datetime.now().isoformat()}")
        results = analyzer.analyze_missing_email_accounts(csv_file_path)
        logger.info(f"Analysis completed at {datetime.now().isoformat()}")
        
    elif mode == 'delete':
        if dry_run:
            logger.info(f"Starting DRY RUN deletion process at {datetime.now().isoformat()}")
        else:
            logger.info(f"Starting ACTUAL deletion process at {datetime.now().isoformat()}")
            
        analyzer.safe_delete_clients(csv_file_path, dry_run=dry_run)
        
        if dry_run:
            logger.info(f"Dry run completed at {datetime.now().isoformat()}")
            logger.info("To execute actual deletion, run with: delete --execute")
        else:
            logger.info(f"Deletion process completed at {datetime.now().isoformat()}")


if __name__ == "__main__":
    main()
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
# import pandas as pd

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
            breakpoint()
            self.connect()
            
            # Get total accounts count for context
            total_accounts = self.get_total_accounts_count()
            
            # Load client IDs from CSV
            client_ids = self.load_client_ids_from_csv(csv_file_path)

            breakpoint()
            
            # Check which ones have accounts
            clients_with_accounts = self.check_clients_with_accounts(client_ids)

            breakpoint()
            
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
            logger.info(f"  Total accounts owned by CSV clients: {total_accounts_from_csv}")
            logger.info(f"  Percentage of total accounts: {(total_accounts_from_csv/total_accounts*100):.2f}%" if total_accounts > 0 else "  Percentage: N/A")
            
            # Detailed breakdown
            logger.info(f"\nClients WITH accounts (⚠️ DO NOT DELETE):")
            for client in clients_with_accounts:
                client_id = client['client_id']
                account_count = client['account_count']
                account_ids = client['account_ids']
                logger.info(f"  ⚠️  Client ID: {client_id} - {account_count} accounts - Account IDs: {account_ids}")
            
            logger.info(f"\nClients WITHOUT accounts (✓ Safe to delete):")
            for client_id in sorted(client_ids_without_accounts):
                logger.info(f"  ✓  Client ID: {client_id} - No accounts found")
            
            # Write results to files
            self.write_results_to_files(clients_with_accounts, client_ids_without_accounts)
            
            return clients_with_accounts
            
        except Exception as e:
            logger.error(f"Error during analysis: {e}")
            raise
        finally:
            self.disconnect()

    def write_results_to_files(self, clients_with_accounts: List[Dict[str, Any]], client_ids_without_accounts: set):
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
    
    logger.info(f"Starting analysis of clients with missing emails at {datetime.now().isoformat()}")
    
    # Create analyzer and run analysis
    analyzer = MissingEmailAnalyzer(CONNECTION_STRING)
    results = analyzer.analyze_missing_email_accounts(csv_file_path)
    
    logger.info(f"Analysis completed at {datetime.now().isoformat()}")


if __name__ == "__main__":
    main()
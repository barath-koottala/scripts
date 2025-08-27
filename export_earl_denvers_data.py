#!/usr/bin/env python3
"""
Earl Denver's QA Data Export Tool with VPN Safety

This script exports Earl Denver's data from UAT database for import into production testing environment.
It includes VPN connection safety measures to prevent data source confusion.

Usage:
    python export_earl_denvers_data.py

Features:
- Automatic VPN connection detection
- Safe database connection validation
- Guided VPN switching with confirmations
- Data export with integrity checks
"""

import psycopg2
import json
import sys
import subprocess
import time
from typing import List, Dict, Set, Tuple, Optional
from collections import defaultdict, deque
from db_config import UAT_WRITE_DATABASE_URL, PROD_READ_URL
from psycopg2.extras import RealDictCursor
from generate_missing_data_sql import (
    get_database_connection, 
    build_foreign_key_dependency_graph,
    find_affected_records_iteratively,
    get_person_name
)

# Earl Denver's entity ID
EARL_DENVER_ENTITY_ID = "936803730128764929"  # Replace with actual entity ID

def check_vpn_connection(target_env: str) -> bool:
    """Check if connected to the correct VPN for the target environment."""
    try:
        # Test connection to environment-specific host
        if target_env == "UAT":
            # Try to resolve UAT-specific hostname
            result = subprocess.run(
                ["nslookup", "uat-cluster-7f9.aws-us-east-1.cockroachlabs.cloud"], 
                capture_output=True, text=True, timeout=5
            )
            return result.returncode == 0
        elif target_env == "PROD":
            # Try to resolve PROD-specific hostname  
            result = subprocess.run(
                ["nslookup", "prod-cluster-7gg.aws-us-east-1.cockroachlabs.cloud"], 
                capture_output=True, text=True, timeout=5
            )
            return result.returncode == 0
    except Exception as e:
        print(f"❌ Error checking VPN connection: {e}")
        return False
    
    return False

def validate_database_connection(conn, expected_env: str, entity_id: str) -> bool:
    """Validate we're connected to the correct database environment."""
    try:
        cursor = conn.cursor()
        
        # Check if Earl Denver exists in this database
        query = "SELECT COUNT(*) as count FROM entity.entity WHERE entity_id = %s"
        cursor.execute(query, (entity_id,))
        result = cursor.fetchone()
        cursor.close()
        
        record_count = result['count'] if result else 0
        
        if expected_env == "UAT":
            # Earl should exist in UAT
            if record_count > 0:
                print(f"✅ Validated UAT connection - Earl Denver found ({record_count} records)")
                return True
            else:
                print(f"❌ UAT validation failed - Earl Denver not found")
                return False
        elif expected_env == "PROD":  
            # Earl should NOT exist in PROD (we're exporting to test environment)
            if record_count == 0:
                print(f"✅ Validated PROD connection - Earl Denver not found (as expected)")
                return True
            else:
                print(f"⚠️  PROD validation warning - Earl Denver found ({record_count} records)")
                response = input("Earl Denver already exists in PROD. Continue anyway? (yes/no): ").lower()
                return response == 'yes'
                
    except Exception as e:
        print(f"❌ Database validation error: {e}")
        return False

def prompt_vpn_switch(from_env: str, to_env: str):
    """Guide user through VPN switching process."""
    print(f"\n🔄 VPN Switch Required")
    print(f"   Current: {from_env}")
    print(f"   Required: {to_env}")
    print()
    print(f"Please follow these steps:")
    print(f"1. Disconnect from {from_env} VPN")
    print(f"2. Connect to {to_env} VPN")
    print(f"3. Wait for connection to stabilize")
    print()
    
    input(f"Press Enter when connected to {to_env} VPN...")
    
    # Wait a moment for connection to stabilize
    print("⏳ Waiting for VPN connection to stabilize...")
    time.sleep(3)

def export_earl_denver_data():
    """Export Earl Denver's data from UAT for production testing."""
    
    print("🏗️  Earl Denver QA Data Export Tool")
    print("=" * 50)
    print()
    
    # Phase 1: Connect to UAT to export data
    print("📍 Phase 1: Exporting data from UAT database")
    print("-" * 45)
    
    # Check UAT VPN connection
    if not check_vpn_connection("UAT"):
        print("❌ UAT VPN connection not detected")
        prompt_vpn_switch("PROD(?)", "UAT")
        
        # Recheck after switch
        if not check_vpn_connection("UAT"):
            print("❌ Still cannot reach UAT. Please verify VPN connection.")
            response = input("Continue anyway? (yes/no): ").lower()
            if response != 'yes':
                sys.exit(1)
    else:
        print("✅ UAT VPN connection detected")
    
    # Prompt for UAT VPN connection
    print("🔄 UAT Database Connection Required")
    print("   Environment: UAT")
    print("   Purpose: Data extraction")
    print()
    print("Please ensure you are connected to UAT VPN, then press Enter to continue...")
    input()
    print("⏳ Proceeding with UAT database connection...")
    
    # Connect to UAT database
    print("🔌 Connecting to UAT database...")
    try:
        uat_conn = get_database_connection(UAT_WRITE_DATABASE_URL)
        if not uat_conn:
            print("❌ Failed to connect to UAT database")
            return False
            
        # Validate UAT connection
        if not validate_database_connection(uat_conn, "UAT", EARL_DENVER_ENTITY_ID):
            print("❌ UAT database validation failed")
            uat_conn.close()
            return False
            
    except Exception as e:
        print(f"❌ UAT connection error: {e}")
        return False
    
    print("✅ Connected to UAT database")
    
    # Get Earl's name for better identification
    first_name, last_name = get_person_name(uat_conn, EARL_DENVER_ENTITY_ID)
    print(f"👤 Found: {first_name} {last_name} (ID: {EARL_DENVER_ENTITY_ID})")
    
    # Build foreign key dependency graph
    print("🔍 Building foreign key dependency graph...")
    fk_graph = build_foreign_key_dependency_graph(uat_conn, include_all_fks=True)
    
    # Export data to SQL file
    export_filename = f"earl_denver_qa_data_export.sql"
    print(f"📤 Exporting data to {export_filename}...")
    
    try:
        with open(export_filename, 'w') as sql_file:
            # Write header
            sql_file.write(f"-- Earl Denver QA Data Export\n")
            sql_file.write(f"-- Entity ID: {EARL_DENVER_ENTITY_ID}\n") 
            sql_file.write(f"-- Name: {first_name} {last_name}\n")
            sql_file.write(f"-- Exported from: UAT\n")
            sql_file.write(f"-- Generated at: {__import__('datetime').datetime.now()}\n")
            sql_file.write(f"-- \n")
            sql_file.write(f"-- USAGE:\n")
            sql_file.write(f"-- 1. Review the generated SQL statements below\n")
            sql_file.write(f"-- 2. Connect to PROD VPN\n")
            sql_file.write(f"-- 3. Execute against production testing environment\n")
            sql_file.write(f"-- 4. Change ROLLBACK to COMMIT when ready to apply\n")
            sql_file.write(f"\nBEGIN;\n\n")
            
            # Find and export all related data (using ON CONFLICT DO NOTHING for deduplication)
            print("📤 Starting data export process...")
            print("ℹ️  Using ON CONFLICT DO NOTHING for safe insertion without existence checks")
            
            # No PROD connection needed - using conflict resolution instead
            affected_records = find_affected_records_iteratively(
                uat_conn, fk_graph, 'entity.entity', f"entity_id = '{EARL_DENVER_ENTITY_ID}'", 
                sql_file, prod_current_conn=None, use_conflict_resolution=True
            )
            
            sql_file.write(f"\n-- Change to COMMIT when ready to apply\nROLLBACK;\n")
        
        uat_conn.close()
        
        # Print export summary
        if affected_records:
            total_records = sum(info['record_count'] for info in affected_records.values())
            print(f"✅ Export completed successfully!")
            print(f"   • Affected tables: {len(affected_records)}")
            print(f"   • Total records: {total_records}")
            print(f"   • Export file: {export_filename}")
            
            # Save analysis file
            analysis_file = f"earl_denver_export_analysis.json"
            analysis_data = {
                'entity_id': EARL_DENVER_ENTITY_ID,
                'name': f"{first_name} {last_name}",
                'export_file': export_filename,
                'total_affected_tables': len(affected_records),
                'total_affected_records': total_records,
                'affected_tables': affected_records
            }
            
            with open(analysis_file, 'w') as f:
                json.dump(analysis_data, f, indent=2)
            print(f"   • Analysis file: {analysis_file}")
            
        else:
            print(f"❌ No data found for Earl Denver (Entity ID: {EARL_DENVER_ENTITY_ID})")
            return False
            
    except Exception as e:
        print(f"❌ Export error: {e}")
        if uat_conn:
            uat_conn.close()
        return False
    
    # Phase 2: Validation against PROD (optional)
    print(f"\n📍 Phase 2: Validating against PROD environment (optional)")
    print("-" * 55)
    
    response = input("Do you want to validate the export against PROD? (yes/no): ").lower()
    if response == 'yes':
        # Prompt for VPN switch
        prompt_vpn_switch("UAT", "PROD")
        
        # Check PROD VPN connection
        if not check_vpn_connection("PROD"):
            print("⚠️  PROD VPN connection not detected")
            response = input("Continue with validation anyway? (yes/no): ").lower()
            if response != 'yes':
                print("✅ Export completed. Skipping PROD validation.")
                return True
        else:
            print("✅ PROD VPN connection detected")
        
        # Connect to PROD for validation
        try:
            print("🔌 Connecting to PROD database for validation...")
            prod_conn = get_database_connection(PROD_READ_URL)
            
            if prod_conn and validate_database_connection(prod_conn, "PROD", EARL_DENVER_ENTITY_ID):
                print("✅ PROD database validation completed")
                prod_conn.close()
            else:
                print("⚠️  PROD validation had issues, but export is still valid")
                if prod_conn:
                    prod_conn.close()
                    
        except Exception as e:
            print(f"⚠️  PROD validation error: {e}")
            print("Export is still valid - validation is optional")
    
    # Final instructions
    print(f"\n🎯 EXPORT COMPLETE!")
    print(f"=" * 50)
    print(f"📋 Next Steps:")
    print(f"   1. Review the export file: {export_filename}")
    print(f"   2. Connect to PROD VPN when ready to import")
    print(f"   3. Execute the SQL against your PROD testing environment")
    print(f"   4. Change ROLLBACK to COMMIT in the SQL file when ready")
    print()
    print(f"⚠️  Important:")
    print(f"   • The SQL file includes ROLLBACK by default for safety")
    print(f"   • Test the import with ROLLBACK first")  
    print(f"   • Only change to COMMIT when you're confident")
    
    return True

if __name__ == "__main__":
    try:
        success = export_earl_denver_data()
        if success:
            print("\n✅ Script completed successfully!")
        else:
            print("\n❌ Script failed!")
            sys.exit(1)
            
    except KeyboardInterrupt:
        print("\n\n⚠️  Script interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
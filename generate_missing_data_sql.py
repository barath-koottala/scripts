#!/usr/bin/env python3
"""
CASCADE DELETE Data Restoration Tool

A comprehensive database recovery system that analyzes CASCADE DELETE relationships
to identify and restore all records affected by accidental data deletion.

OVERVIEW:
=========
This tool addresses the complex challenge of restoring data in highly interconnected
database schemas where a single record deletion can cascade to hundreds or thousands
of related records across dozens of tables. It provides:

1. **CASCADE Impact Analysis**: Maps all foreign key relationships to understand
   the full scope of data affected by CASCADE DELETE operations
2. **Intelligent Data Recovery**: Fetches actual record data from backup/restore
   clusters and generates executable SQL for restoration
3. **Constraint-Aware Processing**: Handles unique constraints, foreign key 
   violations, and referential integrity issues automatically
4. **Production-Safe Output**: Generates reviewable SQL with safety measures

KEY FEATURES:
=============
• **Comprehensive Constraint Handling**:
  - Detects and prevents unique constraint violations
  - Resolves NULL foreign key references automatically  
  - Skips orphaned child records when parents can't be restored
  - Maintains referential integrity throughout restoration

• **Intelligent Dependency Resolution**:
  - Discovers missing parent records and includes them in restoration
  - Processes tables in correct dependency order (parents before children)
  - Handles circular dependencies and complex relationship graphs

• **Production-Ready Safety**:
  - Generates SQL wrapped in transactions with ROLLBACK by default
  - Provides detailed logging of all decisions and skipped records
  - Includes existence checks to prevent duplicate insertions
  - Supports dry-run analysis without making any changes

• **Scalable Processing**:
  - Handles databases with hundreds of tables and complex relationships
  - Optimizes SQL generation to minimize statement count
  - Provides progress feedback for long-running operations

USAGE SCENARIOS:
================
1. **Accidental Person Deletion**: Restore all data for a specific person/entity
2. **CASCADE DELETE Recovery**: Recover from unintended CASCADE DELETE operations  
3. **Data Migration Cleanup**: Fix broken relationships after data migrations
4. **Backup Validation**: Verify completeness of backup/restore operations

COMMAND LINE USAGE:
===================
    python generate_missing_data_sql.py <entity_id>

    Example:
    python generate_missing_data_sql.py 1087425847487037443

OUTPUT FILES:
=============
• person_<id>_restoration.sql - Executable SQL for data restoration
• person_<id>_cascade_analysis.json - Detailed analysis of affected records

CONFIGURATION:
==============
Requires db_config.py with database connection strings:
- PROD_RESTORE_DATABASE_URL: Source database (backup/restore cluster)  
- PROD_READ_URL: Target database (current production) for existence checks

SAFETY NOTES:
=============
• Always review generated SQL before execution
• Test with ROLLBACK first, change to COMMIT when confident
• Verify target database state before and after restoration
• Keep backups of current state before running restoration

Author: Barath Koottala
Version: 2.0 - Enhanced with comprehensive constraint handling
Last Updated: 2025-08-04

SCRIPT EXECUTION FLOW:
======================
1. **main()**: Entry point - parses command line arguments and coordinates execution
2. **build_cascade_graph()**: Builds foreign key dependency graph across all database tables
3. **find_affected_records()**: Recursively discovers all records affected by CASCADE DELETE
4. **collect_insert_and_update_statements()**: Core function that processes each table:
   - get_primary_key_columns_from_constraints(): Determines primary key columns for existence checking
   - check_record_exists(): Verifies if record already exists in current database
   - fetch_related_records(): Gets missing parent records when foreign keys reference non-existent data
   - Generates INSERT statements with constraint violation handling
5. **topological_sort()**: Orders tables by dependency (parents before children)
6. **write_sql_file()**: Outputs final SQL with transaction wrapper and safety measures

DEBUG FLOW:
- Each function includes comprehensive debug logging for troubleshooting
- Primary key detection shows constraint parsing details
- Existence checking shows query execution and results
- Record processing shows skip/include decisions with reasoning
"""

import psycopg2
import json
from typing import List, Dict, Set, Tuple, Optional
from collections import defaultdict, deque
from db_config import PROD_RESTORE_DATABASE_URL, PROD_READ_URL, PROD_RESTORE_WRITE_DATABASE_URL, UAT_READ_DATABASE_URL
from psycopg2.extras import RealDictCursor
import re
from datetime import datetime
import base64


def get_database_connection(url):
    """Establish database connection"""
    try:
        connection = psycopg2.connect(
            url,
            cursor_factory=RealDictCursor
        )
        connection.autocommit = False
        print("Connected to CockroachDB")
        return connection
    except Exception as e:
        print(f"Failed to connect to database: {e}")
        raise


def check_if_record_exists(conn, table: str, record_id: str, id_column: str) -> bool:
    """Check if a record still exists in the current database"""
    try:
        cursor = conn.cursor()
        query = f"SELECT 1 FROM {table} WHERE {id_column} = %s LIMIT 1"
        cursor.execute(query, (record_id,))
        result = cursor.fetchone()
        cursor.close()
        return result is not None
    except Exception as e:
        # If we can't check, assume it doesn't exist (safer to restore)
        return False


def get_all_tables(conn) -> List[Dict[str, str]]:
    """Get all tables in the database."""
    query = """
    SELECT 
        table_schema,
        table_name
    FROM information_schema.tables 
    WHERE table_type = 'BASE TABLE'
    ORDER BY table_schema, table_name;
    """
    
    cursor = conn.cursor()
    cursor.execute(query)
    
    tables = []
    for row in cursor.fetchall():
        # Access dictionary-like row properly
        schema = row['table_schema']
        table = row['table_name']
            
        tables.append({
            'schema': schema,
            'table': table,
            'full_name': f"{schema}.{table}"
        })
    
    cursor.close()
    return tables


def get_table_foreign_key_constraints(conn, schema: str, table: str, include_all_fks: bool = False) -> List[Dict[str, any]]:
    """Get foreign key constraints for a specific table using SHOW CONSTRAINTS."""
    try:
        fk_constraints = []
        query = f"SHOW CONSTRAINTS FROM {schema}.{table};"
        cursor = conn.cursor()
        cursor.execute(query)
        
        for row in cursor.fetchall():
            # Row is already a RealDictRow (dict-like), so use it directly
            constraint_data = dict(row)
            # Check if it's a foreign key constraint
            if constraint_data.get('constraint_type') == 'FOREIGN KEY':
                details = str(constraint_data.get('details', '')).upper()
                
                # Include based on mode
                if include_all_fks or 'ON DELETE CASCADE' in details:
                    
                    # Parse foreign key details
                    details_text = constraint_data.get('details', '')
                    parsed = parse_foreign_key_details(details_text)
                
                    fk_constraints.append({
                        'table_schema': schema,
                        'table_name': table,
                        'constraint_name': constraint_data.get('constraint_name'),
                        'local_column': parsed.get('local_column'),  # Use parsed local column
                        'details': details_text,
                        'referenced_table': parsed.get('referenced_table'),
                        'referenced_column': parsed.get('referenced_column'),
                        'delete_rule': 'CASCADE' if 'ON DELETE CASCADE' in details else 'OTHER'
                    })
        
        cursor.close()
        return fk_constraints
        
    except Exception as e:
        print(f"Error getting constraints for {schema}.{table}: {e}")
        # Rollback the transaction to clear the aborted state
        try:
            conn.rollback()
        except:
            pass
        return fk_constraints  # Return what we've collected so far


def parse_foreign_key_details(details: str) -> Dict[str, str]:
    """Parse the foreign key details to extract referenced table and columns."""
    try:
        # Example: "FOREIGN KEY (beneficiary_id) REFERENCES person.person(person_id) ON DELETE CASCADE"
        if 'REFERENCES' in details:
            # Extract the local column
            local_match = re.search(r'FOREIGN KEY \(([^)]+)\)', details)
            local_column = local_match.group(1).strip() if local_match else 'Unknown'
            
            # Extract referenced table and column
            ref_part = details.split('REFERENCES')[1].split('ON DELETE')[0].strip()
            if '(' in ref_part:
                ref_table = ref_part.split('(')[0].strip()
                ref_column = ref_part.split('(')[1].split(')')[0].strip()
                return {
                    'local_column': local_column,
                    'referenced_table': ref_table,
                    'referenced_column': ref_column
                }
    except Exception:
        pass
    
    return {
        'local_column': 'Unknown',
        'referenced_table': 'Unknown',
        'referenced_column': 'Unknown'
    }


def count_affected_subtables_recursive(cascade_graph: Dict[str, List[Dict]], start_table: str, visited: set = None, depth: int = 0) -> Dict[str, Dict]:
    """
    Recursively count all subtables that would be affected by CASCADE DELETE from start_table.
    
    Args:
        cascade_graph: Dictionary mapping parent_table -> [child table info]
        start_table: The table to start cascade analysis from
        visited: Set of tables already visited (prevents infinite recursion)
        depth: Current recursion depth
        
    Returns:
        Dictionary mapping table_name -> {'depth': int, 'path': str}
    """
    if visited is None:
        visited = set()
    
    # Prevent infinite recursion from circular dependencies
    if start_table in visited:
        return {}
    
    visited.add(start_table)
    affected_tables = {}
    
    # Get all direct children of this table
    direct_children = cascade_graph.get(start_table, [])
    
    for child_info in direct_children:
        child_table = child_info['child_table']
        
        # Add this child table to results
        if child_table not in affected_tables:
            affected_tables[child_table] = {
                'depth': depth + 1,
                'path': f"{start_table} -> {child_table}"
            }
        
        # Recursively find children of this child table
        # Create a copy of visited set for this branch to allow revisiting in other branches
        child_affected = count_affected_subtables_recursive(
            cascade_graph, 
            child_table, 
            visited.copy(), 
            depth + 1
        )
        
        # Merge results, keeping the shortest path/depth for each table
        for table, info in child_affected.items():
            if table not in affected_tables or info['depth'] < affected_tables[table]['depth']:
                affected_tables[table] = {
                    'depth': info['depth'],
                    'path': f"{start_table} -> {info['path']}"
                }
    
    return affected_tables


def build_foreign_key_dependency_graph(conn, include_all_fks: bool = False) -> Dict[str, List[Dict]]:
    """
    Build a comprehensive foreign key dependency graph for the entire database.
    
    This function maps out all foreign key relationships between tables, creating a graph
    that shows which tables reference which other tables. This is essential for:
    - Understanding CASCADE DELETE impact
    - Resolving foreign key references during data restoration
    - Ensuring proper insertion order (parents before children)
    
    Args:
        conn: Database connection to query schema information
        include_all_fks (bool): If True, includes all foreign keys (not just CASCADE DELETE).
                               If False, only includes CASCADE DELETE relationships.
    
    Returns:
        Dict[str, List[Dict]]: A dictionary where:
            - Keys are parent table names (e.g., 'account.physical_account')
            - Values are lists of child relationship dictionaries containing:
                - 'child_table': Name of the child table
                - 'constraint_name': Name of the foreign key constraint
                - 'local_column': Column in child table that references parent
                - 'referenced_column': Column in parent table being referenced
                - 'on_delete': What happens on delete (CASCADE, RESTRICT, etc.)
    
    Example:
        {
            'account.physical_account': [
                {
                    'child_table': 'account.transaction',
                    'constraint_name': 'fk_account_id_ref_physical_account',
                    'local_column': 'account_id',
                    'referenced_column': 'account_id',
                    'on_delete': 'CASCADE'
                }
            ]
        }
    
    Note:
        This function processes all tables in the database and can take some time
        on large schemas. Progress is shown for every 50 tables processed.
    """
    graph_type = "ALL FOREIGN KEY" if include_all_fks else "CASCADE DELETE"
    print(f"🔍 Building {graph_type} dependency graph...")
    tables = get_all_tables(conn)
    fk_graph = defaultdict(list)
    
    total_fks = 0
    for i, table_info in enumerate(tables, 1):
        if i % 50 == 0:
            print(f"   Processed {i}/{len(tables)} tables...")
        
        schema = table_info['schema']
        table = table_info['table']
        full_name = table_info['full_name']
        # Get foreign key constraints for this table (this table is the child)
        constraints = get_table_foreign_key_constraints(conn, schema, table, include_all_fks)
        
        
        for constraint in constraints:
            parent_table = constraint['referenced_table']
            fk_graph[parent_table].append({
                'child_table': full_name,
                'child_schema': schema,
                'child_table_name': table,
                'local_column': constraint['local_column'],
                'referenced_column': constraint['referenced_column'],
                'constraint_name': constraint['constraint_name'],
                'delete_rule': constraint['delete_rule']
            })
            total_fks += 1
    
    relationship_type = "foreign key relationships" if include_all_fks else "CASCADE DELETE relationships"
    print(f"✅ Found {total_fks} {relationship_type} across {len(fk_graph)} parent tables")
    
    # Count subtables affected by deleting from entity.entity
    possible_affected_subtables = count_affected_subtables_recursive(fk_graph, 'entity.entity')
    print(f"🔍 Deleting from entity.entity would affect {len(possible_affected_subtables)} total subtables (including nested)")
    print(f"   Max depth: {max(info['depth'] for info in possible_affected_subtables.values()) if possible_affected_subtables else 0}")
    
    return dict(fk_graph)


def get_foreign_key_columns(conn, table: str, cascade_graph: Dict) -> List[str]:
    """Get all foreign key column names for a given table."""
    fk_columns = []
    
    # Find all foreign key relationships where this table is the child (from CASCADE graph)
    for parent_table, children in cascade_graph.items():
        for child in children:
            if child['child_table'] == table:
                fk_columns.append(child['local_column'])
    
    # Also get ALL foreign key columns (not just CASCADE ones)
    all_fk_relationships = get_all_foreign_key_relationships(conn, table)
    for local_column in all_fk_relationships.keys():
        if local_column not in fk_columns:
            fk_columns.append(local_column)
    
    return fk_columns


def get_table_columns(conn, table: str) -> List[str]:
    """Get all column names for a table."""
    try:
        schema, table_name = table.split('.', 1)
        query = """
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
        """
        
        cursor = conn.cursor()
        cursor.execute(query, (schema, table_name))
        columns = [row['column_name'] for row in cursor.fetchall()]
        cursor.close()
        return columns
    except Exception as e:
        print(f"Error getting columns for {table}: {e}")
        return []


def get_table_timestamp_columns(conn, table: str) -> Set[str]:
    """Get column names that are timestamp/date types for a table."""
    try:
        schema, table_name = table.split('.', 1)
        query = """
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = %s AND table_name = %s
        AND data_type IN ('timestamp', 'timestamptz', 'date', 'time', 'timetz', 'timestamp without time zone', 'timestamp with time zone')
        """
        
        cursor = conn.cursor()
        cursor.execute(query, (schema, table_name))
        timestamp_columns = {row['column_name'] for row in cursor.fetchall()}
        cursor.close()
        return timestamp_columns
    except Exception as e:
        print(f"Error getting timestamp columns for {table}: {e}")
        return set()


def get_all_foreign_key_relationships(conn, table: str) -> Dict[str, Dict[str, str]]:
    """Get ALL foreign key relationships for a table, not just CASCADE ones."""
    try:
        schema, table_name = table.split('.', 1)
        query = """
        SELECT 
            kcu.column_name as local_column,
            ccu.table_schema || '.' || ccu.table_name as referenced_table,
            ccu.column_name as referenced_column
        FROM 
            information_schema.table_constraints AS tc 
            JOIN information_schema.key_column_usage AS kcu
              ON tc.constraint_name = kcu.constraint_name
              AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage AS ccu
              ON ccu.constraint_name = tc.constraint_name
              AND ccu.table_schema = tc.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY' 
          AND tc.table_schema = %s
          AND tc.table_name = %s
        """
        
        cursor = conn.cursor()
        cursor.execute(query, (schema, table_name))
        
        fk_relationships = {}
        for row in cursor.fetchall():
            local_column = row['local_column']
            referenced_table = row['referenced_table']
            referenced_column = row['referenced_column']
            
            fk_relationships[local_column] = {
                'referenced_table': referenced_table,
                'referenced_column': referenced_column
            }
        
        cursor.close()
        return fk_relationships
        
    except Exception as e:
        print(f"Error getting foreign key relationships for {table}: {e}")
        return {}


def check_record_exists(conn, table: str, pk_columns: List[str], record: Dict) -> bool:
    """
    Check if a record already exists in the database table using primary key columns.
    
    This function prevents duplicate insertions by checking if a record with the same
    primary key values already exists in the target database. This is crucial for
    idempotent data restoration operations.
    
    Args:
        conn: Database connection to query against
        table (str): Full table name (e.g., 'account.physical_account')
        pk_columns (List[str]): List of primary key column names for the table
        record (Dict): Dictionary containing the record data to check
    
    Returns:
        bool: True if a record with matching primary key values exists, False otherwise
    
    Example:
        pk_columns = ['account_id']
        record = {'account_id': '123-456-789', 'name': 'Test Account'}
        exists = check_record_exists(conn, 'account.physical_account', pk_columns, record)
        # Returns True if account with ID '123-456-789' already exists
    
    Note:
        - Returns False if any primary key values are None (incomplete record)
        - Handles SQL escaping for string values to prevent injection
        - Returns False on database errors (assumes record doesn't exist for safety)
    """
    if not pk_columns:
        return False
    
    try:
        pk_conditions = []
        for pk_col in pk_columns:
            pk_value = record.get(pk_col)
            if pk_value is None:
                return False  # Can't check existence without all PK values
            
            if isinstance(pk_value, str):
                escaped_pk_value = pk_value.replace("'", "''")
                pk_conditions.append(f"{pk_col} = '{escaped_pk_value}'")
            elif isinstance(pk_value, bool):
                pk_conditions.append(f"{pk_col} = {'TRUE' if pk_value else 'FALSE'}")
            elif 'time' in pk_col.lower() or 'date' in pk_col.lower():
                pk_conditions.append(f"{pk_col} = '{pk_value}'")
            else:
                pk_conditions.append(f"{pk_col} = {pk_value}")
        
        if not pk_conditions:
            return False
        
        pk_where = ' AND '.join(pk_conditions)
        query = f"SELECT 1 FROM {table} WHERE {pk_where} LIMIT 1"
        
        cursor = conn.cursor()
        cursor.execute(query)
        result = cursor.fetchone()
        cursor.close()
        
        return result is not None
        
    except Exception as e:
        print(f"[DEBUG] Error checking if record exists in {table}: {e}")
        import traceback
        print(f"[DEBUG] Exception details: {traceback.format_exc()}")
        return False  # Assume doesn't exist if we can't check


def check_references_skipped_parent(table: str, record: Dict, skipped_records: Dict, cascade_graph: Dict) -> bool:
    """
    Check if a record references a parent record that was skipped due to constraint violations.
    
    This function prevents orphaned child records by detecting when a record would reference
    a parent that was not inserted due to unique constraint violations or other issues.
    This ensures referential integrity in the restored data.
    
    Args:
        table (str): Name of the child table being checked
        record (Dict): The record data being evaluated for insertion
        skipped_records (Dict): Dictionary tracking which parent records were skipped
                               Format: {table_name: {pk_column: set(skipped_values)}}
        cascade_graph (Dict): Foreign key dependency graph showing parent-child relationships
    
    Returns:
        bool: True if this record references a skipped parent and should be skipped, False otherwise
    
    Example:
        # If account.physical_account with ID '123' was skipped due to unique constraint
        skipped_records = {
            'account.physical_account': {
                'account_id': {'123'}
            }
        }
        
        # This transaction record would be skipped because it references the skipped account
        record = {'transaction_id': '456', 'account_id': '123', 'amount': 100}
        should_skip = check_references_skipped_parent('account.transaction', record, 
                                                     skipped_records, cascade_graph)
        # Returns True - this transaction should be skipped
    
    Note:
        - Uses the cascade_graph to identify foreign key relationships
        - Provides detailed logging when records are skipped
        - Critical for maintaining referential integrity during restoration
    """
    
    # Look through all foreign key relationships to see if this table references any skipped parents
    for parent_table, children in cascade_graph.items():
        for child in children:
            if child['child_table'] == table:
                # This table has a foreign key to parent_table
                fk_column = child['local_column']
                parent_pk_column = child['referenced_column']
                
                # Check if we have a value for this foreign key column
                fk_value = record.get(fk_column)
                if fk_value is None:
                    continue
                
                # Check if this parent table has any skipped records
                if parent_table in skipped_records and parent_pk_column in skipped_records[parent_table]:
                    if str(fk_value) in skipped_records[parent_table][parent_pk_column]:
                        print(f"🚫 Skipping {table} record because it references skipped {parent_table} record: {fk_column}={fk_value}")
                        return True
    
    return False


def check_unique_constraint_violation(conn, table: str, record: Dict) -> bool:
    """
    Check if inserting a record would violate any unique constraints in the database.
    
    This function proactively detects unique constraint violations before attempting
    to insert records, preventing SQL errors and ensuring smooth data restoration.
    It maintains a registry of known unique constraints for commonly problematic tables.
    
    Args:
        conn: Database connection to query against
        table (str): Full table name to check (e.g., 'account.physical_account')
        record (Dict): Dictionary containing the record data to be inserted
    
    Returns:
        bool: True if inserting this record would violate a unique constraint, False otherwise
    
    Supported Unique Constraints:
        - account.physical_account: (custodian_account_id, custodian_id)
        - account.virtual_account: (account_id) - Primary key
        - entity.entity: (entity_id) - Primary key
    
    Example:
        record = {
            'account_id': '123-456',
            'custodian_account_id': '20801419',
            'custodian_id': 4
        }
        would_violate = check_unique_constraint_violation(conn, 'account.physical_account', record)
        # Returns True if a physical_account with custodian_account_id='20801419' 
        # and custodian_id=4 already exists
    
    Note:
        - Only checks constraints defined in the unique_constraints dictionary
        - Provides detailed logging when violations are detected
        - Extensible - new constraints can be easily added to the registry
        - Returns False on database errors (assumes no violation for safety)
    """
    
    # Known unique constraints that commonly cause issues
    unique_constraints = {
        'account.physical_account': [
            ['custodian_account_id', 'custodian_id']  # physical_account_custodian_account_id_custodian_id_key
        ],
        'account.virtual_account': [
            ['account_id']  # Primary key
        ],
        'entity.entity': [
            ['entity_id']  # Primary key
        ],
        # Add more as needed
    }
    
    if table not in unique_constraints:
        return False
    
    try:
        for constraint_columns in unique_constraints[table]:
            # Check if all columns for this constraint exist in the record
            constraint_conditions = []
            has_all_columns = True
            
            for col in constraint_columns:
                col_value = record.get(col)
                if col_value is None:
                    has_all_columns = False
                    break
                
                if isinstance(col_value, str):
                    escaped_value = col_value.replace("'", "''")
                    constraint_conditions.append(f"{col} = '{escaped_value}'")
                elif isinstance(col_value, bool):
                    constraint_conditions.append(f"{col} = {'TRUE' if col_value else 'FALSE'}")
                elif 'time' in col.lower() or 'date' in col.lower():
                    constraint_conditions.append(f"{col} = '{col_value}'")
                else:
                    constraint_conditions.append(f"{col} = {col_value}")
            
            if not has_all_columns:
                continue
            
            # Check if a record with these constraint values already exists
            constraint_where = ' AND '.join(constraint_conditions)
            query = f"SELECT 1 FROM {table} WHERE {constraint_where} LIMIT 1"
            
            cursor = conn.cursor()
            cursor.execute(query)
            result = cursor.fetchone()
            cursor.close()
            
            if result is not None:
                print(f"🚨 Unique constraint violation detected in {table}: {constraint_columns} = {[record.get(col) for col in constraint_columns]}")
                return True
        
        return False
        
    except Exception as e:
        print(f"Error checking unique constraints for {table}: {e}")
        return False


def is_timestamp_column(column_name: str, timestamp_columns: Set[str]) -> bool:
    """Check if a column is a timestamp column based on database schema."""
    return column_name in timestamp_columns


def get_primary_key_columns_from_constraints(conn, table: str) -> List[str]:
    """Get primary key columns by querying SHOW CONSTRAINTS."""
    # Handle None database connection
    if conn is None:
        print(f"[WARNING] No database connection available for primary key lookup on {table}")
        return []
    
    try:
        schema, table_name = table.split('.', 1)
        query = f"SHOW CONSTRAINTS FROM {schema}.{table_name}"
        cursor = conn.cursor()
        cursor.execute(query)
        
        rows = cursor.fetchall()
        for row in rows:
            # Create constraint dictionary
            try:
                constraint_data = {}
                
                for i, col_desc in enumerate(cursor.description):
                    if i < len(row):
                        key = col_desc[0]
                        value = row[key]
                        constraint_data[key] = value
                
            except Exception:
                continue
            
            # Check if this is a PRIMARY KEY constraint
            try:
                constraint_type = constraint_data.get('constraint_type')
                
                if constraint_type == 'PRIMARY KEY':
                    # Parse the details
                    details = constraint_data.get('details', '')
                    
                    if 'PRIMARY KEY (' in details:
                        # Extract content between parentheses
                        start = details.find('PRIMARY KEY (') + len('PRIMARY KEY (')
                        end = details.find(')', start)
                        if end > start:
                            columns_part = details[start:end]
                            
                            # Split by comma and clean up (remove ASC, DESC, whitespace)
                            pk_columns = []
                            for col in columns_part.split(','):
                                col = col.strip().split()[0]  # Remove ASC/DESC
                                pk_columns.append(col)
                            
                            cursor.close()
                            return pk_columns
                    
            except Exception as pk_error:
                print(f"[ERROR] Step 4b/4c failed - PRIMARY KEY processing: {pk_error}")
                print(f"[ERROR] Error type: {type(pk_error)}")
                import traceback
                traceback.print_exc()
                continue
        
        # Cleanup and return empty if no PK found
        cursor.close()
        return []
        
    except Exception as main_error:
        print(f"[ERROR] Main exception in get_primary_key_columns_from_constraints for {table}: {main_error}")
        print(f"[ERROR] Error type: {type(main_error)}")
        import traceback
        traceback.print_exc()
        return []


def collect_insert_and_update_statements(table: str, records: List[Dict], fk_columns: List[str], all_columns: List[str], cascade_graph: Dict, processed_tables: Set[str], restore_conn, prod_current_conn=None, insert_statements_seen=None, timestamp_columns=None, restored_records=None, statement_buffer=None, skipped_records=None, use_conflict_resolution=False):
    """
    Process records and generate INSERT statements with comprehensive constraint checking.
    
    This function is the core record processing engine that handles individual records
    and determines whether they should be inserted, updated, or skipped. It performs
    multiple validation checks to ensure data integrity and prevent constraint violations.
    
    Args:
        table (str): Name of the table being processed
        records (List[Dict]): List of record dictionaries to process
        fk_columns (List[str]): Foreign key columns for this table
        all_columns (List[str]): All columns in the table
        cascade_graph (Dict): Foreign key dependency graph
        processed_tables (Set[str]): Tables already processed (for FK resolution)
        conn: Database connection to restore cluster
        current_db_conn: Optional connection to current production database
        insert_statements_seen (Set[str]): Track duplicate INSERT statements
        timestamp_columns (Set[str]): Columns containing timestamp data
        restored_records (Dict): Track successfully restored records
        statement_buffer (List[Dict]): Collect generated statements
        skipped_records (Dict): Track records skipped due to constraints
    
    Returns:
        List[Dict]: List of statement dictionaries ready for SQL generation
    
    Processing Logic for Each Record:
        1. **Data Validation**: Handle special data types (memoryview, bytes, timestamps)
        2. **Constraint Checks**: 
           - Check if record references skipped parent (orphan detection)
           - Check if record already exists (idempotency)
           - Check for unique constraint violations
        3. **Decision Making**:
           - Skip if any constraint check fails
           - Track skipped records for cascading decisions
           - Generate INSERT statement if all checks pass
        4. **Foreign Key Resolution**: Update restored_records for future FK resolution
    
    Constraint Handling:
        - **Orphan Prevention**: Skips records referencing non-existent parents
        - **Duplicate Prevention**: Avoids inserting records that already exist
        - **Unique Constraint Respect**: Prevents violations of unique constraints
        - **Cascading Skips**: Tracks skipped records so children can be skipped too
    
    Example Record Processing:
        ```python
        # Input record
        record = {
            'transaction_id': '123-456',
            'account_id': '789-012',  # References account.physical_account
            'amount': 100.50
        }
        
        # If account '789-012' was skipped due to unique constraint:
        # - This transaction will be automatically skipped
        # - Clear logging: "Skipping account.transaction record because it 
        #   references skipped account.physical_account record: account_id=789-012"
        ```
    
    Special Data Type Handling:
        - **memoryview/bytes**: Converts to base64 strings (for encrypted fields like TIN)
        - **Timestamps**: Preserves timezone information and formatting
        - **Arrays**: Handles PostgreSQL array syntax
        - **NULL values**: Properly handles None vs 'NULL' string distinctions
    
    Note:
        This function is called for every table during the restoration process.
        It's optimized for batch processing but handles each record individually
        to ensure precise constraint checking and error handling.
    """
    
    if not records:
        return []
    
    if skipped_records is None:
        skipped_records = {}
    
    if statement_buffer is None:
        statement_buffer = []
    
    if insert_statements_seen is None:
        insert_statements_seen = set()
    
    if timestamp_columns is None:
        timestamp_columns = set()
    
    if restored_records is None:
        restored_records = {}
    
    # Get primary key columns for tracking restored records
    pk_columns = get_primary_key_columns_from_constraints(restore_conn, table)
    
    # Get foreign key parent table mappings for this table from CASCADE graph
    fk_parent_tables = {}
    fk_parent_columns = {}
    for parent_table, children in cascade_graph.items():
        for child in children:
            if child['child_table'] == table:
                fk_parent_tables[child['local_column']] = parent_table
                fk_parent_columns[child['local_column']] = child['referenced_column']
    
    # Also get ALL foreign key relationships (not just CASCADE ones)
    all_fk_relationships = get_all_foreign_key_relationships(restore_conn, table)
    
    # Merge both sets of FK relationships (CASCADE takes precedence)
    for local_column, relationship in all_fk_relationships.items():
        if local_column not in fk_parent_tables:
            fk_parent_tables[local_column] = relationship['referenced_table']
            fk_parent_columns[local_column] = relationship['referenced_column']
    
    for record in records:
        # Build INSERT statement with smart foreign key handling
        insert_values = []
        update_sets = []
        where_conditions = []
        
        for column in all_columns:
            if column in fk_columns:
                # Check if the parent table for this FK has already been processed
                parent_table = fk_parent_tables.get(column)
                parent_column = fk_parent_columns.get(column)
                actual_value = record.get(column)
                
                if parent_table and parent_table in processed_tables and actual_value is not None:
                    # Parent table already processed in this restoration, use actual FK value
                    if isinstance(actual_value, str):
                        insert_values.append(f"'{actual_value}'")
                    else:
                        insert_values.append(str(actual_value))
                    # No UPDATE needed for this FK
                elif (actual_value is not None and parent_table and parent_column and 
                      str(actual_value) in restored_records.get(parent_table, {}).get(parent_column, set())):
                    # Record is being restored in this script, use actual FK value
                    if isinstance(actual_value, str):
                        insert_values.append(f"'{actual_value}'")
                    else:
                        insert_values.append(str(actual_value))
                    # No UPDATE needed for this FK
                elif (actual_value is not None and prod_current_conn and parent_table and parent_column and 
                      check_if_record_exists(prod_current_conn, parent_table, str(actual_value), parent_column)):
                    # Record still exists in current database, use actual FK value
                    if isinstance(actual_value, str):
                        insert_values.append(f"'{actual_value}'")
                    else:
                        insert_values.append(str(actual_value))
                    # No UPDATE needed for this FK
                else:
                    # Parent table not yet processed and doesn't exist in current DB, use NULL and save for UPDATE
                    insert_values.append('NULL')
                    if actual_value is not None:
                        if isinstance(actual_value, str):
                            update_sets.append(f"{column} = '{actual_value}'")
                        elif isinstance(actual_value, bool):
                            update_sets.append(f"{column} = {'TRUE' if actual_value else 'FALSE'}")
                        elif isinstance(actual_value, (bytes, memoryview)):
                            # Convert binary data to base64 string for UPDATE statements
                            if isinstance(actual_value, memoryview):
                                bytes_data = actual_value.tobytes()
                            else:
                                bytes_data = actual_value
                            b64_encoded = base64.b64encode(bytes_data).decode('ascii')
                            update_sets.append(f"{column} = '{b64_encoded}'")
                        elif 'time' in column.lower() or 'date' in column.lower():
                            update_sets.append(f"{column} = '{actual_value}'")
                        else:
                            update_sets.append(f"{column} = {actual_value}")
            else:
                # Use actual value in INSERT for non-FK columns
                value = record.get(column)
                if value is None:
                    insert_values.append('NULL')
                elif isinstance(value, bool):
                    # Convert Python boolean to SQL boolean
                    insert_values.append('TRUE' if value else 'FALSE')
                elif is_timestamp_column(column, timestamp_columns):
                    # Wrap timestamp/date values in quotes
                    insert_values.append(f"'{value}'")
                elif isinstance(value, str):
                    # Escape single quotes in strings
                    escaped_value = value.replace("'", "''")
                    insert_values.append(f"'{escaped_value}'")
                elif isinstance(value, (bytes, memoryview)):
                    # Convert binary data to base64 string for SQL storage
                    if isinstance(value, memoryview):
                        bytes_data = value.tobytes()
                    else:
                        bytes_data = value
                    b64_encoded = base64.b64encode(bytes_data).decode('ascii')
                    insert_values.append(f"'{b64_encoded}'")
                else:
                    insert_values.append(str(value))
                
                # Build WHERE condition for UPDATE using all non-FK columns
                if value is not None:
                    if isinstance(value, bool):
                        where_conditions.append(f"{column} = {'TRUE' if value else 'FALSE'}")
                    elif is_timestamp_column(column, timestamp_columns):
                        where_conditions.append(f"{column} = '{value}'")
                    elif isinstance(value, str):
                        escaped_value = value.replace("'", "''")
                        where_conditions.append(f"{column} = '{escaped_value}'")
                    elif isinstance(value, (bytes, memoryview)):
                        # Convert binary data to base64 string for WHERE conditions
                        if isinstance(value, memoryview):
                            bytes_data = value.tobytes()
                        else:
                            bytes_data = value
                        b64_encoded = base64.b64encode(bytes_data).decode('ascii')
                        where_conditions.append(f"{column} = '{b64_encoded}'")
                    else:
                        where_conditions.append(f"{column} = {value}")
        
        # Skip existence and parent checks when using ON CONFLICT DO NOTHING
        if use_conflict_resolution:
            record_exists = False
            unique_constraint_violation = False
            references_skipped_parent = False  # Let database handle conflicts
        else:
            # Check if this record references a skipped parent record
            references_skipped_parent = check_references_skipped_parent(table, record, skipped_records, cascade_graph)
            # Check if record already exists before writing INSERT statement
            pk_columns = get_primary_key_columns_from_constraints(restore_conn, table)
            record_exists = check_record_exists(prod_current_conn, table, pk_columns, record)
            
            # Also check for unique constraint violations
            unique_constraint_violation = False
            if not record_exists and prod_current_conn:
                unique_constraint_violation = check_unique_constraint_violation(prod_current_conn, table, record)
        
        # Create INSERT statement string for duplicate checking
        columns_str = ', '.join(all_columns)
        values_str = ', '.join(insert_values)
        
        # Add ON CONFLICT DO NOTHING if conflict resolution is enabled
        if use_conflict_resolution:
            insert_statement = f"INSERT INTO {table} ({columns_str}) VALUES ({values_str}) ON CONFLICT DO NOTHING;"
            # Debug output
            print(f"[DEBUG] Generated ON CONFLICT statement for {table}")
        else:
            insert_statement = f"INSERT INTO {table} ({columns_str}) VALUES ({values_str});"
            # Debug output  
            print(f"[DEBUG] Generated regular INSERT statement for {table}")
        
        if record_exists:
            # Skip existing records
            pass
        elif references_skipped_parent:
            # Skip records that reference skipped parent records
            pass
        elif unique_constraint_violation:
            # Skip records that would violate unique constraints
            # Track skipped records so we can skip dependent child records
            if pk_columns:
                if table not in skipped_records:
                    skipped_records[table] = {}
                for pk_column in pk_columns:
                    if pk_column not in skipped_records[table]:
                        skipped_records[table][pk_column] = set()
                    pk_value = record.get(pk_column)
                    if pk_value is not None:
                        skipped_records[table][pk_column].add(str(pk_value))
            pass
        elif insert_statement not in insert_statements_seen:
            # Collect INSERT statement
            insert_statements_seen.add(insert_statement)
            
            # Create statement object
            statement = {
                'type': 'INSERT',
                'table': table,
                'statement': insert_statement,
                'columns': all_columns,
                'values': {col: record.get(col) for col in all_columns},
                'record_data': record.copy()
            }
            statement_buffer.append(statement)
            
            # Track the restored record for foreign key resolution
            if pk_columns:
                if table not in restored_records:
                    restored_records[table] = {}
                for pk_column in pk_columns:
                    if pk_column not in restored_records[table]:
                        restored_records[table][pk_column] = set()
                    pk_value = record.get(pk_column)
                    if pk_value is not None:
                        restored_records[table][pk_column].add(str(pk_value))
        
        # Collect UPDATE statement if there are foreign keys to update
        if update_sets and where_conditions and not record_exists and insert_statement in insert_statements_seen:
            update_str = ', '.join(update_sets)
            where_str = ' AND '.join(where_conditions)
            update_statement = f"UPDATE {table} SET {update_str} WHERE {where_str};"
            
            # Parse update information
            update_data = {}
            where_data = {}
            
            # Extract update values from update_sets
            for update_set in update_sets:
                if ' = ' in update_set:
                    col, val = update_set.split(' = ', 1)
                    update_data[col.strip()] = val.strip().strip("'")
            
            # Extract where conditions
            for where_condition in where_conditions:
                if ' = ' in where_condition:
                    col, val = where_condition.split(' = ', 1)
                    where_data[col.strip()] = val.strip().strip("'")
            
            statement = {
                'type': 'UPDATE',
                'table': table,
                'statement': update_statement,
                'updates': update_data,
                'where': where_data
            }
            statement_buffer.append(statement)
    
    return statement_buffer


def find_affected_records_iteratively(restore_conn, cascade_graph: Dict, start_table: str, start_conditions: str, sql_file_handle, prod_current_conn=None, restored_records=None, prod_db_url=None, use_conflict_resolution=False) -> Dict:
    """
    Comprehensively find and restore all records affected by CASCADE DELETE operations.
    
    This is the main orchestration function that:
    1. Discovers all records that would be deleted by a CASCADE DELETE operation
    2. Fetches the actual record data from the restore cluster
    3. Resolves foreign key dependencies and missing parent records
    4. Handles constraint violations and orphaned child records
    5. Generates clean, executable SQL for data restoration
    
    The function processes tables level by level, ensuring proper insertion order
    and maintaining referential integrity throughout the restoration process.
    
    Args:
        conn: Database connection to the restore cluster (source of data)
        cascade_graph (Dict): Foreign key dependency graph mapping parent -> child relationships
        start_table (str): Starting table for CASCADE analysis (e.g., 'entity.entity')
        start_conditions (str): SQL WHERE clause for root records (e.g., "entity_id = '123'")
        sql_file_handle: Open file handle for writing restoration SQL statements
        current_db_conn: Optional connection to current production database for existence checks
        restored_records: Optional dictionary tracking already restored records (for idempotency)
    
    Returns:
        Dict: Analysis results containing:
            - Table names as keys
            - Record counts, conditions, and processing levels as values
    
    Processing Flow:
        1. **Discovery Phase**: Starting from root records, traverses CASCADE relationships
           to find all affected tables and record counts
        2. **Data Fetching Phase**: Retrieves actual record data from restore cluster
        3. **Constraint Resolution Phase**: 
           - Checks for existing records (idempotency)
           - Detects unique constraint violations
           - Identifies orphaned child records
        4. **SQL Generation Phase**: Creates optimized INSERT statements with:
           - Proper foreign key value resolution
           - SQL injection protection
           - Transaction safety
        5. **Missing Parent Discovery**: Finds and includes parent records that exist
           in restore cluster but are missing from production
    
    Key Features:
        - **Intelligent Constraint Handling**: Automatically skips records that would
          cause unique constraint violations or reference non-existent parents
        - **NULL Foreign Key Resolution**: Discovers and resolves NULL foreign key
          values by finding the correct parent records
        - **Cascading Skip Logic**: When a parent record is skipped, automatically
          skips all dependent child records to maintain referential integrity
        - **Comprehensive Logging**: Provides detailed progress and decision logging
        - **Production Safety**: Generates SQL with ROLLBACK by default for safe testing
    
    Example Usage:
        # Restore all data for person ID 1087425847487037443
        with open('restoration.sql', 'w') as sql_file:
            affected_records = find_affected_records_iteratively(
                conn=restore_conn,
                cascade_graph=fk_graph,
                start_table='entity.entity',
                start_conditions="entity_id = '1087425847487037443'",
                sql_file_handle=sql_file,
                current_db_conn=prod_conn
            )
    
    Output SQL Structure:
        ```sql
        BEGIN;
        
        -- Missing parent records first
        INSERT INTO account.virtual_account (...) VALUES (...);
        
        -- Root records  
        INSERT INTO entity.entity (...) VALUES (...);
        
        -- Child records in dependency order
        INSERT INTO person.person (...) VALUES (...);
        INSERT INTO account.physical_account (...) VALUES (...);
        INSERT INTO account.transaction (...) VALUES (...);
        
        ROLLBACK; -- Change to COMMIT when ready
        ```
    
    Note:
        This function can process thousands of records across hundreds of tables.
        Progress is logged every 50 tables processed. The generated SQL is designed
        to be reviewed before execution and includes safety measures like ROLLBACK.
    """
    print(f"🔍 Finding all records affected by deleting from {start_table} WHERE {start_conditions}")
    
    # VPN Switch Prompt for PROD Database Checks
    if prod_current_conn is not None and prod_db_url is not None:
        print(f"\n🔄 PROD Database Connection Required")
        print(f"   Environment: PROD")
        print(f"   Purpose: Record existence checks")
        print()
        print(f"Please follow these steps:")
        print(f"1. Keep this script running")
        print(f"2. Switch from UAT VPN to PROD VPN")
        print(f"3. Wait for VPN connection to stabilize")
        print()
        
        input(f"Press Enter when connected to PROD VPN...")
        
        # Wait longer for VPN connection to fully stabilize
        print("⏳ Waiting for VPN connection to stabilize...")
        import time
        time.sleep(10)  # Increased from 3 to 10 seconds
        
        # Re-establish PROD connection after VPN switch with retry logic
        print("🔄 Re-establishing PROD database connection after VPN switch...")
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                if prod_current_conn and hasattr(prod_current_conn, 'close'):
                    prod_current_conn.close()  # Close old connection if it's a real connection
                
                print(f"🔄 Connection attempt {retry_count + 1}/{max_retries}...")
                prod_current_conn = get_database_connection(prod_db_url)
                
                # Test the connection with a simple query
                test_cursor = prod_current_conn.cursor()
                test_cursor.execute("SELECT 1")
                test_cursor.fetchone()
                test_cursor.close()
                
                print("✅ PROD database connection re-established and tested")
                break
                
            except Exception as e:
                retry_count += 1
                print(f"❌ Connection attempt {retry_count} failed: {e}")
                
                if retry_count < max_retries:
                    print(f"⏳ Waiting 5 seconds before retry...")
                    time.sleep(5)
                    prod_current_conn = None
                else:
                    print("❌ All connection attempts failed")
                    print("⚠️  Continuing without PROD existence checks")
                    prod_current_conn = None
        
        print("✅ Continuing with PROD database existence checks...")
        print()
    
    affected_records = {}
    visited_tables = set()
    processed_tables = set()  # Track tables that have been processed for FK resolution
    insert_statements_seen = set()  # Track INSERT statements to prevent duplicates
    timestamp_columns_cache = {}  # Cache timestamp columns for each table
    
    # Initialize restored_records tracking for foreign key resolution
    if restored_records is None:
        restored_records = {}
    
    # Initialize skipped_records tracking for constraint violation handling
    skipped_records = {}
    
    # Initialize statement buffer for collecting all statements
    statement_buffer = []
    
    processing_queue = deque()
    
    # Start with the initial table
    processing_queue.append({
        'table': start_table,
        'conditions': start_conditions,
        'level': 0
    })
    
    level_stats = defaultdict(int)
    
    while processing_queue:
        current = processing_queue.popleft()
        table = current['table']
        conditions = current['conditions']
        level = current['level']
        
        # Skip if we've already processed this table with these conditions
        table_condition_key = f"{table}::{conditions}"
        if table_condition_key in visited_tables:
            continue
        visited_tables.add(table_condition_key)
        
        print(f"   Level {level}: Processing {table}")
        
        # Get actual records from current table and write SQL statements immediately
        try:
            cursor = restore_conn.cursor()
            
            # Get all records
            query = f"SELECT * FROM {table} WHERE {conditions}"
            cursor.execute(query)
            records = cursor.fetchall()
            record_count = len(records)
            cursor.close()
            
            if record_count > 0:
                # Get table structure info
                all_columns = get_table_columns(restore_conn, table)
                fk_columns = get_foreign_key_columns(restore_conn, table, cascade_graph)
                
                # Get timestamp columns for this table (cache them)
                if table not in timestamp_columns_cache:
                    timestamp_columns_cache[table] = get_table_timestamp_columns(restore_conn, table)
                timestamp_columns = timestamp_columns_cache[table]
                
                # Convert records to list of dicts
                record_dicts = [dict(record) for record in records]
                
                # Convert binary data to base64 strings for all tables
                for record_dict in record_dicts:
                    for field_name, field_value in record_dict.items():
                        if isinstance(field_value, (bytes, memoryview)):
                            if isinstance(field_value, memoryview):
                                bytes_data = field_value.tobytes()
                            else:
                                bytes_data = field_value
                            record_dict[field_name] = base64.b64encode(bytes_data).decode('ascii')
                
                # Handle special field conversions for person.person table
                if table == 'person.person':
                    valid_records = []
                    for record_dict in record_dicts:
                        # Handle TIN field conversion
                        tin_value = record_dict.get('tin')
                        if tin_value is not None:
                            if isinstance(tin_value, memoryview):
                                # Convert memoryview to base64 string
                                record_dict['tin'] = base64.b64encode(tin_value.tobytes()).decode('utf-8')
                            elif isinstance(tin_value, bytes):
                                # Convert bytes to base64 string
                                record_dict['tin'] = base64.b64encode(tin_value).decode('utf-8')
                            elif isinstance(tin_value, str):
                                # Already a string - check if it's valid base64, if so keep it as-is
                                try:
                                    # Test if it's valid base64 and of appropriate length for encryption (multiple of 16 bytes when decoded)
                                    decoded = base64.b64decode(tin_value)
                                    if len(decoded) % 16 == 0:
                                        # Valid base64 with proper length, keep as-is
                                        record_dict['tin'] = tin_value
                                    else:
                                        print(f"⚠️  Warning: person_id {record_dict.get('person_id')} has TIN with invalid length ({len(decoded)} bytes), keeping as-is")
                                        record_dict['tin'] = tin_value
                                except:
                                    # Not valid base64, encode it
                                    record_dict['tin'] = base64.b64encode(tin_value.encode('utf-8')).decode('utf-8')
                        
                        # Validate email field - ensure it's not null or blank
                        email = record_dict.get('email')
                        if not email or (isinstance(email, str) and len(email.strip()) == 0):
                            print(f"⚠️  Warning: person_id {record_dict.get('person_id')} has null/blank email, skipping INSERT")
                            continue
                        
                        valid_records.append(record_dict)
                    
                    record_dicts = valid_records
                
                # Write INSERT and UPDATE statements immediately
                statements = collect_insert_and_update_statements(table, record_dicts, fk_columns, all_columns, cascade_graph, processed_tables, restore_conn, prod_current_conn, insert_statements_seen, timestamp_columns, restored_records, statement_buffer, skipped_records, use_conflict_resolution)
                
                # Mark this table as processed for future FK resolution
                processed_tables.add(table)
                
                # Track for summary (without storing actual records)
                affected_records[table] = {
                    'record_count': record_count,
                    'conditions': conditions,
                    'level': level
                }
                level_stats[level] += record_count
                print(f"      → Found {record_count} records, wrote SQL statements")
            else:
                print(f"      → No records found")
                continue
                
        except Exception as e:
            print(f"      → Error querying {table}: {e}")
            # Rollback the transaction to clear the aborted state
            try:
                restore_conn.rollback()
            except:
                pass
            continue
        
        # Find all child tables that would cascade delete from this table
        if table in cascade_graph:
            children = cascade_graph[table]
            print(f"      → Has {len(children)} child tables with CASCADE DELETE")
            
            for child_info in children:
                child_table = child_info['child_table']
                local_column = child_info['local_column']
                referenced_column = child_info['referenced_column']
                
                # Build conditions for child table based on parent records
                # We need to find all child records where local_column matches parent referenced_column
                try:
                    cursor = restore_conn.cursor()
                    
                    # Create subquery to get matching parent values
                    parent_values_query = f"SELECT DISTINCT {referenced_column} FROM {table} WHERE {conditions}"
                    
                    # Handle multi-column foreign keys - add parentheses if there are commas
                    if ',' in local_column:
                        child_conditions = f"({local_column}) IN ({parent_values_query})"
                    else:
                        child_conditions = f"{local_column} IN ({parent_values_query})"
                    
                    # Add child to processing queue
                    processing_queue.append({
                        'table': child_table,
                        'conditions': child_conditions,
                        'level': level + 1
                    })
                    
                    cursor.close()
                    
                except Exception as e:
                    print(f"      → Error building conditions for child {child_table}: {e}")
                    continue
        else:
            print(f"      → Table {table} not found in cascade graph with {len(cascade_graph)} keys")
            if table == 'entity.entity':
                print(f"         Available keys containing 'entity': {[k for k in cascade_graph.keys() if 'entity' in k]}")
    
    # Discover and add missing parent records that exist in restore cluster but not in prod
    print(f"\n🔍 Checking for missing parent records...")
    missing_parents_added = discover_missing_parent_records(restore_conn, prod_current_conn, cascade_graph, statement_buffer, affected_records)
    if missing_parents_added > 0:
        print(f"✅ Added {missing_parents_added} missing parent records to restoration")
    
    # Post-process statements: merge INSERT + UPDATE pairs into optimized INSERTs
    optimized_statements = optimize_statements(statement_buffer)
    
    # Fix NULL foreign keys by replacing them with actual values from restored records
    fixed_statements = fix_null_foreign_keys(optimized_statements, cascade_graph)
    
    # Write optimized statements to file
    write_statements_to_file(sql_file_handle, fixed_statements)
    
    print(f"\n📊 CASCADE DELETE Impact Summary:")
    print(f"   • Total affected tables: {len(affected_records)}")
    total_records = sum(info['record_count'] for info in affected_records.values())
    print(f"   • Total affected records: {total_records}")
    
    for level in sorted(level_stats.keys()):
        print(f"   • Level {level}: {level_stats[level]} records")
    
    return affected_records


def discover_missing_parent_records(restore_conn, prod_current_conn, cascade_graph: Dict, statement_buffer: List[Dict], affected_records: Dict) -> int:
    """
    Discover parent records that exist in restore cluster but are missing from prod.
    Add them to the statement buffer for restoration.
    """
    if not prod_current_conn:
        return 0
    
    missing_parents_added = 0
    referenced_ids_to_check = set()
    
    # Create caches for existence checks to avoid repeated queries
    current_db_exists_cache = {}  # {(table, column, value): bool}
    restore_db_exists_cache = {}  # {(table, column, value): bool}
    fk_relationships_cache = {}   # {table: relationships_dict}
    
    print("   → Building FK reference map...")
    
    # Extract all foreign key references from existing statements
    for stmt in statement_buffer:
        if stmt['type'] == 'INSERT':
            table = stmt['table']
            
            # Cache FK relationships per table to avoid repeated queries
            if table not in fk_relationships_cache:
                fk_relationships_cache[table] = get_all_foreign_key_relationships(restore_conn, table)
            
            all_fk_relationships = fk_relationships_cache[table]
            
            for fk_column, relationship in all_fk_relationships.items():
                referenced_table = relationship['referenced_table']
                referenced_column = relationship['referenced_column']
                referenced_value = stmt['values'].get(fk_column)
                
                if referenced_value is not None and referenced_value != 'NULL':
                    cache_key = (referenced_table, referenced_column, str(referenced_value))
                    
                    # Check current DB existence (with caching)
                    if cache_key not in current_db_exists_cache:
                        current_db_exists_cache[cache_key] = check_if_record_exists(
                            prod_current_conn, referenced_table, str(referenced_value), referenced_column
                        )
                    
                    if not current_db_exists_cache[cache_key]:
                        # Check restore DB existence (with caching) 
                        if cache_key not in restore_db_exists_cache:
                            restore_db_exists_cache[cache_key] = check_if_record_exists(
                                restore_conn, referenced_table, str(referenced_value), referenced_column
                            )
                        
                        if restore_db_exists_cache[cache_key]:
                            referenced_ids_to_check.add(cache_key)
    
    print(f"   → Found {len(referenced_ids_to_check)} potentially missing parent records")
    print(f"   → Cache stats: {len(current_db_exists_cache)} current DB checks, {len(restore_db_exists_cache)} restore DB checks")
    
    # For each missing parent record, fetch and add to restoration
    for referenced_table, referenced_column, referenced_value in referenced_ids_to_check:
        # Check if this specific record is already in the statement buffer
        record_already_exists = False
        for stmt in statement_buffer:
            if (stmt['type'] == 'INSERT' and 
                stmt['table'] == referenced_table and 
                stmt['values'].get(referenced_column) == referenced_value):
                record_already_exists = True
                break
        
        if record_already_exists:
            continue
        
        try:
            # Fetch the missing parent record from restore cluster
            cursor = restore_conn.cursor()
            query = f"SELECT * FROM {referenced_table} WHERE {referenced_column} = %s"
            cursor.execute(query, (referenced_value,))
            parent_records = cursor.fetchall()
            cursor.close()
            
            
            if parent_records:
                print(f"   → Found missing parent: {referenced_table} ({len(parent_records)} records)")
                
                
                # Get table metadata
                all_columns = get_table_columns(restore_conn, referenced_table)
                fk_columns = get_foreign_key_columns(restore_conn, referenced_table, cascade_graph)
                timestamp_columns = get_table_timestamp_columns(restore_conn, referenced_table)
                
                
                # Convert to record dicts
                record_dicts = []
                for record in parent_records:
                    record_dict = dict(record)
                    record_dicts.append(record_dict)
                
                # Create INSERT statements for missing parent records
                parent_statements = collect_insert_and_update_statements(
                    referenced_table, record_dicts, fk_columns, all_columns, 
                    cascade_graph, set(), restore_conn, prod_current_conn, set(), timestamp_columns, {}, [], {}, use_conflict_resolution
                )
                
                
                # Add parent statements to the beginning of statement buffer (so they're restored first)
                statement_buffer[:0] = parent_statements
                missing_parents_added += len(parent_statements)
                
                # Track this in affected_records (but don't prevent other records from same table)
                if referenced_table not in affected_records:
                    affected_records[referenced_table] = {
                        'record_count': len(record_dicts),
                        'conditions': f"{referenced_column} = '{referenced_value}'",
                        'level': -1  # Special level for discovered parents
                    }
                else:
                    # Update existing entry to include multiple conditions
                    existing = affected_records[referenced_table]
                    existing['record_count'] += len(record_dicts)
                    if f"{referenced_column} = '{referenced_value}'" not in existing['conditions']:
                        existing['conditions'] += f" OR {referenced_column} = '{referenced_value}'"
                
        except Exception as e:
            print(f"   → Error fetching parent {referenced_table}: {e}")
            import traceback
            continue
    
    return missing_parents_added


def optimize_statements(statement_buffer: List[Dict]) -> List[Dict]:
    """Optimize INSERT + UPDATE pairs into single INSERT statements."""
    
    # Group statements by table
    tables = {}
    for stmt in statement_buffer:
        table = stmt['table']
        if table not in tables:
            tables[table] = {'inserts': [], 'updates': []}
        
        if stmt['type'] == 'INSERT':
            tables[table]['inserts'].append(stmt)
        elif stmt['type'] == 'UPDATE':
            tables[table]['updates'].append(stmt)
    
    optimized = []
    
    for table, statements in tables.items():
        inserts = statements['inserts']
        updates = statements['updates']
        
        # Find INSERT + UPDATE pairs that can be merged
        merged_inserts = []
        used_updates = set()
        
        for insert_stmt in inserts:
            # Look for a matching UPDATE for this INSERT
            matching_update = None
            
            for i, update_stmt in enumerate(updates):
                if i in used_updates:
                    continue
                
                # Check if UPDATE targets the same record as INSERT
                if records_match(insert_stmt, update_stmt):
                    matching_update = update_stmt
                    used_updates.add(i)
                    break
            
            if matching_update:
                # Merge INSERT + UPDATE into optimized INSERT
                merged_stmt = merge_insert_update(insert_stmt, matching_update)
                merged_inserts.append(merged_stmt)
            else:
                # Keep original INSERT
                merged_inserts.append(insert_stmt)
        
        optimized.extend(merged_inserts)
        
        # Add any unmatched UPDATEs (shouldn't happen in normal cases)
        for i, update_stmt in enumerate(updates):
            if i not in used_updates:
                optimized.append(update_stmt)
    
    return optimized


def records_match(insert_stmt: Dict, update_stmt: Dict) -> bool:
    """Check if INSERT and UPDATE target the same record."""
    
    # Compare based on WHERE conditions in UPDATE vs INSERT values
    for where_col, where_val in update_stmt['where'].items():
        insert_val = insert_stmt['values'].get(where_col)
        if str(insert_val) != str(where_val):
            return False
    
    return True


def merge_insert_update(insert_stmt: Dict, update_stmt: Dict) -> Dict:
    """Merge INSERT + UPDATE into a single optimized INSERT."""
    
    # Start with original INSERT values
    merged_values = insert_stmt['values'].copy()
    
    # Apply UPDATE changes
    for update_col, update_val in update_stmt['updates'].items():
        # Remove quotes if present
        if isinstance(update_val, str) and update_val.startswith("'") and update_val.endswith("'"):
            update_val = update_val[1:-1]
        merged_values[update_col] = update_val
    
    # Build new INSERT statement with merged values
    columns = insert_stmt['columns']
    values_list = []
    
    for col in columns:
        value = merged_values.get(col)
        if value is None:
            values_list.append('NULL')
        elif isinstance(value, bool):
            values_list.append('TRUE' if value else 'FALSE')
        elif isinstance(value, str):
            escaped_value = value.replace("'", "''")
            values_list.append(f"'{escaped_value}'")
        else:
            values_list.append(str(value))
    
    columns_str = ', '.join(columns)
    values_str = ', '.join(values_list)
    merged_statement = f"INSERT INTO {insert_stmt['table']} ({columns_str}) VALUES ({values_str});"
    
    return {
        'type': 'INSERT',
        'table': insert_stmt['table'],
        'statement': merged_statement,
        'columns': columns,
        'values': merged_values,
        'optimized': True
    }


def fix_null_foreign_keys(statements: List[Dict], fk_graph: Dict) -> List[Dict]:
    """
    Resolve NULL foreign key values by finding and substituting correct references.
    
    This function addresses a common issue in CASCADE DELETE restoration where foreign
    key columns contain NULL values instead of proper references. It analyzes all
    INSERT statements to build a map of available records, then substitutes NULL
    foreign keys with actual values from records being restored.
    
    Args:
        statements (List[Dict]): List of INSERT statement dictionaries to process
        fk_graph (Dict): Foreign key dependency graph for relationship mapping
    
    Returns:
        List[Dict]: List of statements with NULL foreign keys resolved to actual values
    
    Resolution Process:
        1. **Record Mapping**: Build a map of all records being inserted:
           - Maps table -> primary_key_value -> full_record_data
           - Identifies potential foreign key targets
        
        2. **NULL Detection**: For each INSERT statement:
           - Scan for NULL or 'NULL' values in foreign key columns
           - Use fk_graph to identify which parent table the FK references
        
        3. **Value Substitution**: When a NULL FK is found:
           - Look up the parent table in the insertion map
           - Select the first available record from that parent table
           - Replace NULL with the actual primary key value
        
        4. **SQL Regeneration**: Recreate the INSERT statement with fixed values
    
    Example Transformation:
        ```sql
        -- Before (broken - will cause FK constraint error):
        INSERT INTO account_group.account_group_account 
        (account_group_id, virtual_account_id) 
        VALUES ('abc-123', NULL);
        
        -- After (fixed - references actual virtual_account being restored):
        INSERT INTO account_group.account_group_account 
        (account_group_id, virtual_account_id) 
        VALUES ('abc-123', 'e9086927-67fe-4e60-b2cb-c52af22aa0b8');
        ```
    
    Key Features:
        - **Automatic Discovery**: Finds available parent records from the restoration set
        - **Relationship Awareness**: Uses foreign key graph to match columns to parent tables
        - **SQL Regeneration**: Creates properly formatted SQL with correct quoting
        - **Comprehensive Logging**: Reports all foreign key fixes applied
    
    Common Use Cases:
        - Restoring records where parent was created after child in the backup
        - Handling circular foreign key dependencies
        - Fixing broken references in CASCADE DELETE scenarios
    
    Note:
        This function assumes that if a parent record is being restored, it's the
        correct one to reference. For more complex scenarios with multiple possible
        parents, additional business logic may be needed.
    """
    
    # Build a map of all records being inserted: table -> primary_key_value -> record_data
    inserted_records = {}
    
    for stmt in statements:
        if stmt['type'] == 'INSERT':
            table = stmt['table']
            if table not in inserted_records:
                inserted_records[table] = {}
            
            # Try to identify the primary key value for this record
            # For most tables, this will be an ID column
            record_data = stmt['values']
            
            # Common primary key patterns
            pk_candidates = ['id', 'account_id', 'entity_id', 'person_id', 'group_id']
            for col_name, col_value in record_data.items():
                if any(pk in col_name.lower() for pk in pk_candidates):
                    if col_value and col_value != 'NULL':
                        # Remove quotes from string values
                        clean_value = col_value.strip("'") if isinstance(col_value, str) and col_value.startswith("'") else col_value
                        inserted_records[table][clean_value] = record_data
                        break
    
    # Fix NULL foreign keys
    fixed_statements = []
    
    for stmt in statements:
        if stmt['type'] == 'INSERT':
            table = stmt['table']
            values = stmt['values'].copy()  # Make a copy to avoid modifying original
            
            # Check each column for NULL foreign keys
            for col_name, col_value in values.items():
                if col_value == 'NULL' or col_value is None:
                    # Find the parent table for this foreign key
                    parent_table = None
                    parent_column = None
                    
                    # Look in FK graph for this table's foreign keys
                    for parent_table_name, children in fk_graph.items():
                        for child in children:
                            if (child['child_table'] == table and 
                                child['local_column'] == col_name):
                                parent_table = parent_table_name
                                parent_column = child['referenced_column']
                                break
                        if parent_table:
                            break
                    
                    # If we found the parent table and it's being restored, find the reference
                    if parent_table and parent_table in inserted_records:
                        # For virtual_account_id -> account.virtual_account.account_id
                        # We need to find which account.virtual_account record to reference
                        
                        # This is complex to determine automatically, so for now use a heuristic:
                        # Use the first record being inserted for that parent table
                        if inserted_records[parent_table]:
                            # Get the first available record
                            first_record_key = next(iter(inserted_records[parent_table].keys()))
                            values[col_name] = f"'{first_record_key}'"
                            print(f"🔧 Fixed NULL FK: {table}.{col_name} -> {parent_table}.{parent_column} = '{first_record_key}'")
            
            # Create new statement with fixed values and regenerated SQL
            fixed_stmt = stmt.copy()
            fixed_stmt['values'] = values
            
            # Regenerate SQL statement from fixed values
            columns = list(values.keys())
            value_list = []
            
            for col in columns:
                val = values[col]
                if val is None or str(val) == 'NULL':
                    value_list.append('NULL')
                elif isinstance(val, str):
                    # Quote string values (including UUIDs)
                    if val.startswith("'") and val.endswith("'"):
                        value_list.append(val)  # Already quoted
                    else:
                        value_list.append(f"'{val}'")
                elif isinstance(val, bool):
                    value_list.append('TRUE' if val else 'FALSE')
                elif isinstance(val, (int, float)):
                    value_list.append(str(val))
                else:
                    # Handle other types like timestamps, arrays, etc.
                    value_list.append(f"'{str(val)}'")
            
            columns_str = ', '.join(columns)
            values_str = ', '.join(value_list)
            new_sql = f"INSERT INTO {table} ({columns_str}) VALUES ({values_str});"
            fixed_stmt['statement'] = new_sql
            
            fixed_statements.append(fixed_stmt)
        else:
            # Keep non-INSERT statements as-is
            fixed_statements.append(stmt)
    
    return fixed_statements


def filter_computed_columns_from_statement(statement: str) -> str:
    """Remove computed columns from INSERT statements."""
    if not statement.startswith('INSERT INTO'):
        return statement
    
    # Extract the table name, columns, and values
    try:
        # Parse: INSERT INTO table_name (col1, col2, crdb_internal_col, col3) VALUES (val1, val2, NULL, val3);
        insert_part = statement.split('VALUES')[0].strip()
        values_part = statement.split('VALUES')[1].strip()
        
        # Extract columns from INSERT INTO table (col1, col2, ...) 
        columns_start = insert_part.find('(')
        columns_end = insert_part.find(')')
        if columns_start == -1 or columns_end == -1:
            return statement
            
        table_part = insert_part[:columns_start].strip()
        columns_text = insert_part[columns_start+1:columns_end]
        columns = [col.strip() for col in columns_text.split(',')]
        
        # Extract values from VALUES (val1, val2, ...)
        values_start = values_part.find('(')
        values_end = values_part.rfind(')')
        if values_start == -1 or values_end == -1:
            return statement
            
        values_text = values_part[values_start+1:values_end]
        values = []
        
        # Parse values more carefully to handle commas inside quoted strings
        current_value = ""
        in_quotes = False
        paren_depth = 0
        
        for char in values_text:
            if char == "'" and (len(current_value) == 0 or current_value[-1] != '\\'):
                in_quotes = not in_quotes
            elif char == '(' and not in_quotes:
                paren_depth += 1
            elif char == ')' and not in_quotes:
                paren_depth -= 1
            elif char == ',' and not in_quotes and paren_depth == 0:
                values.append(current_value.strip())
                current_value = ""
                continue
            
            current_value += char
        
        if current_value.strip():
            values.append(current_value.strip())
        
        # Filter out crdb_internal columns
        filtered_columns = []
        filtered_values = []
        
        for i, col in enumerate(columns):
            if not col.startswith('crdb_internal_') and i < len(values):
                filtered_columns.append(col)
                filtered_values.append(values[i])
        
        if len(filtered_columns) != len(columns):
            # Rebuild the statement
            new_columns = ', '.join(filtered_columns)
            new_values = ', '.join(filtered_values)
            return f"{table_part}({new_columns}) VALUES ({new_values});"
        
    except Exception as e:
        # If parsing fails, return original statement
        print(f"Warning: Could not filter computed columns from statement: {e}")
    
    return statement


def write_statements_to_file(sql_file_handle, statements: List[Dict]):
    """Write optimized statements to SQL file grouped by table."""
    
    # Group by table for better organization
    tables = {}
    for stmt in statements:
        table = stmt['table']
        if table not in tables:
            tables[table] = []
        tables[table].append(stmt)
    
    # Write statements grouped by table
    for table, table_statements in tables.items():
        if table_statements:
            insert_count = len([s for s in table_statements if s['type'] == 'INSERT'])
            sql_file_handle.write(f"\n-- {table}: {insert_count} records\n")
            
            for stmt in table_statements:
                # Filter out computed columns from the statement
                filtered_statement = filter_computed_columns_from_statement(stmt['statement'])
                sql_file_handle.write(f"{filtered_statement}\n")
            
            sql_file_handle.write("\n")


def get_person_name(conn, person_id: str) -> Tuple[str, str]:
    """Get the first and last name for a person_id from the restore database."""
    try:
        cursor = conn.cursor()
        query = "SELECT first, last FROM person.person WHERE person_id = %s"
        cursor.execute(query, (person_id,))
        result = cursor.fetchone()
        cursor.close()
        
        if result:
            return result['first'] or 'Unknown', result['last'] or 'Unknown'
        else:
            return 'Unknown', 'Unknown'
    except Exception as e:
        print(f"Warning: Could not retrieve person name: {e}")
        return 'Unknown', 'Unknown'


def save_analysis_files(affected_records: Dict, queries: List[str], script_lines: List[str], person_id: str) -> Tuple[str, str, str]:
    """Save analysis results to files."""
    
    # Save analysis JSON
    analysis_filename = f"person_{person_id}_cascade_analysis.json"
    analysis_data = {
        'person_id': person_id,
        'total_affected_tables': len(affected_records),
        'total_affected_records': sum(info['record_count'] for info in affected_records.values()),
        'affected_tables': affected_records
    }
    
    with open(analysis_filename, 'w') as f:
        json.dump(analysis_data, f, indent=2)
    
    # Save extraction queries
    queries_filename = f"person_{person_id}_data_extraction.sql"
    with open(queries_filename, 'w') as f:
        f.write('\n'.join(queries))
    
    # Save restoration script
    script_filename = f"person_{person_id}_cascade_restoration.sql"
    with open(script_filename, 'w') as f:
        f.write('\n'.join(script_lines))
    
    return analysis_filename, queries_filename, script_filename


def main():
    """Main function."""
    import sys
    
    if len(sys.argv) != 2:
        print("Usage: python generate_missing_data_sql.py <entity_id>")
        print("Example: python generate_missing_data_sql.py 1234567890123456789")
        sys.exit(1)
    
    person_id = sys.argv[1]
    
    print(f"🔍 Entity CASCADE DELETE Data Analysis")
    print(f"Entity ID: {person_id}")
    print()
    
    # Connect to restore database (contains the data we need to restore)
    conn = get_database_connection(UAT_READ_DATABASE_URL)
    if not conn:
        return
    
    # Connect to current database (to check what still exists)
    current_db_conn = None
    try:
        current_db_conn = get_database_connection(PROD_READ_URL)
        print("Connected to current database for existence checks")
    except Exception as e:
        print(f"Warning: Could not connect to current database: {e}")
        print("Will proceed without existence checks (may restore some existing data)")
    
    try:
        # Build comprehensive foreign key graph (includes all FKs, not just CASCADE)
        fk_graph = build_foreign_key_dependency_graph(conn, include_all_fks=True)
        
        # Open SQL file for writing restoration statements
        sql_filename = f"person_{person_id}_restoration.sql"
        
        # Get person name for better identification
        first_name, last_name = get_person_name(conn, person_id)
        
        # Find all affected records starting from entity.entity
        start_table = 'entity.entity'
        start_conditions = f"entity_id = '{person_id}'"
        
        with open(sql_filename, 'w') as sql_file:
            # Write header comments at the top
            sql_file.write(f"-- Entity Data Restoration SQL\n")
            sql_file.write(f"-- Entity ID: {person_id}\n")
            sql_file.write(f"-- Person Name: {first_name} {last_name}\n")
            sql_file.write(f"-- Generated at: {__import__('datetime').datetime.now()}\n")
            sql_file.write(f"-- \n")
            sql_file.write(f"-- This script contains INSERT statements with UPDATE statements only when necessary\n")
            sql_file.write(f"-- for foreign key references that don't exist in the current database\n")
            sql_file.write(f"-- \n")
            sql_file.write(f"-- USAGE:\n")
            sql_file.write(f"-- 1. Review the generated SQL statements below\n")
            sql_file.write(f"-- 2. Change ROLLBACK to COMMIT when ready to apply\n")
            sql_file.write(f"-- 3. Execute in a transaction for safety\n")
            sql_file.write(f"\nBEGIN;\n\n")
            
            affected_records = find_affected_records_iteratively(
                conn, fk_graph, start_table, start_conditions, sql_file, current_db_conn, prod_db_url=PROD_READ_URL, use_conflict_resolution=False
            )
            
            # Write footer
            sql_file.write(f"\nROLLBACK;\n")
        
        if not affected_records:
            print(f"❌ No records found for person_id {person_id}")
            return
        
        # Save analysis summary
        analysis_file = f"person_{person_id}_cascade_analysis.json"
        analysis_data = {
            'person_id': person_id,
            'total_affected_tables': len(affected_records),
            'total_affected_records': sum(info['record_count'] for info in affected_records.values()),
            'affected_tables': affected_records
        }
        
        with open(analysis_file, 'w') as f:
            json.dump(analysis_data, f, indent=2)
        
        # Print summary
        total_records = sum(info['record_count'] for info in affected_records.values())
        
        print(f"\n📊 CASCADE DELETE ANALYSIS COMPLETE")
        print(f"   • Person: {first_name} {last_name} (ID: {person_id})")
        print(f"   • Affected tables: {len(affected_records)}")
        print(f"   • Total records: {total_records}")
        print(f"   • Analysis: {analysis_file}")
        print(f"   • Restoration SQL: {sql_filename}")
        
        print(f"\n📋 AFFECTED TABLES BY LEVEL:")
        sorted_tables = sorted(affected_records.items(), key=lambda x: x[1]['level'])
        for table, info in sorted_tables:
            print(f"   Level {info['level']}: {table} ({info['record_count']} records)")
        
        print(f"\n⚠️  NEXT STEPS:")
        print(f"   1. Review the generated restoration SQL: {sql_filename}")
        print(f"   2. Test restoration with ROLLBACK first")
        print(f"   3. Change ROLLBACK to COMMIT when ready to apply")
        
    except Exception as e:
        print(f"❌ Error during analysis: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()
        if current_db_conn:
            current_db_conn.close()


if __name__ == "__main__":
    main()
import psycopg2
import json
import csv
from psycopg2.extras import RealDictCursor, execute_values
from db_config import UAT_RESTORE_DATABASE_URL, UAT_WRITE_DATABASE_URL

# Constants
EXCLUDED_PERSON_ID = '1050546708150026243'
RESTORE_EMAIL_SUFFIX = 'restore@farther.com'

# 1. Connect to your DB
def connect(url):
    """Establish database connection"""
    try:
        connection = psycopg2.connect(
            url,
            cursor_factory=RealDictCursor
        )
        connection.autocommit = False # Ensure we can control transactions
        print("Connected to CockroachDB")
        return connection
    except Exception as e:
        print(f"Failed to connect to database: {e}")
        raise

def get_virtual_account_subquery():
    """Returns the virtual account count subquery used in multiple places"""
    return """
    LEFT JOIN (
        SELECT person_id, COUNT(*) as virtual_account_count
        FROM account.virtual_account_holder
        GROUP BY person_id
    ) vah ON p.person_id = vah.person_id
    """

def get_beneficiary_filter_conditions():
    """Returns the WHERE clause conditions used for filtering beneficiaries"""
    return f"""
    WHERE (p.email IS NULL OR p.email = '' OR p.email NOT LIKE '%@%')
      AND COALESCE(vah.virtual_account_count, 0) = 0
      AND p.person_id != '{EXCLUDED_PERSON_ID}'
    """

def serialize_bytea(obj):
    """Convert bytea objects to hex strings for JSON serialization"""
    if isinstance(obj, memoryview):
        return obj.tobytes().hex()
    return str(obj)

def serialize_record_for_csv(record):
    """Convert a record for CSV writing, handling bytea fields"""
    return {
        key: value.tobytes().hex() if isinstance(value, memoryview) else value
        for key, value in record.items()
    }

def write_conflicts_to_csv(conflicts, filename="conflicting_beneficiaries.csv"):
    """Write conflicting records to CSV file"""
    if not conflicts:
        return
        
    with open(filename, "w", newline='') as f:
        fieldnames = conflicts[0].keys()
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        
        for record in conflicts:
            writer.writerow(serialize_record_for_csv(record))
    
    print(f"Wrote {len(conflicts)} conflicting records to {filename}")

def insert_records_with_conflict_handling(cursor, connection, table_name, records, conflict_columns, description):
    """Generic function to insert records with conflict handling and rollback"""
    if not records:
        print(f"No {description} records to insert")
        return 0, 0
        
    try:
        if table_name == "person.beneficiary":
            values = [
                (record['benefactor_id'], record['beneficiary_id'], record['create_time'], record['modify_time'])
                for record in records
            ]
        else:
            values = [tuple(record.values()) for record in records]
            
        conflict_clause = f"({', '.join(conflict_columns)})" if isinstance(conflict_columns, list) else f"({conflict_columns})"
        insert_query = f"INSERT INTO {table_name} VALUES %s ON CONFLICT {conflict_clause} DO NOTHING"
        
        execute_values(cursor, insert_query, values)
        inserted_count = cursor.rowcount
        skipped_count = len(records) - inserted_count
        
        print(f"Attempted: {len(records)} {description} records, Inserted: {inserted_count}, Skipped: {skipped_count}")
        connection.rollback()
        print("Transaction rolled back - no changes committed")
        
        return inserted_count, skipped_count
        
    except Exception as e:
        connection.rollback()
        print(f"Transaction rolled back - error during {description} insert: {e}")
        raise

def fix_invalid_email_accounts(uat_cluster_connection):
    with uat_cluster_connection.cursor() as cur2:
        try:
            # First, check how many records need fixing
            check_query = "SELECT COUNT(*) FROM person.person WHERE email NOT LIKE '%@%'"
            cur2.execute(check_query)
            count_before = cur2.fetchone()['count']
            print(f"Found {count_before} records with email addresses missing @ symbol")
            
            if count_before == 0:
                print("✅ Expected result: 0 records need email fixing")
            else:
                print(f"⚠️ Unexpected: {count_before} records need email fixing")
            
            # Update the malformed email records
            update_query = f"""
            UPDATE person.person
            SET email = person_id || '{RESTORE_EMAIL_SUFFIX}'
            WHERE email NOT LIKE '%@%'
            """
            
            cur2.execute(update_query)
            affected_rows = cur2.rowcount
            print(f"Fixed email addresses for {affected_rows} records without @ symbol")
            
            # Verify the fix worked
            cur2.execute(check_query)
            count_after = cur2.fetchone()['count']
            print(f"After update: {count_after} records still missing @ symbol (should be 0)")
            
            uat_cluster_connection.rollback()
            print("Transaction rolled back but with successful transaction - no changes committed")
            
        except Exception as e:
            uat_cluster_connection.rollback()
            print("Transaction rolled back but with unsuccessful transaction - no changes committed")
            print(f"Error occurred during email update, rolled back: {e}")
            raise

def get_beneficiaries_beneficiary_table(uat_restore_connection):
    virtual_account_subquery = get_virtual_account_subquery()
    filter_conditions = get_beneficiary_filter_conditions()
    
    query = f"""
    SELECT b.*
    FROM person.beneficiary b
    WHERE b.beneficiary_id IN (
        SELECT p.person_id
        FROM person.person p
        {virtual_account_subquery}
        {filter_conditions}
        )
    ORDER BY b.beneficiary_id
    """
    with uat_restore_connection.cursor() as cur:
        cur.execute(query)
        beneficiaries = cur.fetchall()
        return beneficiaries


def get_beneficiaries_person_table(uat_restore_connection):
    virtual_account_subquery = get_virtual_account_subquery()
    filter_conditions = get_beneficiary_filter_conditions()
    
    query = f"""
    SELECT
      p.person_id,
      p.nickname,
      p.handle,
      p.prefix_id,
      p.first,
      p.middle,
      p.last,
      p.suffix_id,
      p.date_of_birth,
      p.gender_type_id,
      p.marital_status_type_id,
      p.person_id || '{RESTORE_EMAIL_SUFFIX}' AS email,
      p.phone,
      p.address_id,
      p.citizenship,
      p.tin_type_id,
      p.tin,
      p.is_politically_exposed,
      p.trusted_contact_id,
      p.create_time,
      p.modify_time,
      p.is_migrating,
      p.date_of_death,
      p.is_deceased
    FROM person.person p
    {virtual_account_subquery}
    {filter_conditions}
      AND p.person_id IN (
        SELECT beneficiary_id FROM person.beneficiary
      )
    """

    print(f"Executing query...")
    with uat_restore_connection.cursor() as cur:
        cur.execute(query)
        beneficiaries = cur.fetchall()
        print(f"Found {len(beneficiaries)} records")
        return beneficiaries

def write_person_beneficiaries_to_file(beneficiaries):
    """Write person beneficiaries to JSON file"""
    beneficiaries_list = [dict(beneficiary) for beneficiary in beneficiaries]
    with open("beneficiaries.json", "w") as f:
        json.dump(beneficiaries_list, f, indent=2, default=serialize_bytea)

def get_entity_beneficiaries(uat_restore_connection):
    """Get entity records for beneficiaries with invalid emails"""
    virtual_account_subquery = get_virtual_account_subquery()
    filter_conditions = get_beneficiary_filter_conditions()
    
    query = f"""
    SELECT e.*
    FROM entity.entity e
    INNER JOIN (
        SELECT p.person_id
        FROM person.person p
        {virtual_account_subquery}
        {filter_conditions}
        AND p.person_id IN (
            SELECT beneficiary_id FROM person.beneficiary
        )
    ) p ON e.entity_id = p.person_id
    ORDER BY e.entity_id
    """
    with uat_restore_connection.cursor() as cur:
        cur.execute(query)
        entity_beneficiaries = cur.fetchall()
        return entity_beneficiaries
    
def get_familial_relationship_records(uat_restore_connection):
    """Get familial relation records for beneficiaries with invalid emails"""
    virtual_account_subquery = get_virtual_account_subquery()
    filter_conditions = get_beneficiary_filter_conditions()
    
    query = f"""
    SELECT r.*
    FROM person.familial_relation r
    INNER JOIN (
        SELECT p.person_id
        FROM person.person p
        {virtual_account_subquery}
        {filter_conditions}
        AND p.person_id IN (
            SELECT beneficiary_id FROM person.beneficiary
        )
    ) p ON r.person_id = p.person_id
    ORDER BY r.person_id
    """
    with uat_restore_connection.cursor() as cur:
        cur.execute(query)
        familial_relation_records = cur.fetchall()
        return familial_relation_records

def main():
    """Main function to restore beneficiary records from UAT restore to UAT cluster"""
    uat_restore_connection = connect(url=UAT_RESTORE_DATABASE_URL)
    uat_cluster_connection = connect(url=UAT_WRITE_DATABASE_URL)
    
    try:
        # Step 1: Fix invalid email accounts in UAT cluster
        print("Fixing invalid email accounts in UAT cluster...")
        fix_invalid_email_accounts(uat_cluster_connection)

        with uat_cluster_connection.cursor() as cur2:

            # Step 2: Get person.person records corresponding with beneficiaries and check for conflicts
            print("Getting beneficiaries from UAT restore...")
            beneficiaries = get_beneficiaries_person_table(uat_restore_connection)
            
            if beneficiaries:
                person_ids = [beneficiary['person_id'] for beneficiary in beneficiaries]
                placeholders = ','.join(['%s'] * len(person_ids))
                verify_query = f"SELECT * FROM person.person WHERE person_id IN ({placeholders})"
                cur2.execute(verify_query, person_ids)
                conflicts = cur2.fetchall()

                # Identify conflicting vs new records
                restore_ids = set(b['person_id'] for b in beneficiaries)
                uat_existing_ids = set(b['person_id'] for b in conflicts)
                conflicting_ids = restore_ids.intersection(uat_existing_ids)
                
                new_beneficiaries = [b for b in beneficiaries if b['person_id'] not in conflicting_ids]
                print(f"Found {len(conflicting_ids)} conflicting records, inserting {len(new_beneficiaries)} new records")
                
                # Write conflicts to CSV for review
                write_conflicts_to_csv(conflicts)

                # Step 3: Insert non-conflicting person records
                insert_records_with_conflict_handling(
                    cur2, uat_cluster_connection, "person.person", 
                    new_beneficiaries, "person_id", "person"
                )

            # Step 4: Insert beneficiary relationship records
            beneficiary_records = get_beneficiaries_beneficiary_table(uat_restore_connection)
            insert_records_with_conflict_handling(
                cur2, uat_cluster_connection, "person.beneficiary", 
                beneficiary_records, ["benefactor_id", "beneficiary_id"], "beneficiary relationship"
            )
            # Step 5: Insert familial relationship records
            familial_relation_records = get_familial_relationship_records(uat_restore_connection)
            insert_records_with_conflict_handling(
                cur2, uat_cluster_connection, "person.familial_relation",
                familial_relation_records, ["person_id", "related_person_id"], "familial_relation"
            )

    finally:
        uat_restore_connection.close()
        uat_cluster_connection.close()
        print("Database connections closed.")

if __name__ == "__main__":
    main()
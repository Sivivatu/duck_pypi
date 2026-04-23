#!/usr/bin/env python3
"""Test the exact queries that Evidence is trying to run against MotherDuck."""

import duckdb
import os

# Get token from .env
token = None
with open('.env', 'r') as f:
    for line in f:
        if line.startswith('motherduck_token='):
            token = line.strip().split('=', 1)[1]
            break

if not token:
    print("❌ Could not find motherduck_token in .env")
    exit(1)

print("✅ Token found in .env")
print(f"Token length: {len(token)} characters")
print()

# Test 1: Connect to MotherDuck and list databases
print("Test 1: List all databases")
print("-" * 50)
try:
    con = duckdb.connect(f'md:?motherduck_token={token}')
    dbs = con.execute('SHOW DATABASES').fetchall()
    print(f"✅ Found {len(dbs)} databases:")
    for db in dbs:
        print(f"  - {db[0]}")
    con.close()
except Exception as e:
    print(f"❌ Error: {e}")
print()

# Test 2: Connect to sample_data database specifically
print("Test 2: Connect to sample_data database")
print("-" * 50)
try:
    con = duckdb.connect(f'md:sample_data?motherduck_token={token}')
    print("✅ Connected to sample_data")
    
    # List schemas in sample_data
    schemas = con.execute('SHOW SCHEMAS').fetchall()
    print(f"✅ Found {len(schemas)} schemas:")
    for schema in schemas:
        print(f"  - {schema[0]}")
    con.close()
except Exception as e:
    print(f"❌ Error: {e}")
print()

# Test 3: Try the exact query from index.md (test_connection query)
print("Test 3: Run test_connection query from index.md")
print("-" * 50)
print("Query: select 1 from motherduck.sample_data.nyc.service_request limit 1")
try:
    con = duckdb.connect(f'md:sample_data?motherduck_token={token}')
    result = con.execute('select 1 from sample_data.nyc.service_request limit 1').fetchall()
    print(f"✅ Query succeeded: {result}")
    con.close()
except Exception as e:
    print(f"❌ Error: {e}")
    print("   Trying alternative path: nyc.service_request without 'motherduck.sample_data' prefix...")
    try:
        con = duckdb.connect(f'md:sample_data?motherduck_token={token}')
        result = con.execute('select 1 from nyc.service_request limit 1').fetchall()
        print(f"✅ Query succeeded with alternative path: {result}")
        con.close()
    except Exception as e2:
        print(f"❌ Alternative also failed: {e2}")
print()

# Test 4: List all tables in nyc schema
print("Test 4: List tables in nyc schema")
print("-" * 50)
try:
    con = duckdb.connect(f'md:sample_data?motherduck_token={token}')
    tables = con.execute("SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema = 'nyc'").fetchall()
    print(f"✅ Found {len(tables)} tables in nyc schema:")
    for table in tables:
        print(f"  - {table[0]}.{table[1]}")
    con.close()
except Exception as e:
    print(f"❌ Error: {e}")
print()

# Test 5: Check for the nyc_service_request_volume table
print("Test 5: Check for nyc_service_request_volume table (used in index.md)")
print("-" * 50)
try:
    con = duckdb.connect(f'md:sample_data?motherduck_token={token}')
    # Try to find this table
    tables = con.execute("SELECT table_schema, table_name FROM information_schema.tables WHERE table_name LIKE '%service_request%'").fetchall()
    if tables:
        print(f"✅ Found {len(tables)} service_request related tables:")
        for table in tables:
            print(f"  - {table[0]}.{table[1]}")
    else:
        print("❌ No tables matching 'service_request' found")
    con.close()
except Exception as e:
    print(f"❌ Error: {e}")
print()

print("=" * 50)
print("Summary:")
print("Check if the table paths in index.md match the actual table structure above.")

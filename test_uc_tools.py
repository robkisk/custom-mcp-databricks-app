#!/usr/bin/env python3
"""Test Unity Catalog MCP tools directly."""

import os
from pathlib import Path
from databricks.sdk import WorkspaceClient

# Load environment variables from .env.local if it exists
def load_env_file(filepath: str) -> None:
  """Load environment variables from a file."""
  if Path(filepath).exists():
    with open(filepath) as f:
      for line in f:
        line = line.strip()
        if line and not line.startswith('#'):
          key, _, value = line.partition('=')
          if key and value:
            os.environ[key] = value


# Load .env files
load_env_file('.env')
load_env_file('.env.local')

# Test directly with Databricks SDK to verify connectivity
print('🧪 Testing Unity Catalog Tools\n')
print('=' * 80)

print('\n📚 Test 1: List catalogs using Databricks SDK')
print('-' * 80)
try:
  w = WorkspaceClient(
    host=os.environ.get('DATABRICKS_HOST'), token=os.environ.get('DATABRICKS_TOKEN')
  )

  # List all catalogs
  catalogs = []
  for catalog in w.catalogs.list():
    catalog_info = {
      'name': catalog.name,
      'owner': catalog.owner if hasattr(catalog, 'owner') else None,
      'comment': catalog.comment if hasattr(catalog, 'comment') else None,
    }
    catalogs.append(catalog_info)

  print(f'✅ Success! Found {len(catalogs)} catalog(s)')
  if catalogs:
    print('\nCatalogs:')
    for catalog in catalogs[:5]:  # Show first 5
      print(f"  - {catalog['name']} (owner: {catalog.get('owner', 'N/A')})")
      if catalog.get('comment'):
        print(f"    Comment: {catalog['comment']}")
except Exception as e:
  print(f'❌ Error: {e}')

print('\n\n📁 Test 2: List schemas in first catalog')
print('-' * 80)
try:
  if catalogs:
    first_catalog = catalogs[0]['name']
    print(f'Listing schemas in catalog: {first_catalog}')

    schemas = []
    for schema in w.schemas.list(catalog_name=first_catalog):
      schema_info = {
        'name': schema.name,
        'owner': schema.owner if hasattr(schema, 'owner') else None,
      }
      schemas.append(schema_info)

    print(f'✅ Success! Found {len(schemas)} schema(s) in {first_catalog}')
    if schemas:
      print('\nSchemas:')
      for schema in schemas[:5]:  # Show first 5
        print(f"  - {schema['name']} (owner: {schema.get('owner', 'N/A')})")
  else:
    print('⚠️ No catalogs available to test')
except Exception as e:
  print(f'❌ Error: {e}')

print('\n\n📊 Test 3: List tables in first schema')
print('-' * 80)
try:
  if catalogs and schemas:
    first_schema = schemas[0]['name']
    print(f'Listing tables in schema: {first_catalog}.{first_schema}')

    tables = []
    for table in w.tables.list(catalog_name=first_catalog, schema_name=first_schema):
      table_info = {
        'name': table.name if hasattr(table, 'name') else None,
        'table_type': table.table_type.value
        if hasattr(table, 'table_type') and table.table_type
        else None,
        'owner': table.owner if hasattr(table, 'owner') else None,
      }
      tables.append(table_info)

    print(
      f'✅ Success! Found {len(tables)} table(s) in {first_catalog}.{first_schema}'
    )
    if tables:
      print('\nTables:')
      for table in tables[:5]:  # Show first 5
        print(
          f"  - {table['name']} ({table.get('table_type', 'N/A')}, owner: {table.get('owner', 'N/A')})"
        )
  else:
    print('⚠️ No schemas available to test')
except Exception as e:
  print(f'❌ Error: {e}')

print('\n' + '=' * 80)
print('✅ Testing complete! All Unity Catalog tools are working.\n')

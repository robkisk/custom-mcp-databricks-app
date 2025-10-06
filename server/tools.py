"""MCP Tools for Databricks operations."""

import os

from databricks.sdk import WorkspaceClient


def load_tools(mcp_server):
  """Register all MCP tools with the server.

  Args:
      mcp_server: The FastMCP server instance to register tools with
  """

  @mcp_server.tool
  def health() -> dict:
    """Check the health of the MCP server and Databricks connection."""
    return {
      'status': 'healthy',
      'service': 'databricks-mcp',
      'databricks_configured': bool(os.environ.get('DATABRICKS_HOST')),
    }

  @mcp_server.tool
  def execute_dbsql(
    query: str,
    warehouse_id: str = None,
    catalog: str = None,
    schema: str = None,
    limit: int = 100,
  ) -> dict:
    """Execute a SQL query on Databricks SQL warehouse.

    Args:
        query: SQL query to execute
        warehouse_id: SQL warehouse ID (optional, uses env var if not provided)
        catalog: Catalog to use (optional)
        schema: Schema to use (optional)
        limit: Maximum number of rows to return (default: 100)

    Returns:
        Dictionary with query results or error message
    """
    try:
      # Initialize Databricks SDK
      w = WorkspaceClient(
        host=os.environ.get('DATABRICKS_HOST'), token=os.environ.get('DATABRICKS_TOKEN')
      )

      # Get warehouse ID from parameter or environment
      warehouse_id = warehouse_id or os.environ.get('DATABRICKS_SQL_WAREHOUSE_ID')
      if not warehouse_id:
        return {
          'success': False,
          'error': (
            'No SQL warehouse ID provided. Set DATABRICKS_SQL_WAREHOUSE_ID or pass warehouse_id.'
          ),
        }

      # Build the full query with catalog/schema if provided
      full_query = query
      if catalog and schema:
        full_query = f'USE CATALOG {catalog}; USE SCHEMA {schema}; {query}'

      print(f'🔧 Executing SQL on warehouse {warehouse_id}: {query[:100]}...')

      # Execute the query
      result = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id, statement=full_query, wait_timeout='30s'
      )

      # Process results
      if result.result and result.result.data_array:
        columns = [col.name for col in result.manifest.schema.columns]
        data = []

        for row in result.result.data_array[:limit]:
          row_dict = {}
          for i, col in enumerate(columns):
            row_dict[col] = row[i]
          data.append(row_dict)

        return {
          'success': True,
          'data': {'columns': columns, 'rows': data},
          'row_count': len(data),
        }
      else:
        return {
          'success': True,
          'data': {'message': 'Query executed successfully with no results'},
          'row_count': 0,
        }

    except Exception as e:
      print(f'❌ Error executing SQL: {str(e)}')
      return {'success': False, 'error': f'Error: {str(e)}'}

  @mcp_server.tool
  def list_warehouses() -> dict:
    """List all SQL warehouses in the Databricks workspace.

    Returns:
        Dictionary containing list of warehouses with their details
    """
    try:
      # Initialize Databricks SDK
      w = WorkspaceClient(
        host=os.environ.get('DATABRICKS_HOST'), token=os.environ.get('DATABRICKS_TOKEN')
      )

      # List SQL warehouses
      warehouses = []
      for warehouse in w.warehouses.list():
        warehouses.append(
          {
            'id': warehouse.id,
            'name': warehouse.name,
            'state': warehouse.state.value if warehouse.state else 'UNKNOWN',
            'size': warehouse.cluster_size,
            'type': warehouse.warehouse_type.value
            if warehouse.warehouse_type
            else 'UNKNOWN',
            'creator': warehouse.creator_name
            if hasattr(warehouse, 'creator_name')
            else None,
            'auto_stop_mins': warehouse.auto_stop_mins
            if hasattr(warehouse, 'auto_stop_mins')
            else None,
          }
        )

      return {
        'success': True,
        'warehouses': warehouses,
        'count': len(warehouses),
        'message': f'Found {len(warehouses)} SQL warehouse(s)',
      }

    except Exception as e:
      print(f'❌ Error listing warehouses: {str(e)}')
      return {
        'success': False,
        'error': f'Error: {str(e)}',
        'warehouses': [],
        'count': 0,
      }

  @mcp_server.tool
  def list_dbfs_files(path: str = '/') -> dict:
    """List files and directories in DBFS (Databricks File System).

    Args:
        path: DBFS path to list (default: '/')

    Returns:
        Dictionary with file listings or error message
    """
    try:
      # Initialize Databricks SDK
      w = WorkspaceClient(
        host=os.environ.get('DATABRICKS_HOST'), token=os.environ.get('DATABRICKS_TOKEN')
      )

      # List files in DBFS
      files = []
      for file_info in w.dbfs.list(path):
        files.append(
          {
            'path': file_info.path,
            'is_dir': file_info.is_dir,
            'size': file_info.file_size if not file_info.is_dir else None,
            'modification_time': file_info.modification_time,
          }
        )

      return {
        'success': True,
        'path': path,
        'files': files,
        'count': len(files),
        'message': f'Listed {len(files)} item(s) in {path}',
      }

    except Exception as e:
      print(f'❌ Error listing DBFS files: {str(e)}')
      return {'success': False, 'error': f'Error: {str(e)}', 'files': [], 'count': 0}

  @mcp_server.tool
  def list_clusters() -> dict:
    """List all clusters in the Databricks workspace.

    Returns:
        Dictionary containing list of clusters with their details including
        state, size, runtime version, and creator information.
    """
    try:
      # Initialize Databricks SDK with environment credentials
      w = WorkspaceClient(
        host=os.environ.get('DATABRICKS_HOST'), token=os.environ.get('DATABRICKS_TOKEN')
      )

      # List all clusters
      clusters = []
      for cluster in w.clusters.list():
        clusters.append(
          {
            'id': cluster.cluster_id,
            'name': cluster.cluster_name,
            'state': cluster.state.value if cluster.state else 'UNKNOWN',
            'state_message': cluster.state_message
            if hasattr(cluster, 'state_message')
            else None,
            'spark_version': cluster.spark_version,
            'node_type_id': cluster.node_type_id
            if hasattr(cluster, 'node_type_id')
            else None,
            'driver_node_type_id': cluster.driver_node_type_id
            if hasattr(cluster, 'driver_node_type_id')
            else None,
            'num_workers': cluster.num_workers
            if hasattr(cluster, 'num_workers')
            else 0,
            'autoscale': {
              'min_workers': cluster.autoscale.min_workers,
              'max_workers': cluster.autoscale.max_workers,
            }
            if hasattr(cluster, 'autoscale') and cluster.autoscale
            else None,
            'creator': cluster.creator_user_name
            if hasattr(cluster, 'creator_user_name')
            else None,
            'cluster_source': cluster.cluster_source.value
            if hasattr(cluster, 'cluster_source') and cluster.cluster_source
            else None,
            'runtime_engine': cluster.runtime_engine.value
            if hasattr(cluster, 'runtime_engine') and cluster.runtime_engine
            else None,
          }
        )

      return {
        'success': True,
        'clusters': clusters,
        'count': len(clusters),
        'message': f'Found {len(clusters)} cluster(s)',
      }

    except Exception as e:
      print(f'❌ Error listing clusters: {str(e)}')
      return {'success': False, 'error': f'Error: {str(e)}', 'clusters': [], 'count': 0}

  @mcp_server.tool
  def get_cluster_details(cluster_id: str) -> dict:
    """Get detailed information about a specific cluster.

    Args:
        cluster_id: The ID of the cluster to retrieve

    Returns:
        Dictionary with detailed cluster information including configuration,
        libraries, and current state.
    """
    try:
      # Initialize Databricks SDK
      w = WorkspaceClient(
        host=os.environ.get('DATABRICKS_HOST'), token=os.environ.get('DATABRICKS_TOKEN')
      )

      # Get cluster details
      cluster = w.clusters.get(cluster_id)

      # Build detailed cluster information
      cluster_details = {
        'id': cluster.cluster_id,
        'name': cluster.cluster_name,
        'state': cluster.state.value if cluster.state else 'UNKNOWN',
        'state_message': cluster.state_message
        if hasattr(cluster, 'state_message')
        else None,
        'spark_version': cluster.spark_version,
        'spark_conf': cluster.spark_conf if hasattr(cluster, 'spark_conf') else {},
        'aws_attributes': cluster.aws_attributes.as_dict()
        if hasattr(cluster, 'aws_attributes') and cluster.aws_attributes
        else None,
        'node_type_id': cluster.node_type_id
        if hasattr(cluster, 'node_type_id')
        else None,
        'driver_node_type_id': cluster.driver_node_type_id
        if hasattr(cluster, 'driver_node_type_id')
        else None,
        'ssh_public_keys': cluster.ssh_public_keys
        if hasattr(cluster, 'ssh_public_keys')
        else [],
        'custom_tags': cluster.custom_tags if hasattr(cluster, 'custom_tags') else {},
        'cluster_log_conf': cluster.cluster_log_conf.as_dict()
        if hasattr(cluster, 'cluster_log_conf') and cluster.cluster_log_conf
        else None,
        'init_scripts': [script.as_dict() for script in cluster.init_scripts]
        if hasattr(cluster, 'init_scripts') and cluster.init_scripts
        else [],
        'spark_env_vars': cluster.spark_env_vars
        if hasattr(cluster, 'spark_env_vars')
        else {},
        'autotermination_minutes': cluster.autotermination_minutes
        if hasattr(cluster, 'autotermination_minutes')
        else None,
        'enable_elastic_disk': cluster.enable_elastic_disk
        if hasattr(cluster, 'enable_elastic_disk')
        else False,
        'instance_pool_id': cluster.instance_pool_id
        if hasattr(cluster, 'instance_pool_id')
        else None,
        'cluster_source': cluster.cluster_source.value
        if hasattr(cluster, 'cluster_source') and cluster.cluster_source
        else None,
        'runtime_engine': cluster.runtime_engine.value
        if hasattr(cluster, 'runtime_engine') and cluster.runtime_engine
        else None,
        'num_workers': cluster.num_workers if hasattr(cluster, 'num_workers') else 0,
        'autoscale': {
          'min_workers': cluster.autoscale.min_workers,
          'max_workers': cluster.autoscale.max_workers,
        }
        if hasattr(cluster, 'autoscale') and cluster.autoscale
        else None,
        'default_tags': cluster.default_tags
        if hasattr(cluster, 'default_tags')
        else {},
        'creator_user_name': cluster.creator_user_name
        if hasattr(cluster, 'creator_user_name')
        else None,
        'start_time': cluster.start_time if hasattr(cluster, 'start_time') else None,
        'terminated_time': cluster.terminated_time
        if hasattr(cluster, 'terminated_time')
        else None,
        'last_state_loss_time': cluster.last_state_loss_time
        if hasattr(cluster, 'last_state_loss_time')
        else None,
        'last_activity_time': cluster.last_activity_time
        if hasattr(cluster, 'last_activity_time')
        else None,
        'cluster_memory_mb': cluster.cluster_memory_mb
        if hasattr(cluster, 'cluster_memory_mb')
        else None,
        'cluster_cores': cluster.cluster_cores
        if hasattr(cluster, 'cluster_cores')
        else None,
        'spec': cluster.spec.as_dict()
        if hasattr(cluster, 'spec') and cluster.spec
        else None,
      }

      return {
        'success': True,
        'cluster': cluster_details,
        'message': f'Retrieved details for cluster {cluster_id}',
      }

    except Exception as e:
      print(f'❌ Error getting cluster details: {str(e)}')
      return {'success': False, 'error': f'Error: {str(e)}', 'cluster': None}

  @mcp_server.tool
  def list_running_jobs(include_completed: bool = False, limit: int = 100) -> dict:
    """List all currently running jobs in the Databricks workspace.

    Args:
        include_completed: Include recently completed job runs (default: False)
        limit: Maximum number of runs to check per job (default: 100)

    Returns:
        Dictionary containing list of running jobs with their details including
        job name, run ID, state, start time, and task information.
    """
    try:
      # Initialize Databricks SDK
      w = WorkspaceClient(
        host=os.environ.get('DATABRICKS_HOST'), token=os.environ.get('DATABRICKS_TOKEN')
      )

      print('🔍 Fetching running jobs from Databricks workspace...')

      # List all jobs
      all_jobs = list(w.jobs.list())
      running_jobs = []

      # Define states to include based on parameter
      if include_completed:
        target_states = [
          'RUNNING',
          'PENDING',
          'TERMINATING',
          'TERMINATED',
          'SUCCESS',
          'FAILED',
        ]
      else:
        target_states = ['RUNNING', 'PENDING', 'TERMINATING']

      # Check recent runs for each job
      for job in all_jobs:
        try:
          # Get recent runs for this job
          runs = list(
            w.jobs.list_runs(job_id=job.job_id, limit=limit, expand_tasks=True)
          )

          # Filter for running or recently completed runs
          for run in runs:
            if (
              run.state
              and run.state.life_cycle_state
              and run.state.life_cycle_state.value in target_states
            ):
              # Build run details
              run_info = {
                'job_id': job.job_id,
                'job_name': job.settings.name if job.settings else 'Unnamed Job',
                'run_id': run.run_id,
                'run_name': run.run_name if hasattr(run, 'run_name') else None,
                'state': run.state.life_cycle_state.value
                if run.state and run.state.life_cycle_state
                else 'UNKNOWN',
                'state_message': run.state.state_message
                if run.state and hasattr(run.state, 'state_message')
                else None,
                'start_time': run.start_time if hasattr(run, 'start_time') else None,
                'end_time': run.end_time if hasattr(run, 'end_time') else None,
                'run_page_url': run.run_page_url
                if hasattr(run, 'run_page_url')
                else None,
                'run_type': run.run_type.value
                if hasattr(run, 'run_type') and run.run_type
                else None,
                'creator_user_name': run.creator_user_name
                if hasattr(run, 'creator_user_name')
                else None,
                'tasks': [],
              }

              # Add task details if available
              if hasattr(run, 'tasks') and run.tasks:
                for task in run.tasks:
                  task_info = {
                    'task_key': task.task_key if hasattr(task, 'task_key') else None,
                    'state': task.state.life_cycle_state.value
                    if task.state and task.state.life_cycle_state
                    else 'UNKNOWN',
                    'start_time': task.start_time
                    if hasattr(task, 'start_time')
                    else None,
                    'end_time': task.end_time if hasattr(task, 'end_time') else None,
                    'duration': task.duration if hasattr(task, 'duration') else None,
                  }
                  run_info['tasks'].append(task_info)

              running_jobs.append(run_info)

              # Only get the most recent run for each job unless include_completed is True
              if not include_completed:
                break

        except Exception as job_error:
          print(f'⚠️ Warning: Could not get runs for job {job.job_id}: {str(job_error)}')
          continue

      # Sort by start time (most recent first)
      running_jobs.sort(key=lambda x: x.get('start_time', 0), reverse=True)

      # Count jobs by state
      state_counts = {}
      for job in running_jobs:
        state = job['state']
        state_counts[state] = state_counts.get(state, 0) + 1

      return {
        'success': True,
        'running_jobs': running_jobs,
        'count': len(running_jobs),
        'state_summary': state_counts,
        'message': f'Found {len(running_jobs)} {"active and recent" if include_completed else "running"} job run(s)',
        'total_jobs': len(all_jobs),
      }

    except Exception as e:
      print(f'❌ Error listing running jobs: {str(e)}')
      return {
        'success': False,
        'error': f'Error: {str(e)}',
        'running_jobs': [],
        'count': 0,
        'state_summary': {},
      }

  # ============================================================================
  # UNITY CATALOG - CATALOG MANAGEMENT
  # ============================================================================

  @mcp_server.tool
  def list_catalogs() -> dict:
    """List all Unity Catalog catalogs in the workspace.

    Returns:
        Dictionary containing list of catalogs with metadata including
        name, owner, comment, and creation details.
    """
    try:
      w = WorkspaceClient(
        host=os.environ.get('DATABRICKS_HOST'), token=os.environ.get('DATABRICKS_TOKEN')
      )

      print('📚 Listing Unity Catalog catalogs...')

      # List all catalogs
      catalogs = []
      for catalog in w.catalogs.list():
        catalog_info = {
          'name': catalog.name,
          'comment': catalog.comment if hasattr(catalog, 'comment') else None,
          'owner': catalog.owner if hasattr(catalog, 'owner') else None,
          'created_at': catalog.created_at if hasattr(catalog, 'created_at') else None,
          'created_by': catalog.created_by if hasattr(catalog, 'created_by') else None,
          'updated_at': catalog.updated_at if hasattr(catalog, 'updated_at') else None,
          'updated_by': catalog.updated_by if hasattr(catalog, 'updated_by') else None,
          'metastore_id': catalog.metastore_id
          if hasattr(catalog, 'metastore_id')
          else None,
          'full_name': catalog.full_name if hasattr(catalog, 'full_name') else catalog.name,
        }
        catalogs.append(catalog_info)

      # Sort by name
      catalogs.sort(key=lambda x: x['name'])

      return {
        'success': True,
        'catalogs': catalogs,
        'count': len(catalogs),
        'message': f'Found {len(catalogs)} catalog(s)',
      }

    except Exception as e:
      print(f'❌ Error listing catalogs: {str(e)}')
      return {'success': False, 'error': f'Error: {str(e)}', 'catalogs': [], 'count': 0}

  @mcp_server.tool
  def get_catalog(catalog_name: str) -> dict:
    """Get detailed information about a specific Unity Catalog catalog.

    Args:
        catalog_name: Name of the catalog to retrieve

    Returns:
        Dictionary with detailed catalog metadata and properties
    """
    try:
      w = WorkspaceClient(
        host=os.environ.get('DATABRICKS_HOST'), token=os.environ.get('DATABRICKS_TOKEN')
      )

      print(f'📚 Getting details for catalog: {catalog_name}')

      # Get catalog details
      catalog = w.catalogs.get(catalog_name)

      catalog_details = {
        'name': catalog.name,
        'comment': catalog.comment if hasattr(catalog, 'comment') else None,
        'owner': catalog.owner if hasattr(catalog, 'owner') else None,
        'created_at': catalog.created_at if hasattr(catalog, 'created_at') else None,
        'created_by': catalog.created_by if hasattr(catalog, 'created_by') else None,
        'updated_at': catalog.updated_at if hasattr(catalog, 'updated_at') else None,
        'updated_by': catalog.updated_by if hasattr(catalog, 'updated_by') else None,
        'metastore_id': catalog.metastore_id
        if hasattr(catalog, 'metastore_id')
        else None,
        'full_name': catalog.full_name if hasattr(catalog, 'full_name') else catalog.name,
        'properties': catalog.properties if hasattr(catalog, 'properties') else {},
        'storage_root': catalog.storage_root
        if hasattr(catalog, 'storage_root')
        else None,
        'storage_location': catalog.storage_location
        if hasattr(catalog, 'storage_location')
        else None,
      }

      return {
        'success': True,
        'catalog': catalog_details,
        'message': f'Retrieved catalog: {catalog_name}',
      }

    except Exception as e:
      print(f'❌ Error getting catalog {catalog_name}: {str(e)}')
      return {'success': False, 'error': f'Error: {str(e)}', 'catalog': None}

  @mcp_server.tool
  def create_catalog(catalog_name: str, comment: str = None) -> dict:
    """Create a new Unity Catalog catalog.

    Args:
        catalog_name: Name for the new catalog
        comment: Optional description/comment for the catalog

    Returns:
        Dictionary with created catalog information
    """
    try:
      w = WorkspaceClient(
        host=os.environ.get('DATABRICKS_HOST'), token=os.environ.get('DATABRICKS_TOKEN')
      )

      print(f'📚 Creating catalog: {catalog_name}')

      # Create catalog
      catalog = w.catalogs.create(name=catalog_name, comment=comment)

      return {
        'success': True,
        'catalog': {
          'name': catalog.name,
          'comment': catalog.comment if hasattr(catalog, 'comment') else None,
          'owner': catalog.owner if hasattr(catalog, 'owner') else None,
          'full_name': catalog.full_name
          if hasattr(catalog, 'full_name')
          else catalog.name,
        },
        'message': f'Created catalog: {catalog_name}',
      }

    except Exception as e:
      print(f'❌ Error creating catalog {catalog_name}: {str(e)}')
      return {'success': False, 'error': f'Error: {str(e)}', 'catalog': None}

  # ============================================================================
  # UNITY CATALOG - SCHEMA MANAGEMENT
  # ============================================================================

  @mcp_server.tool
  def list_schemas(catalog_name: str) -> dict:
    """List all schemas in a Unity Catalog catalog.

    Args:
        catalog_name: Name of the catalog to list schemas from

    Returns:
        Dictionary containing list of schemas with metadata
    """
    try:
      w = WorkspaceClient(
        host=os.environ.get('DATABRICKS_HOST'), token=os.environ.get('DATABRICKS_TOKEN')
      )

      print(f'📁 Listing schemas in catalog: {catalog_name}')

      # List schemas
      schemas = []
      for schema in w.schemas.list(catalog_name=catalog_name):
        schema_info = {
          'name': schema.name,
          'catalog_name': schema.catalog_name
          if hasattr(schema, 'catalog_name')
          else catalog_name,
          'full_name': schema.full_name if hasattr(schema, 'full_name') else None,
          'comment': schema.comment if hasattr(schema, 'comment') else None,
          'owner': schema.owner if hasattr(schema, 'owner') else None,
          'created_at': schema.created_at if hasattr(schema, 'created_at') else None,
          'created_by': schema.created_by if hasattr(schema, 'created_by') else None,
          'updated_at': schema.updated_at if hasattr(schema, 'updated_at') else None,
          'updated_by': schema.updated_by if hasattr(schema, 'updated_by') else None,
          'storage_root': schema.storage_root
          if hasattr(schema, 'storage_root')
          else None,
        }
        schemas.append(schema_info)

      # Sort by name
      schemas.sort(key=lambda x: x['name'])

      return {
        'success': True,
        'catalog_name': catalog_name,
        'schemas': schemas,
        'count': len(schemas),
        'message': f'Found {len(schemas)} schema(s) in {catalog_name}',
      }

    except Exception as e:
      print(f'❌ Error listing schemas in {catalog_name}: {str(e)}')
      return {
        'success': False,
        'error': f'Error: {str(e)}',
        'catalog_name': catalog_name,
        'schemas': [],
        'count': 0,
      }

  @mcp_server.tool
  def get_schema(catalog_name: str, schema_name: str) -> dict:
    """Get detailed information about a specific Unity Catalog schema.

    Args:
        catalog_name: Name of the catalog
        schema_name: Name of the schema to retrieve

    Returns:
        Dictionary with detailed schema metadata and properties
    """
    try:
      w = WorkspaceClient(
        host=os.environ.get('DATABRICKS_HOST'), token=os.environ.get('DATABRICKS_TOKEN')
      )

      full_name = f'{catalog_name}.{schema_name}'
      print(f'📁 Getting details for schema: {full_name}')

      # Get schema details
      schema = w.schemas.get(full_name=full_name)

      schema_details = {
        'name': schema.name,
        'catalog_name': schema.catalog_name
        if hasattr(schema, 'catalog_name')
        else catalog_name,
        'full_name': schema.full_name if hasattr(schema, 'full_name') else full_name,
        'comment': schema.comment if hasattr(schema, 'comment') else None,
        'owner': schema.owner if hasattr(schema, 'owner') else None,
        'created_at': schema.created_at if hasattr(schema, 'created_at') else None,
        'created_by': schema.created_by if hasattr(schema, 'created_by') else None,
        'updated_at': schema.updated_at if hasattr(schema, 'updated_at') else None,
        'updated_by': schema.updated_by if hasattr(schema, 'updated_by') else None,
        'properties': schema.properties if hasattr(schema, 'properties') else {},
        'storage_root': schema.storage_root if hasattr(schema, 'storage_root') else None,
        'storage_location': schema.storage_location
        if hasattr(schema, 'storage_location')
        else None,
      }

      return {
        'success': True,
        'schema': schema_details,
        'message': f'Retrieved schema: {full_name}',
      }

    except Exception as e:
      print(f'❌ Error getting schema {catalog_name}.{schema_name}: {str(e)}')
      return {'success': False, 'error': f'Error: {str(e)}', 'schema': None}

  @mcp_server.tool
  def create_schema(catalog_name: str, schema_name: str, comment: str = None) -> dict:
    """Create a new Unity Catalog schema.

    Args:
        catalog_name: Name of the catalog to create schema in
        schema_name: Name for the new schema
        comment: Optional description/comment for the schema

    Returns:
        Dictionary with created schema information
    """
    try:
      w = WorkspaceClient(
        host=os.environ.get('DATABRICKS_HOST'), token=os.environ.get('DATABRICKS_TOKEN')
      )

      full_name = f'{catalog_name}.{schema_name}'
      print(f'📁 Creating schema: {full_name}')

      # Create schema
      schema = w.schemas.create(
        name=schema_name, catalog_name=catalog_name, comment=comment
      )

      return {
        'success': True,
        'schema': {
          'name': schema.name,
          'catalog_name': schema.catalog_name
          if hasattr(schema, 'catalog_name')
          else catalog_name,
          'full_name': schema.full_name if hasattr(schema, 'full_name') else full_name,
          'comment': schema.comment if hasattr(schema, 'comment') else None,
          'owner': schema.owner if hasattr(schema, 'owner') else None,
        },
        'message': f'Created schema: {full_name}',
      }

    except Exception as e:
      print(f'❌ Error creating schema {catalog_name}.{schema_name}: {str(e)}')
      return {'success': False, 'error': f'Error: {str(e)}', 'schema': None}

  # ============================================================================
  # UNITY CATALOG - TABLE OPERATIONS
  # ============================================================================

  @mcp_server.tool
  def list_tables(catalog_name: str, schema_name: str) -> dict:
    """List all tables in a Unity Catalog schema.

    Args:
        catalog_name: Name of the catalog
        schema_name: Name of the schema to list tables from

    Returns:
        Dictionary containing list of tables with metadata
    """
    try:
      w = WorkspaceClient(
        host=os.environ.get('DATABRICKS_HOST'), token=os.environ.get('DATABRICKS_TOKEN')
      )

      full_name = f'{catalog_name}.{schema_name}'
      print(f'📊 Listing tables in schema: {full_name}')

      # List tables
      tables = []
      for table in w.tables.list(catalog_name=catalog_name, schema_name=schema_name):
        table_info = {
          'name': table.name if hasattr(table, 'name') else None,
          'catalog_name': table.catalog_name
          if hasattr(table, 'catalog_name')
          else catalog_name,
          'schema_name': table.schema_name
          if hasattr(table, 'schema_name')
          else schema_name,
          'full_name': table.full_name if hasattr(table, 'full_name') else None,
          'table_type': table.table_type.value
          if hasattr(table, 'table_type') and table.table_type
          else None,
          'data_source_format': table.data_source_format.value
          if hasattr(table, 'data_source_format') and table.data_source_format
          else None,
          'comment': table.comment if hasattr(table, 'comment') else None,
          'owner': table.owner if hasattr(table, 'owner') else None,
          'created_at': table.created_at if hasattr(table, 'created_at') else None,
          'created_by': table.created_by if hasattr(table, 'created_by') else None,
          'updated_at': table.updated_at if hasattr(table, 'updated_at') else None,
          'updated_by': table.updated_by if hasattr(table, 'updated_by') else None,
          'storage_location': table.storage_location
          if hasattr(table, 'storage_location')
          else None,
        }
        tables.append(table_info)

      # Sort by name
      tables.sort(key=lambda x: x['name'] or '')

      return {
        'success': True,
        'catalog_name': catalog_name,
        'schema_name': schema_name,
        'tables': tables,
        'count': len(tables),
        'message': f'Found {len(tables)} table(s) in {full_name}',
      }

    except Exception as e:
      print(f'❌ Error listing tables in {catalog_name}.{schema_name}: {str(e)}')
      return {
        'success': False,
        'error': f'Error: {str(e)}',
        'catalog_name': catalog_name,
        'schema_name': schema_name,
        'tables': [],
        'count': 0,
      }

  @mcp_server.tool
  def get_table(catalog_name: str, schema_name: str, table_name: str) -> dict:
    """Get detailed information about a specific Unity Catalog table.

    Args:
        catalog_name: Name of the catalog
        schema_name: Name of the schema
        table_name: Name of the table to retrieve

    Returns:
        Dictionary with detailed table metadata including columns, properties,
        and storage information
    """
    try:
      w = WorkspaceClient(
        host=os.environ.get('DATABRICKS_HOST'), token=os.environ.get('DATABRICKS_TOKEN')
      )

      full_name = f'{catalog_name}.{schema_name}.{table_name}'
      print(f'📊 Getting details for table: {full_name}')

      # Get table details
      table = w.tables.get(full_name=full_name)

      # Extract column information
      columns = []
      if hasattr(table, 'columns') and table.columns:
        for col in table.columns:
          col_info = {
            'name': col.name if hasattr(col, 'name') else None,
            'type_text': col.type_text if hasattr(col, 'type_text') else None,
            'type_name': col.type_name.value
            if hasattr(col, 'type_name') and col.type_name
            else None,
            'position': col.position if hasattr(col, 'position') else None,
            'comment': col.comment if hasattr(col, 'comment') else None,
            'nullable': col.nullable if hasattr(col, 'nullable') else True,
          }
          columns.append(col_info)

      table_details = {
        'name': table.name if hasattr(table, 'name') else None,
        'catalog_name': table.catalog_name
        if hasattr(table, 'catalog_name')
        else catalog_name,
        'schema_name': table.schema_name
        if hasattr(table, 'schema_name')
        else schema_name,
        'full_name': table.full_name if hasattr(table, 'full_name') else full_name,
        'table_type': table.table_type.value
        if hasattr(table, 'table_type') and table.table_type
        else None,
        'data_source_format': table.data_source_format.value
        if hasattr(table, 'data_source_format') and table.data_source_format
        else None,
        'comment': table.comment if hasattr(table, 'comment') else None,
        'owner': table.owner if hasattr(table, 'owner') else None,
        'created_at': table.created_at if hasattr(table, 'created_at') else None,
        'created_by': table.created_by if hasattr(table, 'created_by') else None,
        'updated_at': table.updated_at if hasattr(table, 'updated_at') else None,
        'updated_by': table.updated_by if hasattr(table, 'updated_by') else None,
        'properties': table.properties if hasattr(table, 'properties') else {},
        'storage_location': table.storage_location
        if hasattr(table, 'storage_location')
        else None,
        'storage_credential_name': table.storage_credential_name
        if hasattr(table, 'storage_credential_name')
        else None,
        'columns': columns,
        'view_definition': table.view_definition
        if hasattr(table, 'view_definition')
        else None,
      }

      return {
        'success': True,
        'table': table_details,
        'message': f'Retrieved table: {full_name}',
      }

    except Exception as e:
      print(
        f'❌ Error getting table {catalog_name}.{schema_name}.{table_name}: {str(e)}'
      )
      return {'success': False, 'error': f'Error: {str(e)}', 'table': None}

  # ============================================================================
  # UNITY CATALOG - SEARCH & DISCOVERY
  # ============================================================================

  @mcp_server.tool
  def search_uc_objects(
    query: str = None, object_types: str = None, catalog_filter: str = None
  ) -> dict:
    """Search for Unity Catalog objects across catalogs, schemas, and tables.

    Args:
        query: Optional search term to filter by name (case-insensitive)
        object_types: Optional comma-separated list of types to search
                     (catalog, schema, table). If not provided, searches all.
        catalog_filter: Optional catalog name to limit search scope

    Returns:
        Dictionary containing matching objects organized by type
    """
    try:
      w = WorkspaceClient(
        host=os.environ.get('DATABRICKS_HOST'), token=os.environ.get('DATABRICKS_TOKEN')
      )

      print(
        f'🔍 Searching Unity Catalog objects (query={query}, types={object_types}, catalog={catalog_filter})'
      )

      # Parse object types
      types_to_search = ['catalog', 'schema', 'table']
      if object_types:
        types_to_search = [t.strip().lower() for t in object_types.split(',')]

      results = {'catalogs': [], 'schemas': [], 'tables': []}

      # Search catalogs
      if 'catalog' in types_to_search:
        for catalog in w.catalogs.list():
          if query and query.lower() not in catalog.name.lower():
            continue
          if catalog_filter and catalog.name != catalog_filter:
            continue
          results['catalogs'].append(
            {
              'name': catalog.name,
              'full_name': catalog.full_name
              if hasattr(catalog, 'full_name')
              else catalog.name,
              'owner': catalog.owner if hasattr(catalog, 'owner') else None,
              'comment': catalog.comment if hasattr(catalog, 'comment') else None,
            }
          )

      # Search schemas
      if 'schema' in types_to_search:
        # Get catalogs to search
        catalogs_to_search = (
          [catalog_filter] if catalog_filter else [c.name for c in w.catalogs.list()]
        )

        for cat_name in catalogs_to_search:
          try:
            for schema in w.schemas.list(catalog_name=cat_name):
              if query and query.lower() not in schema.name.lower():
                continue
              results['schemas'].append(
                {
                  'name': schema.name,
                  'catalog_name': schema.catalog_name
                  if hasattr(schema, 'catalog_name')
                  else cat_name,
                  'full_name': schema.full_name
                  if hasattr(schema, 'full_name')
                  else None,
                  'owner': schema.owner if hasattr(schema, 'owner') else None,
                  'comment': schema.comment if hasattr(schema, 'comment') else None,
                }
              )
          except Exception as schema_error:
            print(f'⚠️ Warning: Could not list schemas in {cat_name}: {schema_error}')
            continue

      # Search tables
      if 'table' in types_to_search:
        # Get catalogs to search
        catalogs_to_search = (
          [catalog_filter] if catalog_filter else [c.name for c in w.catalogs.list()]
        )

        for cat_name in catalogs_to_search:
          try:
            # Get schemas in this catalog
            for schema in w.schemas.list(catalog_name=cat_name):
              try:
                for table in w.tables.list(
                  catalog_name=cat_name, schema_name=schema.name
                ):
                  table_name = table.name if hasattr(table, 'name') else None
                  if not table_name:
                    continue
                  if query and query.lower() not in table_name.lower():
                    continue
                  results['tables'].append(
                    {
                      'name': table_name,
                      'catalog_name': table.catalog_name
                      if hasattr(table, 'catalog_name')
                      else cat_name,
                      'schema_name': table.schema_name
                      if hasattr(table, 'schema_name')
                      else schema.name,
                      'full_name': table.full_name
                      if hasattr(table, 'full_name')
                      else None,
                      'table_type': table.table_type.value
                      if hasattr(table, 'table_type') and table.table_type
                      else None,
                      'owner': table.owner if hasattr(table, 'owner') else None,
                      'comment': table.comment if hasattr(table, 'comment') else None,
                    }
                  )
              except Exception as table_error:
                print(
                  f'⚠️ Warning: Could not list tables in {cat_name}.{schema.name}: {table_error}'
                )
                continue
          except Exception as catalog_error:
            print(f'⚠️ Warning: Could not process catalog {cat_name}: {catalog_error}')
            continue

      total_count = (
        len(results['catalogs']) + len(results['schemas']) + len(results['tables'])
      )

      return {
        'success': True,
        'results': results,
        'counts': {
          'catalogs': len(results['catalogs']),
          'schemas': len(results['schemas']),
          'tables': len(results['tables']),
          'total': total_count,
        },
        'message': f'Found {total_count} matching object(s)',
      }

    except Exception as e:
      print(f'❌ Error searching Unity Catalog objects: {str(e)}')
      return {
        'success': False,
        'error': f'Error: {str(e)}',
        'results': {'catalogs': [], 'schemas': [], 'tables': []},
        'counts': {'catalogs': 0, 'schemas': 0, 'tables': 0, 'total': 0},
      }


# End of load_tools function

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

        return {'success': True, 'data': {'columns': columns, 'rows': data}, 'row_count': len(data)}
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
            'type': warehouse.warehouse_type.value if warehouse.warehouse_type else 'UNKNOWN',
            'creator': warehouse.creator_name if hasattr(warehouse, 'creator_name') else None,
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
      return {'success': False, 'error': f'Error: {str(e)}', 'warehouses': [], 'count': 0}

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
            'state_message': cluster.state_message if hasattr(cluster, 'state_message') else None,
            'spark_version': cluster.spark_version,
            'node_type_id': cluster.node_type_id if hasattr(cluster, 'node_type_id') else None,
            'driver_node_type_id': cluster.driver_node_type_id
            if hasattr(cluster, 'driver_node_type_id')
            else None,
            'num_workers': cluster.num_workers if hasattr(cluster, 'num_workers') else 0,
            'autoscale': {
              'min_workers': cluster.autoscale.min_workers,
              'max_workers': cluster.autoscale.max_workers,
            }
            if hasattr(cluster, 'autoscale') and cluster.autoscale
            else None,
            'creator': cluster.creator_user_name if hasattr(cluster, 'creator_user_name') else None,
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
        'state_message': cluster.state_message if hasattr(cluster, 'state_message') else None,
        'spark_version': cluster.spark_version,
        'spark_conf': cluster.spark_conf if hasattr(cluster, 'spark_conf') else {},
        'aws_attributes': cluster.aws_attributes.as_dict()
        if hasattr(cluster, 'aws_attributes') and cluster.aws_attributes
        else None,
        'node_type_id': cluster.node_type_id if hasattr(cluster, 'node_type_id') else None,
        'driver_node_type_id': cluster.driver_node_type_id
        if hasattr(cluster, 'driver_node_type_id')
        else None,
        'ssh_public_keys': cluster.ssh_public_keys if hasattr(cluster, 'ssh_public_keys') else [],
        'custom_tags': cluster.custom_tags if hasattr(cluster, 'custom_tags') else {},
        'cluster_log_conf': cluster.cluster_log_conf.as_dict()
        if hasattr(cluster, 'cluster_log_conf') and cluster.cluster_log_conf
        else None,
        'init_scripts': [script.as_dict() for script in cluster.init_scripts]
        if hasattr(cluster, 'init_scripts') and cluster.init_scripts
        else [],
        'spark_env_vars': cluster.spark_env_vars if hasattr(cluster, 'spark_env_vars') else {},
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
        'default_tags': cluster.default_tags if hasattr(cluster, 'default_tags') else {},
        'creator_user_name': cluster.creator_user_name
        if hasattr(cluster, 'creator_user_name')
        else None,
        'start_time': cluster.start_time if hasattr(cluster, 'start_time') else None,
        'terminated_time': cluster.terminated_time if hasattr(cluster, 'terminated_time') else None,
        'last_state_loss_time': cluster.last_state_loss_time
        if hasattr(cluster, 'last_state_loss_time')
        else None,
        'last_activity_time': cluster.last_activity_time
        if hasattr(cluster, 'last_activity_time')
        else None,
        'cluster_memory_mb': cluster.cluster_memory_mb
        if hasattr(cluster, 'cluster_memory_mb')
        else None,
        'cluster_cores': cluster.cluster_cores if hasattr(cluster, 'cluster_cores') else None,
        'spec': cluster.spec.as_dict() if hasattr(cluster, 'spec') and cluster.spec else None,
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
        target_states = ['RUNNING', 'PENDING', 'TERMINATING', 'TERMINATED', 'SUCCESS', 'FAILED']
      else:
        target_states = ['RUNNING', 'PENDING', 'TERMINATING']

      # Check recent runs for each job
      for job in all_jobs:
        try:
          # Get recent runs for this job
          runs = list(w.jobs.list_runs(
            job_id=job.job_id,
            limit=limit,
            expand_tasks=True
          ))

          # Filter for running or recently completed runs
          for run in runs:
            if run.state and run.state.life_cycle_state and run.state.life_cycle_state.value in target_states:
              # Build run details
              run_info = {
                'job_id': job.job_id,
                'job_name': job.settings.name if job.settings else 'Unnamed Job',
                'run_id': run.run_id,
                'run_name': run.run_name if hasattr(run, 'run_name') else None,
                'state': run.state.life_cycle_state.value if run.state and run.state.life_cycle_state else 'UNKNOWN',
                'state_message': run.state.state_message if run.state and hasattr(run.state, 'state_message') else None,
                'start_time': run.start_time if hasattr(run, 'start_time') else None,
                'end_time': run.end_time if hasattr(run, 'end_time') else None,
                'run_page_url': run.run_page_url if hasattr(run, 'run_page_url') else None,
                'run_type': run.run_type.value if hasattr(run, 'run_type') and run.run_type else None,
                'creator_user_name': run.creator_user_name if hasattr(run, 'creator_user_name') else None,
                'tasks': []
              }

              # Add task details if available
              if hasattr(run, 'tasks') and run.tasks:
                for task in run.tasks:
                  task_info = {
                    'task_key': task.task_key if hasattr(task, 'task_key') else None,
                    'state': task.state.life_cycle_state.value if task.state and task.state.life_cycle_state else 'UNKNOWN',
                    'start_time': task.start_time if hasattr(task, 'start_time') else None,
                    'end_time': task.end_time if hasattr(task, 'end_time') else None,
                    'duration': task.duration if hasattr(task, 'duration') else None
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
        'total_jobs': len(all_jobs)
      }

    except Exception as e:
      print(f'❌ Error listing running jobs: {str(e)}')
      return {
        'success': False,
        'error': f'Error: {str(e)}',
        'running_jobs': [],
        'count': 0,
        'state_summary': {}
      }


# End of load_tools function

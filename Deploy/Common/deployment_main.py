# ----------------------------
# Imports
# ----------------------------
import os 
import json
import requests
import time 
from datetime import datetime, timezone
from workspace_utilities import *            # Custom utility functions for workspace management
from token_utilities import *                # Handles service principal token acquisition
from workspace_item_utilities import *       # Manage items (reports, datasets, etc.) inside workspaces
import pandas as pd                          # Used for reading CSV configurations

# ----------------------------
# Environment variables (pulled from Azure DevOps variable groups)
# ----------------------------
tenant_id = os.getenv("tenant_id")           # Azure AD tenant ID for SPN authentication
client_id = os.getenv("client_id")           # SPN client ID
client_secret = os.getenv("client_secret")   # SPN client secret

deployment_env = os.getenv("deployment_env")     # Deployment environment (DEV/QA/PROD)
environment_type = os.getenv("environment_type") # Environment type (Primary/Secondary)
artifact_path = os.getenv("artifact_path")       # Path to build artifacts
build_number = os.getenv("build_number")         # Pipeline build number (for tracking)
connections_json = os.getenv("connections")      # JSON string containing connection info

# Parse the JSON string into a dictionary
connections_data = json.loads(connections_json)

# Normalize env/type values for filtering (case-insensitive, trimmed)
trimmed_lower_deployment_env = deployment_env.lower().strip()
trimmed_lower_environment_type = environment_type.lower().strip()

# ----------------------------
# Paths to configuration files (stored in repo)
# ----------------------------
config_base_path = f"Configuration/{deployment_env}"
deployment_profile_path = f"{config_base_path}/DEPLOYMENT_PROFILE.csv"
configuration_files_list = ["DEPLOYMENT_PROFILE.csv", "IN_TAKE_CONFIG.csv"]


def wait_for_items_deletion(workspace_id, access_token, max_wait_time=300, check_interval=30):
    """
    Polls the workspace to check if old items have been deleted before proceeding.
    
    Parameters:
    workspace_id (str): The workspace ID to check
    access_token (str): Authentication token
    max_wait_time (int): Maximum time to wait in seconds (default: 5 minutes)
    check_interval (int): Time between checks in seconds (default: 30 seconds)
    
    Returns:
    bool: True if deletion is complete, False if timeout
    """
    start_time = time.time()
    
    while time.time() - start_time < max_wait_time:
        try:
            current_items = list_workspace_all_items(workspace_id, access_token)
            
            # Check if any items with "_Old" suffix still exist
            old_items_exist = any(
                item.get("displayName", "").endswith("_Old") 
                for item in current_items
            )
            
            if not old_items_exist:
                print("All old items have been deleted successfully")
                return True
                
            print(f"Old items still exist, waiting {check_interval} seconds...")
            time.sleep(check_interval)
            
        except Exception as e:
            print(f"Error checking deletion status: {str(e)}")
            time.sleep(check_interval)
    
    print(f"Warning: Timeout reached ({max_wait_time}s), proceeding with deployment")
    return False

# ----------------------------
# Orchestrator Function
# ----------------------------
# ----------------------------
# Orchestrator Function
# ----------------------------
def orchestrator(tenant_id, client_id, client_secret, connections_data):
    """
    Orchestrates the deployment of Fabric workspaces & artifacts:
    - Reads deployment profiles
    - Creates/updates workspaces
    - Adds security groups
    - Deploys or updates artifacts
    """
    error_messages = []  # Collect errors during workflow instead of overwriting

    try:
        # ----------------------------
        # Step 1: Load deployment profile
        # ----------------------------
        all_deployment_profile_df = pd.read_csv(deployment_profile_path)

        # Filter rows matching environment/type and transformation layer = operations
        deployment_operation_ws_details_df = all_deployment_profile_df[
            (all_deployment_profile_df["to_be_onboarded"]) &
            (all_deployment_profile_df["deployment_env"].str.strip().str.lower() == trimmed_lower_deployment_env) &
            (all_deployment_profile_df["environment_type"].str.strip().str.lower() == trimmed_lower_environment_type) &
            (all_deployment_profile_df["transformation_layer"].str.strip().str.lower() == "operations")
        ]

        if deployment_operation_ws_details_df.empty:
            raise ValueError("No matching deployment profile found.")

        # ----------------------------
        # Step 2: Extract workspace details
        # ----------------------------
        row = deployment_operation_ws_details_df.iloc[0]
        capacity_id = row["capacity_id"]
        workspace_name = row["workspace_prefix"]
        transformation_layer = row["transformation_layer"]
        workspace_users = row["workspace_default_groups"]
        notebooks_to_deploy = row["notebooks_to_deploy"]   # List of notebooks to deploy
        deployment_code = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")

        # Get SPN token for API calls
        spn_access_token = get_spn_access_token(tenant_id, client_id, client_secret)

        # ----------------------------
        # Step 3: Check if workspace exists
        # ----------------------------
        operations_workspace_details = does_workspace_exists_by_name(workspace_name, spn_access_token)
        workspace_id = operations_workspace_details["id"] if operations_workspace_details else None

        # ----------------------------
        # Case A: Workspace does not exist → Create it
        # ----------------------------
        if workspace_id is None:
            try:
                # Create new workspace in given capacity
                workspace_id = create_workspace(workspace_name, capacity_id, spn_access_token)

                # Add default security groups
                add_security_group_to_workspace(workspace_id, workspace_name, spn_access_token, workspace_users)

                # Deploy all artifacts (initial deployment mode)
                deploy_artifacts(
                    transformation_layer, connections_data, artifact_path,
                    "ARM/" + transformation_layer, spn_access_token, workspace_id, workspace_name,
                    True, items={}, notebooks_to_deploy=notebooks_to_deploy
                )
            except Exception as e:
                # Cleanup on failure (delete workspace if partially created)
                if workspace_id:
                    try:
                        delete_workspace(workspace_id, spn_access_token)
                        workspace_id = None
                    except Exception as delete_error:
                        print(f"Warning: Failed to cleanup workspace {workspace_id}: {str(delete_error)}")
                raise e

        # ----------------------------
        # Case B: Workspace already exists → Update it
        # ----------------------------
        else:
            try:
                # Ensure security groups are up-to-date
                add_security_group_to_workspace(workspace_id, workspace_name, spn_access_token, workspace_users)

                # Get list of current workspace items (reports, datasets, etc.)
                items = list_workspace_all_items(workspace_id, spn_access_token)
                try:
                    delete_old_items(workspace_id, items, artifact_path, "ARM/" + transformation_layer, spn_access_token)
                    wait_for_items_deletion(workspace_id, spn_access_token, max_wait_time=300, check_interval=30)

                except Exception as delete_exc:
                    error_messages.append(f"Delete items error: {str(delete_exc)}")

                # Deploy updated artifacts (update mode)
                deploy_artifacts(
                    transformation_layer, connections_data, artifact_path,
                    "ARM/" + transformation_layer, spn_access_token, workspace_id, workspace_name,
                    False, items=items, notebooks_to_deploy=notebooks_to_deploy
                )
            except Exception as exc:
                error_messages.append(str(exc))

        # ----------------------------
        # Step 4: Handle errors if any
        # ----------------------------
        if error_messages:
            combined_error_message = "; ".join(error_messages)
            raise Exception(combined_error_message)

    except Exception as e:
        # Re-raise proper exceptions only
        raise e if isinstance(e, BaseException) else Exception(f"Orchestrator error: {repr(e)}")



# ----------------------------
# Script Entry Point
# ----------------------------
if __name__ == "__main__":
    try:
        # Call orchestrator with secrets/config pulled from variable groups
        orchestrator(tenant_id, client_id, client_secret, connections_data)
    except Exception as e:
        print(f"Deployment failed: {str(e)}")
        raise e

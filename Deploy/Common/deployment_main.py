# ----------------------------
# Imports
# ----------------------------
import os 
import json
import requests
import time 
from datetime import datetime, timezone
from workspace_utilities import *
from token_utilities import *
from workspace_item_utilities import *
import pandas as pd

# ----------------------------
# Environment Variables
# ----------------------------
tenant_id = os.getenv("TENANT_ID")
client_id = os.getenv("CLIENT_ID")
client_secret = os.getenv("CLIENT_SECRET")

deployment_env = os.getenv("DEPLOYMENT_ENV")
environment_type = os.getenv("ENVIRONMENT_TYPE")
artifact_path = os.getenv("ARTIFACT_PATH")
build_number = os.getenv("BUILD_NUMBER")
connections_file_path = os.getenv("connections_file", "connections.json")

# ----------------------------
# Load Connections JSON from file
# ----------------------------
try:
    with open(connections_file_path, "r") as file:
        connections_data = json.load(file)
except Exception as e:
    raise Exception(f"Failed to read connections JSON file at {connections_file_path}: {str(e)}")

trimmed_lower_deployment_env = deployment_env.lower().strip()
trimmed_lower_environment_type = environment_type.lower().strip()

# ----------------------------
# Paths
# ----------------------------
config_base_path = f"Configuration/{deployment_env}"
deployment_profile_path = f"{config_base_path}/DEPLOYMENT_PROFILE.csv"

# ----------------------------
# Utility Function
# ----------------------------
def wait_for_items_deletion(workspace_id, access_token, max_wait_time=300, check_interval=30):
    start_time = time.time()
    
    while time.time() - start_time < max_wait_time:
        try:
            current_items = list_workspace_all_items(workspace_id, access_token)
            old_items_exist = any(item.get("displayName", "").endswith("_Old") for item in current_items)
            
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
# Orchestrator
# ----------------------------
def orchestrator(tenant_id, client_id, client_secret, connections_data):
    error_messages = []

    try:
        df = pd.read_csv(deployment_profile_path)

        df_filtered = df[
            (df["to_be_onboarded"]) &
            (df["deployment_env"].str.strip().str.lower() == trimmed_lower_deployment_env) &
            (df["environment_type"].str.strip().str.lower() == trimmed_lower_environment_type) &
            (df["transformation_layer"].str.strip().str.lower() == "operations")
        ]

        if df_filtered.empty:
            raise ValueError("No matching deployment profile found.")

        row = df_filtered.iloc[0]
        capacity_id = row["capacity_id"]
        workspace_name = row["workspace_prefix"]
        transformation_layer = row["transformation_layer"]
        workspace_users = row["workspace_default_groups"]
        notebooks_to_deploy = row["notebooks_to_deploy"]

        deployment_code = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        spn_access_token = get_spn_access_token(tenant_id, client_id, client_secret)

        operations_workspace_details = does_workspace_exists_by_name(workspace_name, spn_access_token)
        workspace_id = operations_workspace_details["id"] if operations_workspace_details else None

        if workspace_id is None:
            try:
                workspace_id = create_workspace(workspace_name, capacity_id, spn_access_token)
                add_security_group_to_workspace(workspace_id, workspace_name, spn_access_token, workspace_users)

                deploy_artifacts(
                    transformation_layer, connections_data, artifact_path,
                    "ARM/" + transformation_layer, spn_access_token, workspace_id, workspace_name,
                    True, items={}, notebooks_to_deploy=notebooks_to_deploy
                )
            except Exception as e:
                if workspace_id:
                    try:
                        delete_workspace(workspace_id, spn_access_token)
                    except Exception as delete_error:
                        print(f"Warning: Failed to cleanup workspace {workspace_id}: {str(delete_error)}")
                raise e
        else:
            try:
                add_security_group_to_workspace(workspace_id, workspace_name, spn_access_token, workspace_users)
                items = list_workspace_all_items(workspace_id, spn_access_token)

                try:
                    delete_old_items(workspace_id, items, artifact_path, "ARM/" + transformation_layer, spn_access_token)
                    wait_for_items_deletion(workspace_id, spn_access_token)
                except Exception as delete_exc:
                    error_messages.append(f"Delete items error: {str(delete_exc)}")

                deploy_artifacts(
                    transformation_layer, connections_data, artifact_path,
                    "ARM/" + transformation_layer, spn_access_token, workspace_id, workspace_name,
                    False, items=items, notebooks_to_deploy=notebooks_to_deploy
                )
            except Exception as exc:
                error_messages.append(str(exc))

        if error_messages:
            raise Exception("; ".join(error_messages))

    except Exception as e:
        raise e

# ----------------------------
# Script Entry Point
# ----------------------------
if __name__ == "__main__":
    try:
        orchestrator(tenant_id, client_id, client_secret, connections_data)
    except Exception as e:
        print(f"Deployment failed: {str(e)}")
        raise e

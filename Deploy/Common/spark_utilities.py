import requests
import time
from datetime import datetime, timezone

def create_environment(workspace_id, access_token, display_name, description):
    """
    Creates an environment in a given workspace in Microsoft Fabric.

    Parameters:
        workspace_id (str): The ID of the workspace.
        access_token (str): The service principal access token for authentication.
        display_name (str): The display name of the environment.
        description (str): A description for the environment.

    Returns:
        str: The ID of the created environment.
    """
    # Define the API endpoint for creating environments within a specific workspace.
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/environments"

    # Set up the headers for the HTTP request, including authentication and content type.
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    # Prepare the payload (body) of the request with the environment's display name and description.
    payload = {
        "displayName": display_name,
        "description": description
    }

    try:
        # Send a POST request to create the environment.
        response = requests.post(url, headers=headers, json=payload)
        # Raise an HTTPError for bad responses (4xx or 5xx).
        response.raise_for_status()
        # Extract and return the 'id' of the newly created environment from the JSON response.
        return response.json().get("id", "").strip()
    except requests.exceptions.RequestException as e:
        # Catch any request-related exceptions and re-raise with a descriptive error message.
        raise Exception(f"Error occurred while creating environment: {str(e)}")


def publish_environment(workspace_id, artifact_id, access_token):
    """
    Publishes the environment after uploading libraries.

    Parameters:
        workspace_id (str): The ID of the workspace.
        artifact_id (str): The ID of the environment artifact.
        access_token (str): The service principal access token for authentication.

    Returns:
        dict: The response from the API confirming the publish operation.
    """
    # Define the API endpoint for publishing a specific environment's staging version.
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/environments/{artifact_id}/staging/publish"

    # Set up the headers for the HTTP request, including authentication.
    headers = {"Authorization": f"Bearer {access_token}"}

    try:
        # Send a POST request to initiate the publishing process.
        response = requests.post(url, headers=headers)
        # Raise an HTTPError for bad responses (4xx or 5xx).
        response.raise_for_status()
        # Return the JSON response from the API.
        return response.json()
    except requests.exceptions.RequestException as e:
        # Catch any request-related exceptions and re-raise with a descriptive error message.
        raise Exception(f"Error publishing environment: {str(e)}")


def poll_environment_publish_status(workspace_id, artifact_id, access_token, polling_interval=60, maximum_duration=1200):
    """
    Polls the environment publish status at given intervals.
    Uses a 5-minute interval for the first 10 minutes, then switches to the provided polling_interval.
    Stops polling as soon as the status changes from 'Running' or when the maximum duration is exceeded.

    Parameters:
        workspace_id (str): The ID of the workspace.
        artifact_id (str): The ID of the environment.
        access_token (str): The service principal access token for authentication.
        polling_interval (int): Time in seconds between each poll after the first 10 minutes (default: 60 seconds).
        maximum_duration (int): Maximum duration in seconds to poll (default: 1200 seconds).

    Returns:
        str: The publish state once it is no longer 'Running'.

    Raises:
        Exception: If the maximum polling duration is exceeded without a state change.
    """
    # Define the API endpoint to get the details of a specific environment.
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/environments/{artifact_id}"
    # Set up the headers for the HTTP GET request, including authentication and content type.
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    elapsed_time = 0 # Initialize a counter for elapsed polling time.

    # Loop until the maximum polling duration is exceeded.
    while elapsed_time < maximum_duration:
        try:
            # Send a GET request to retrieve the environment's current status.
            response = requests.get(url, headers=headers)
            # Raise an HTTPError for bad responses (4xx or 5xx).
            response.raise_for_status()
            result = response.json() # Parse the JSON response.

            # Extract the current publish state from the API response's nested structure.
            current_state = result.get("properties", {}).get("publishDetails", {}).get("state", None)

            # If the current state is not 'Running', the publishing process has completed (or failed), so return the state.
            if current_state != "Running":
                return current_state

        except Exception as e:
            # Catch any exceptions during the API call and re-raise with a descriptive error message.
            raise(f"Error getting environment publish status: {str(e)}")

        # Determine the sleep interval: 5 minutes for the first 10 minutes, then the specified polling_interval.
        if elapsed_time < 600: # 600 seconds = 10 minutes
            sleep_interval = 300  # 5 minutes
        else:
            sleep_interval = polling_interval

        # Pause execution for the determined interval.
        time.sleep(sleep_interval)
        # Increment the elapsed time.
        elapsed_time += sleep_interval

    # If the loop completes without the state changing from 'Running', raise an exception.
    raise Exception("Maximum polling duration exceeded without status change.")


def update_default_environment(workspace_id, access_token, environment_name, runtime_version):
    """
    Updates the default environment settings in a workspace.

    Parameters:
        workspace_id (str): The ID of the workspace.
        access_token (str): The service principal access token for authentication.
        environment_name (str): The name of the environment.
        runtime_version (str): The runtime version to set.

    Returns:
        dict: The response from the API confirming the update.
    """
    # Define the API endpoint for updating Spark settings within a specific workspace.
    url = f"https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/spark/settings"

    # Set up the headers for the HTTP request, including authentication and content type.
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }

    # Prepare the payload with the new environment name and runtime version to be set as default.
    payload = {
        "environment": {
            "name": environment_name,
            "runtimeVersion": runtime_version
        }
    }

    try:
        # Send a PATCH request to update the Spark environment settings.
        response = requests.patch(url, headers=headers, json=payload)
        # Raise an HTTPError for bad responses (4xx or 5xx).
        response.raise_for_status()
        # Return the JSON response from the API.
        return response.json()
    except requests.exceptions.RequestException as e:
        # Catch any request-related exceptions and re-raise with a descriptive error message.
        raise Exception(f"Error updating default environment: {str(e)}")


def deploy_custom_environment(workspace_id, access_token):
    """
    Deploys a custom environment to a workspace, including creating the environment,
    uploading a library, publishing the environment, and setting it as the default environment.

    Parameters:
    - workspace_id (str): The ID of the workspace.
    - access_token (str): The service principal access token to authenticate with the workspace.

    Raises:
        Exception: If an error occurs during any of the deployment steps, an exception is raised.
    """
    try:
        env_name = "Spark_Environment" # Define a name for the new Spark environment.
        artifact_deployment_time = datetime.now(timezone.utc) # Capture the current UTC time for deployment tracking (though not explicitly used later in this snippet).

        # Phase 1: Create the Environment
        # Call the helper function to create a new environment and retrieve its ID.
        # The description is set to None as per the original function's call.
        artifact_id = create_environment(workspace_id, access_token, env_name, None)
        print(f"Environment '{env_name}' created with ID: {artifact_id}")

        # Phase 2: Publish the Environment
        # Call the helper function to publish the newly created environment.
        publish_response = publish_environment(workspace_id, artifact_id, access_token)
        print(f"Environment '{artifact_id}' publishing initiated. Response: {publish_response}")

        # Phase 3: Poll Environment Publish Status
        # Continuously check the publish status until it's no longer 'Running' or max duration is reached.
        publish_status = poll_environment_publish_status(workspace_id, artifact_id, access_token)
        print(f"Environment '{artifact_id}' publish status: {publish_status}")

        # Phase 4: Set as Default Environment (Conditional on Publish Success)
        # If the environment was published successfully, proceed to set it as the default.
        if publish_status.lower() == "success":
            # Call the helper function to update the workspace's default Spark environment settings.
            update_response = update_default_environment(workspace_id, access_token, env_name, "1.3")
            print(f"Environment '{env_name}' set as default with runtime version '1.3'. Response: {update_response}")
        else:
            # If publishing failed, raise an exception indicating the failure.
            raise Exception("Error in publishing environment.")
    except Exception as e:
        # Catch any exceptions that occur during the deployment process and re-raise with a comprehensive error message.
        error_message = f"Error occurred while deploying custom environment: {str(e)}"
        raise Exception(error_message)
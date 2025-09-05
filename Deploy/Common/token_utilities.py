import requests
from azure.identity import UsernamePasswordCredential

def get_spn_access_token(tenant_id, client_id, client_secret):
    """
    Retrieve an access token using a service principal.

    Parameters:
    - tenant_id: Tenant ID of the Azure Active Directory tenant.
    - client_id: The client ID of the service principal (application ID).
    - client_secret: The client secret of the service principal.

    Returns:
    - str: The access token (Bearer token).
    """

    # Define the token endpoint URL for Azure AD, which includes the tenant ID.
    url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"

    # Define the payload (body) of the POST request for client credentials flow.
    # - client_id: The application's client ID.
    # - client_secret: The application's client secret.
    # - grant_type: Specifies the OAuth 2.0 grant type as 'client_credentials'.
    # - scope: Defines the permissions requested. Here, it requests default permissions
    #          for the Microsoft Fabric API.
    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials",
        "scope": "https://api.fabric.microsoft.com/.default",
    }

    # Make a POST request to the token endpoint with the defined payload.
    response = requests.post(url, data=payload)

    # Check if the request was successful (HTTP status code 200).
    if response.status_code == 200:
        # Parse the JSON response to extract the 'access_token'.
        token_data = response.json()
        return token_data["access_token"]
    else:
        # If the request failed, raise an exception with the status code and error message.
        raise Exception(f"Failed to get access token: {response.status_code}, {response.text}")


def get_upn_access_token(upn_client_id, upn_user_id, upn_password):
    """
    Retrieve an access token using UPN (username/password) credentials.

    Parameters:
    - upn_client_id: The client ID of the application registered in Azure AD.
    - upn_user_id: The User Principal Name (username) of the user.
    - upn_password: The password of the user.

    Returns:
    - str: The access token (Bearer token).
    """

    try:
        # Create an instance of UsernamePasswordCredential from azure.identity.
        # This object handles the complexities of authenticating with Azure AD using UPN.
        credential = UsernamePasswordCredential(
            client_id=upn_client_id,
            username=upn_user_id,
            password=upn_password
        )

        # Get the access token. The scope "https://analysis.windows.net/powerbi/api/.default"
        # is typically used for Power BI API access. This can be adjusted for other APIs
        # if needed. The .token attribute extracts the actual token string.
        token = credential.get_token("https://analysis.windows.net/powerbi/api/.default").token

        # Return the retrieved access token.
        return token

    except Exception as e:
        # Catch any exceptions that occur during the credential or token retrieval process
        # and raise a new exception with a descriptive error message.
        raise Exception(f"Failed to get access token: {str(e)}")
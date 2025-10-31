import msal
import requests
import logging
import pandas as pd
from django.conf import settings

logger = logging.getLogger(__name__)

class PowerBIConnector:
    def __init__(self):
        """
        Initializes the Power BI connector and authenticates with Azure AD.
        Credentials are fetched from Django's POWERBI_CREDENTIALS.
        """
        self.credentials = settings.POWERBI_CREDENTIALS
        self.access_token = self._get_access_token()
        self.headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
        self.base_url = "https://api.powerbi.com/v1.0/myorg/groups"

    def _get_access_token(self) -> str:
        """
        Acquires an access token for the Power BI API using client credentials flow.
        """
        client_id = self.credentials.get("client_id")
        tenant_id = self.credentials.get("tenant_id")
        client_secret = self.credentials.get("client_secret")

        if not all([client_id, tenant_id, client_secret]):
            raise ValueError("Power BI credentials are not complete. Please check Django settings.")

        authority = f"https://login.microsoftonline.com/{tenant_id}"
        scope = ["https://analysis.windows.net/powerbi/api/.default"]
        
        try:
            client = msal.ConfidentialClientApplication(
                client_id=client_id, authority=authority, client_credential=client_secret
            )
            response = client.acquire_token_for_client(scopes=scope)

            if 'access_token' not in response:
                error_msg = f"Failed to acquire Power BI access token: {response.get('error_description', 'Unknown error')}"
                logger.error(error_msg)
                raise Exception(error_msg)

            logger.info("Power BI access token acquired successfully.")
            return response['access_token']
        except Exception as e:
            error_msg = f"Error during Power BI token acquisition: {str(e)}"
            logger.error(error_msg)
            raise ConnectionError(error_msg)

    def _execute_dax_query(self, workspace_id: str, dataset_id: str, dax_query: str) -> pd.DataFrame:
        """
        Executes a DAX query against a Power BI dataset and returns results as a DataFrame.
        """
        api_url = f"{self.base_url}/{workspace_id}/datasets/{dataset_id}/executeQueries"
        
        payload = {
            "queries": [
                {
                    "query": dax_query,
                    "queryType": "DAX"
                }
            ],
            "serializerSettings": {
                "includeNulls": True
            }
        }
        
        try:
            logger.info(f"Sending DAX query to Power BI: {dax_query}")
            response = requests.post(api_url, headers=self.headers, json=payload)
            logger.info(f"Power BI API response status: {response.status_code}")
            logger.info(f"Power BI API response content: {response.text}")
            response.raise_for_status()
            result = response.json()

            if not result.get('results') or not result['results'][0].get('tables') or not result['results'][0]['tables'][0].get('rows'):
                logger.warning("No rows found in Power BI query result.")
                return pd.DataFrame()

            rows = result['results'][0]['tables'][0]['rows']
            df = pd.DataFrame(rows)
            logger.info("DAX query successful and data converted to DataFrame.")
            return df
        except requests.exceptions.RequestException as e:
            error_msg = f"Power BI API Request Error: {str(e)}"
            logger.error(error_msg)
            if e.response is not None:
                logger.error(f"Power BI Response content: {e.response.text}")
                error_msg += f" Response: {e.response.text}"
            raise RuntimeError(error_msg)
        except Exception as e:
            error_msg = f"Error processing Power BI query result: {str(e)}"
            logger.error(error_msg)
            raise RuntimeError(error_msg)
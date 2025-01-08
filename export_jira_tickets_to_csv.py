import requests
from requests.auth import HTTPBasicAuth
import pandas as pd
from datetime import datetime

# Your Jira credentials
JIRA_URL = "jira-url" #example https://orgnization.atlassian.net
USERNAME = "username"
API_TOKEN = "your-token"
FILTER_ID = "filter-id"

try:
    # Get the filter's JQL query
    filter_url = f"{JIRA_URL}/rest/api/3/filter/{FILTER_ID}"
    filter_response = requests.get(
        filter_url,
        auth=HTTPBasicAuth(USERNAME, API_TOKEN)
    )
    filter_response.raise_for_status()
    
    jql_query = filter_response.json()['jql']
    
    # Get issues using the JQL query
    search_url = f"{JIRA_URL}/rest/api/3/search"
    params = {
        'jql': jql_query,
        'maxResults': 1000,
        'fields': 'summary,issuetype,customfield_10004,status'
    }
    
    search_response = requests.get(
        search_url,
        auth=HTTPBasicAuth(USERNAME, API_TOKEN),
        params=params
    )
    search_response.raise_for_status()
    
    # Extract issues
    issues = search_response.json()['issues']
    
    # Transform to DataFrame with new column names and default values
    data = []
    for issue in issues:
        data.append({
            'Module': 'Commerce - SAP Hybris',  # Default value for first column
            'Ticket#': issue['key'],            # Renamed from 'Issue key'
            'Description': issue['fields']['summary'],  # Renamed from 'Summary'
            'Type': issue['fields']['issuetype']['name'],  # Renamed from 'Issue Type'
            'Story Points': issue['fields'].get('customfield_10004', ''),  # Kept same name
            'Status': issue['fields']['status']['name'],  # Kept same name
            'Reopen': 'NO'  # Default value for last column
        })
    
    df = pd.DataFrame(data)
    
    # Save to current directory with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"jira_export_.csv"
    df.to_csv(filename, index=False)
    print(f"CSV file saved successfully: {filename}")
    
except Exception as e:
    print(f"An error occurred: {str(e)}")
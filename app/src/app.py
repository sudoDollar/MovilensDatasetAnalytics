from flask import Flask, jsonify, request
import requests

app = Flask(__name__)

# Databricks workspace URL and token
databricks_url = "https://dbc-2f2560fd-e86a.cloud.databricks.com/?o=8200463898314705"
token = "dapi502d8d59361c3460ea5792cec587a481"

# Example Spark SQL query
spark_sql_query = "SELECT * FROM moviesTable"

@app.route('/query', methods=['POST'])
def query_databricks():
    # Retrieve query parameters from the request
    
    # Create a REST API endpoint for submitting Spark SQL queries
    endpoint = f"{databricks_url}/api/2.0/sql/endpoints/execute-query"
    print(endpoint)
    # Define the query payload
    payload = {
        "language": "sql",
        "clusterId": "your_cluster_id",
        "context": "your_sql_context_id",
        "query": spark_sql_query,
        # Add any other parameters or configurations as needed
    }

    # Set up headers with the Databricks token
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    # Make the request to Databricks
    response = requests.post(endpoint, json=payload, headers=headers)

    # Handle the response
    if response.status_code == 200:
        result = response.json()
        # Process the result and return it as needed
        return jsonify(result)
    else:
        return jsonify({"error": f"Query failed with status code {response.status_code}"})

if __name__ == '__main__':
    app.run(debug=True)
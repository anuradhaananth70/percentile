import streamlit as st
import pandas as pd
import numpy as np
from google.cloud import bigquery

# Authenticate to BigQuery
client = bigquery.Client()

# Load data from BigQuery
def load_data():
    job = client.get_job('bquxjob_a9ca091_18f0a7026c8')
    return job.to_dataframe()

# Function to calculate percentiles and create DataFrame
def calculate_table(results):
    # Replace this list with the actual values of _video_count column
    video_count_values = results['_video_count']

    # Calculate the p100th percentile
    p100th_percentile = np.percentile(video_count_values, 100)
    p99th_percentile = np.percentile(video_count_values, 99)

    # Filter the DataFrame to get only users in the 99-100 percentage bucket
    users_99_100 = results[results['completion_percentage_range'] == '99%-100%']

    # Group by 'event_date' to get the total count of unique users for each day
    unique_users_per_day = users_99_100.groupby('event_date')['user_id'].nunique().reset_index()
    unique_users_per_day.columns = ['event_date', 'total_unique_users']

    # Create the DataFrame with the specified columns
    table = pd.DataFrame({
        'event_date': unique_users_per_day['event_date'],
        'total_unique_users': unique_users_per_day['total_unique_users'],
        'P100th_value': p100th_percentile,
        'P99th_value': p99th_percentile
    })
    return table

# Main function to run the Streamlit app
def main():
    st.title('Streamlit App for BigQuery Results')
    
    # Load data
    st.write('Loading data from BigQuery...')
    results = load_data()
    st.write('Data loaded successfully!')
    
    # Calculate table
    st.write('Calculating table...')
    table = calculate_table(results)
    st.write('Table calculated successfully!')
    
    # Display table
    st.write('Displaying table:')
    st.write(table)

if __name__ == "__main__":
    main()

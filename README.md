# mongobqetl

A Python ETL pipeline that extracts data from MongoDB, cleans and enriches it, and loads it into Google BigQuery.  
It processes the `ESIS` and `ESOS` collections while avoiding duplicate records.

## Features

- Extract data from MongoDB collections
- Normalize nested MongoDB documents
- Clean and transform date fields
- Compute derived metrics (lease duration, year/month, financial differences)
- Remove MongoDB technical fields (`$oid`, `$date`)
- Load only new records into BigQuery

## Technologies

- Python
- Pandas
- MongoDB
- Google BigQuery

## Requirements

- Python 3.8+
- MongoDB
- Google Cloud project with BigQuery enabled

## Install dependencies:
pip install pandas pymongo google-cloud-bigquery google-auth python-dateutil



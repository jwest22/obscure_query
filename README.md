# Obscura Pro Machina

## Installation

To run this application, you'll need to install the necessary dependencies:

- streamlit
- pandas
- duckdb
- datasketch

or `pip install -r requirements.txt`

## Running the Application

To start the application, navigate to your project directory in the terminal and run:

streamlit run main.py

## Features

### Database Loading

- Build Database File: Load CSV files from a specified directory into the DuckDB database.
- Please note that DuckDB is configured as a file database for persistence.

### SQL Query Execution

- Database Schema: View the schema of the currently loaded tables in the database.
- Write SQL Query: Input and execute custom SQL queries against the DuckDB database.
- The output of DML queries executed against the file DB will be persisted.

### Database Utilities

- Build Similarity Index: Generate a similarity index for data assets.
- Build Cardinality Index: Create a cardinality index for database tables.
- Build Relation Map: Establish a relation map based on the cardinality and similarity indexes.

## Usage

The application interface is divided into several sections:

1. Database Schema: View the current schema of the database. Reload the application to reflect any changes made to the database.

2. Write SQL Query: This section allows you to write and execute SQL queries directly. DML operations are supported.

3. Utilities: Buttons for utility functions: building database files, similarity indexes, cardinality indexes, and relation maps.

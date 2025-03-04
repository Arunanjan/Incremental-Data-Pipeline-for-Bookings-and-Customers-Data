Databricks ETL Pipeline for Booking and Customer Data
Overview
This project processes daily booking and customer data in Databricks using PySpark, Delta Lake, and PyDeequ for data quality validation. The pipeline includes data ingestion, validation, transformation, and storage in Delta tables.
Steps
1. Environment Setup
	The script retrieves the Spark version from the environment.
	Fetches the current_date parameter from Databricks widgets.
2. Data Ingestion
	Reads daily CSV files from DBFS:
	Bookings: dbfs:/DataEngineering/bookings_daily_data/bookings_<date>.csv
	Customers: dbfs:/DataEngineering/customers_daily_data/customers_<date>.csv
	Loads data into PySpark DataFrames with schema inference and multiline handling.
3. Data Quality Checks
	Uses PyDeequ to validate booking and customer data:
	Ensures data is not empty.
	Validates uniqueness and completeness of key fields.
	Checks non-negative values for numeric fields.
	Displays verification results and raises an error if validation fails.
4. Transformation & Business Logic
	Adds an ingestion_time column to the booking data.
	Joins booking data with customer data.
	Computes total_cost after discount and filters records where quantity > 0.
	Aggregates data by booking_type and customer_id to calculate:
	Total amount sum.
	Total quantity sum.
5. Writing to Delta Lake
	Fact Table (booking_fact):
	Checks if the Delta table exists.
	If exists, merges new data with existing data and re-aggregates.
	Otherwise, writes new aggregated data as a fresh table.
	Slowly Changing Dimension Type 2 (SCD2) (customer_scd):
	If the SCD table exists, performs an SCD Type 2 Merge:
	Updates valid_to for existing records.
	Appends new customer data.
	If the table doesn't exist, writes a new Delta table.
Data Flow
	Source: DBFS (Bookings & Customers CSV files)
	Processing Layer: Databricks (PySpark, Delta Lake, PyDeequ)
	Storage: Delta Tables (booking_fact, customer_scd)
Execution
	Run the Databricks notebook with the current_date parameter.
	Ensure DBFS contains the required daily CSV files.
	Verify logs and Delta tables after execution.
Technologies Used
	Databricks for cloud-based ETL processing
	PySpark for distributed data processing
	Delta Lake for ACID-compliant storage
	PyDeequ for data quality validation


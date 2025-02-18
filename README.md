# Project: Sparkify Data Warehouse Pipeline

This project is a data warehousing solution for Sparkify, a music streaming service. It builds an ELT pipeline to extract user activity and song metadata from S3, stage the data in Amazon Redshift, and transform it into a star schema for analytics. The star schema contains one fact table and four dimension tables.

## Data sources

* s3://udacity-dend/song_data
* s3://udacity-dend/log_data
* s3://udacity-dend/log_json_path.json

## Table Schema

The star schema for this data warehouse consists of a fact table and four dimension tables.  The SQL table creation statements are below:

### 1. Staging Tables

These tables hold raw data before transformations.

**staging_events** - Contains user activity logs.

**staging_songs** - Contains song metadata.


### 2.1 Fact Table

**songplays_fact** - Contains records of song plays, capturing information about each play event (user, song, timestamp, etc.).

### 2.2 Dimension Tables

These tables hold descriptive data joined with the fact table for analysis.

**users_dim** - Contains user information.

**songs_dim** - Contains song metadata.

**artists_dim** - Contains artist metadata.

**time_dim** - Contains timestamp breakdown for time-based queries.

## ELT Process Overview

The ELT pipeline follows these steps:

1. `Load Staging Tables`

   Data is loaded from S3 buckets (song and log data) into Redshift staging tables (staging_events and staging_songs) using the COPY command.

2. `Transform Data`

   Data from staging tables is transformed and inserted into fact and dimension tables using SQL in Redshift. The fact table (songplays_fact) stores song play events, and dimension tables hold details about users, songs, artists, and timestamps.

## Project Structure

The project's codebase consists of the following key components:

### Core Components

1. **`README.md`**  
   Provides a comprehensive overview of the project, including setup instructions, execution steps, and troubleshooting guidance.

2. **`dwh.cfg`**  
   Contains sensitive information such as authentication credentials and connection parameters required to interact with the Amazon Redshift cluster.  
   **Note**: Ensure this file is not committed to version control (e.g., add it to `.gitignore`).

3. **`create_tables.py`**  
   Establishes the relational structure within the Amazon Redshift database. It creates the fact and dimension tables that comprise the star schema.

4. **`elt.py`**  
   Orchestrates the ETL pipeline. It handles:
   - Extracting data from S3.
   - Loading data into Redshift staging tables.
   - Transforming and populating the final fact and dimension tables.

5. **`data_quality_checks.py`**  
   Ensures the integrity and accuracy of the data loaded into Redshift. It performs validation checks such as:
   - Verifying the absence of null values in key columns.
   - Ensuring referential integrity between tables.
   - Confirming data completeness and consistency.

6. **`sql_queries.py`**  
   Centralizes all SQL commands used by `create_tables.py`, `elt.py`, and `data_quality_checks.py`. This includes:
   - Table creation and dropping queries.
   - Data insertion and transformation queries.
   - Data quality check queries.

7. **`analytics_queries.sql`**  
   Includes analytical queries and their visualisations to generate insights from the data stored in Redshift. Examples include:
   - Song popularity.
   - Usage metrics.
   - Subscription levels.

### Optional Components

1. **`iac_aws_resources_create.ipynb`**  
   Contains infrastructure-as-code (IaC) for provisioning AWS resources, including:
   - Redshift cluster.
   - IAM roles and policies.
   - S3 buckets.  
   This notebook uses the Boto3 SDK to programmatically create and configure resources.

2. **`iac_aws_resources_cleanup.ipynb`**  
   Contains infrastructure-as-code (IaC) for cleaning up AWS resources created by `iac_aws_resources_create.ipynb`. This ensures no unnecessary costs are incurred after the project is complete.

---

## Setup Instructions

### Prerequisites

1. AWS Resources Setup
You have two options to set up the required AWS resources:

   **Option 1A: Use an Existing Redshift Cluster**

   - Set up an Amazon Redshift cluster manually.
   - Note the connection details:
   - - Host (endpoint)
   - - Port
   - - Database name
   - - Username
   - - Password


   - Ensure your AWS credentials have permissions to access S3 and Redshift.

   - Modify the `dwh.cfg` file with your credentials and cluster details.

   **Option 1B: Automate Resource Creation with infrastructure-as-code (IaC)**

   - Run the `iac_aws_resources_create.ipynb` notebook to automatically provision the required AWS resources, including:

   - - Redshift cluster

   - - IAM role

   - - S3 buckets

   - Follow the instructions to update the `dwh.cfg` file.

   -------

### **1. AWS Resources Setup**  
You have two options to set up the required AWS resources:  

#### **Option: Use an Existing Redshift Cluster**  
- Set up an Amazon Redshift cluster manually.  
- Note the following connection details:  
  - **Host** (endpoint)  
  - **Port**  
  - **Database name**  
  - **Username**  
  - **Password**  
- Ensure your AWS credentials have permissions to access **S3** and **Redshift**.  
- Modify the `dwh.cfg` file with your credentials and cluster details.  

#### **Option: Automate Resource Creation with Infrastructure-as-Code (IaC)**  
- Run the **`iac_aws_resources_create.ipynb`** notebook to automatically provision the required AWS resources, including:  
  - **Redshift cluster**  
  - **IAM role**  
- Follow the instructions to update the `dwh.cfg` file accordingly.

### **2. Clone the repository**  
```bash
git clone <repository_url>
```

### **3. Install the requirements file**  
Run the following command to install the required Python dependencies:  
```bash
pip install -r requirements.txt
```

### **4. Create Tables in Redshift**  
Run the following command to create the necessary tables in Redshift:  
```bash
python create_tables.py
```

### **5. Run the ELT Pipeline**  
Execute the elt script to run the ELT pipeline:  
```bash
python elt.py
```

### **6. Run the Data Quality Checks Script**  
After running the ELT pipeline, execute the following script to check data quality:  
```bash
python data_quality_checks.py
```

### **7. Run analytics queries and visualise results**  
A sample can be found in `analytics` > `analytical_queries.sql`

### **END: Destroy the Created Resources (DO NOT SKIP THIS STEP)**  
Once you're done, ensure to clean up the AWS resources by following one of the two options:  

#### **Option: Clean Up Resources Manually**  
- If you prefer to clean up the resources manually, ensure to:  
  - Delete the **Redshift cluster**  
  - Remove the **IAM role**  
  - Follow AWS guidelines to terminate any other associated resources.

#### **Option: Use the Cleanup infrastructure-as-code (IaC) Notebook**  
- Run the **`iac_aws_resources_cleanup.ipynb`** notebook to automatically destroy the AWS resources that were created.



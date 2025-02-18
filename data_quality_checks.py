import configparser
import psycopg2
from sql_queries import data_quality_queries

def run_data_quality_checks(cur, conn):
    """
    Run data quality checks on the database.
    """
    print("Running data quality checks...")

    # Define descriptions for each check (for logging purposes)
    check_descriptions = [
        "Null values in songplays_fact",
        "Year consistency in songs_dim",
        "Start time accuracy in songplays_fact",
        "Unique song IDs in songs_dim"
    ]

    # Initialize a list to store the results of the checks
    check_results = []
    
    # Iterate through the data quality queries and execute them
    for i, query in enumerate(data_quality_queries):
        try:
            cur.execute(query)
            result = cur.fetchone()

            # Check if the query returned any issues
            if result is None:
                check_results.append((check_descriptions[i], "Passed", "No issues found."))
                print(f"Data quality check passed: {check_descriptions[i]}. No issues found.")
            elif result[0] > 0:
                check_results.append((check_descriptions[i], "Failed", f"Issues found: {result[0]}"))
                print(f"Data quality check failed: {check_descriptions[i]}. Issues found: {result[0]}")
            else:
                check_results.append((check_descriptions[i], "Passed", "No issues found."))
                print(f"Data quality check passed: {check_descriptions[i]}.")
        except Exception as e:
            check_results.append((check_descriptions[i], "Error", str(e)))
            print(f"Error running data quality check: {check_descriptions[i]}. Error: {e}")

    
    # Print summary of all checks
    print("\nData quality checks summary:")
    for description, status, message in check_results:
        print(f"{description}: {status} - {message}")

    # Check if any checks failed and raise an exception if necessary
    if any(status == "Failed" for _, status, _ in check_results):
        raise ValueError("One or more data quality checks failed. See the summary above for details.")
    elif any(status == "Error" for _, status, _ in check_results):
        raise ValueError("One or more data quality checks encountered an error. See the summary above for details.")
    else:
        print("All data quality checks passed successfully.")


def main():
    """
    Main function to run data quality checks.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # Connect to the database
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    # Run data quality checks
    run_data_quality_checks(cur, conn)

    # Close the connection
    conn.close()


if __name__ == "__main__":
    main()

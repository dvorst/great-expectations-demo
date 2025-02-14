from pathlib import Path
import great_expectations as gx
from great_expectations import expectations as gxe
import pandas as pd
from datetime import datetime

# Constants
URL = "https://raw.githubusercontent.com/great-expectations/gx_tutorials/main/data/yellow_tripdata_sample_2019-01.csv"
FILE = "yellow_tripdata_sample.csv"

# Download data if it doesn't exist
# you would normally not do this, but refer to the file on S3, or define an SQL query instead.
if not Path(FILE).exists():
    pd.read_csv(URL).to_csv(FILE, index=False)


def main():
    ### 1. Set up environment
    ctx = gx.get_context()

    ### 2. Connect to data
    # Define data interface
    # great-expectations calls this 'data source', not to be confused with a filesource
    ctx.data_sources.add_pandas_filesystem(name="pandas-local", base_directory=".")

    # Define assets
    # slightly different from a single file, since great-expectation allows you to combine multiple files into a
    # single asset. Can be convenient if you have files such as data_YYYY_MM_DD.parquet, but need to combine all of
    # these to run the validation.
    ctx.data_sources.get("pandas-local").add_csv_asset("tripdata")

    # Define batches
    ctx.data_sources.get("pandas-local").get_asset("tripdata").add_batch_definition_path("batch", FILE)

    ### 3. Define Expectations
    # define expectations
    expectations = [
        gxe.ExpectColumnToExist(column="passenger_count"),
        gxe.ExpectColumnToExist(column="total_amount"),
        gxe.ExpectColumnToExist(column="trip_distance"),
        gxe.ExpectTableColumnCountToEqual(value=18),
        gxe.ExpectColumnValuesToBeBetween(column="passenger_count", min_value=1, max_value=6),
        gxe.ExpectColumnValuesToBeBetween(column="total_amount", min_value=0, max_value=100),
    ]

    # Create a suite from them and add to context
    suite = gx.ExpectationSuite(name="expectations")
    for exp in expectations:
        suite.add_expectation(exp)
    ctx.suites.add(suite)
    
    # Create and add validator
    ctx.validation_definitions.add(
        gx.ValidationDefinition(
            data=ctx.data_sources.get("pandas-local").get_asset("tripdata").get_batch_definition("batch"),
            suite=ctx.suites.get("expectations"),
            name="validator",
        )
    )

    ### 4. run validators
    dag = "airflow-dag-id"
    task = "airflow-task-id"
    date = datetime.now().strftime("%y-%m-%d_%H:%M:%S")
    run_id = f"{dag}/{task}/{date}"
    ctx.validation_definitions.get("validator").run()

    # build site
    site_config = {
        "class_name": "SiteBuilder",
        "site_index_builder": {"class_name": "DefaultSiteIndexBuilder"},
        "store_backend": {
            "class_name": "TupleFilesystemStoreBackend",
            "base_directory": f"{Path().cwd()}/site",
        },
    }
    ctx.add_data_docs_site(site_name="site", site_config=site_config)
    ctx.build_data_docs()

    # open it
    ctx.open_data_docs()


if __name__ == "__main__":
    main()

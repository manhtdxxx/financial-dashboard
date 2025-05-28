from utils.logger import get_logger
from db.dwh_schema import create_or_replace_schema, create_full_view
from etl.extract import extract_dim_account, extract_fact_entry
from etl.transform import transform_dim_account, transform_fact_entry
from etl.load import load_dim_account, load_fact_entry

logger = get_logger()

if __name__ == "__main__":
    try:
        logger.info("ETL process started.")
        
        logger.info("Starting extraction of dim_account...")
        dim_account_df = extract_dim_account()
        logger.info(f"Extracted dim_account rows: {len(dim_account_df)}")
        
        logger.info("Starting extraction of fact_entry...")
        fact_entry_df = extract_fact_entry()
        logger.info(f"Extracted fact_entry rows: {len(fact_entry_df)}")
        
        logger.info("Starting transformation of dim_account...")
        transformed_dim_account_df = transform_dim_account(dim_account_df)
        logger.info(f"Transformed dim_account rows: {len(transformed_dim_account_df)}")
        
        logger.info("Starting transformation of fact_entry...")
        transformed_fact_entry_df = transform_fact_entry(fact_entry_df, transformed_dim_account_df)
        logger.info(f"Transformed fact_entry rows: {len(transformed_fact_entry_df)}")
        
        logger.info("Creating schema in target database...")
        create_or_replace_schema()
        logger.info("Schema created successfully.")
        
        logger.info("Loading dim_account into target database...")
        load_dim_account(transformed_dim_account_df)
        logger.info(f"Loaded dim_account rows: {len(transformed_dim_account_df)}")

        logger.info("Loading fact_entry into target database...")
        load_fact_entry(transformed_fact_entry_df)
        logger.info(f"Loaded fact_entry rows: {len(transformed_fact_entry_df)}")
            
        logger.info("Creating full view...")
        create_full_view()
        logger.info("Full view created successfully.")
        
        logger.info("ETL process completed successfully.\n\n")
        
    except Exception as e:
        logger.error(f"ETL process failed: {e}\n\n", exc_info=True)

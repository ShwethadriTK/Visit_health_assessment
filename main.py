import pandas as pd
import logging
import db_ops as db

# Configure the logger
logging.basicConfig(
    filename='data_processing.log', 
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def sanity_check():
    try:
        # Read sponsor data
        df_sponsor = pd.read_csv("/Users/apple/Downloads/sponsor_data (1).csv")
        rows_with_nulls = df_sponsor[df_sponsor.isnull().any(axis=1)]
        df_sponsor = df_sponsor.merge(rows_with_nulls, how='outer', indicator=True).query('_merge == "left_only"').drop('_merge', axis=1)
        df_sponsor = df_sponsor.drop_duplicates()
        logger.info("Processed Sponsor Data: Removed rows with nulls and duplicates")

        # Read user data
        df_user_data = pd.read_csv("/Users/apple/Downloads/users_data (1).csv")
        missing_value = df_user_data[df_user_data.isnull().any(axis=1)]
        df_user_data = df_user_data.merge(missing_value, how='outer', indicator=True).query('_merge=="left_only"').drop('_merge', axis=1)
        df_user_data = df_user_data.drop_duplicates()
        logger.info("Processed User Data: Removed rows with nulls and duplicates")

        # Read HR QnA data
        df_hr_qna = pd.read_csv("/Users/apple/Downloads/hra_qna_scores (1).csv")
        logger.info("Loaded HR QnA Data")

        # Read HR response data
        df_hr_responses = pd.read_csv("/Users/apple/Downloads/hra_responses (1).csv")
        missing_value = df_hr_responses[df_hr_responses.isnull().any(axis=1)]
        df_hr_responses = df_hr_responses.merge(missing_value, how='outer', indicator=True).query('_merge=="left_only"').drop('_merge', axis=1)
        df_hr_responses.drop_duplicates()
        logger.info("Processed HR Responses Data: Removed rows with nulls and duplicates")

        return df_sponsor, df_user_data, df_hr_qna, df_hr_responses

    except Exception as e:
        logger.error(f"Error in sanity_check: {e}")
        raise

def main():
    try:
        # Check for duplicates and missing values only
        df_sponsor, df_user_data, df_hr_qna, df_hr_responses = sanity_check()

        # Create table based on available data
        db.create_table()
        logger.info("Tables created successfully")

        # Load the data into the table
        db.load_data(df_sponsor, df_user_data, df_hr_qna, df_hr_responses)
        logger.info("Data loaded successfully into the tables")

        # Merge the tables to a new dataframe
        merged_df = db.merge_dataframe()
        logger.info("Data merged successfully into a new dataframe")

        # Store the merged dataframe to local for further analysis
        merged_df.to_csv("/Users/apple/Documents/Visit_health_assessment/merged_dataset.csv", index=False)
        logger.info("Merged dataset saved to CSV")

    except Exception as e:
        logger.error(f"Error in main: {e}")

if __name__ == "__main__":
    main()

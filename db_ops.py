import pandas as pd
import logging
from sqlalchemy import create_engine, text
import json
from main import *

# Configure the logger
logging.basicConfig(
    filename='database_operations.log', 
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Step 1: Read credentials from settings.json
with open('settings.json', 'r') as file:
    settings = json.load(file)

# Step 2: Extract the connection details from the JSON configuration
creds = settings["sqltools.connections"][0]
postgres_user = creds["username"]
postgres_password = ''  # Add password here if available in your config or leave empty
postgres_host = creds["server"]
postgres_port = creds["port"]
postgres_dbname = creds["database"]

# Step 3: Construct the database URL
db_url = f"postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_dbname}"

def create_table():
    engine = create_engine(db_url, echo=True)  # Enable echo for SQL logging

    sponsor_table_query = """
    CREATE TABLE IF NOT EXISTS public.df_sponsor(
    id INTEGER PRIMARY KEY,
    name VARCHAR(100)
    );
    """
    user_table_query = """
    CREATE TABLE IF NOT EXISTS public.df_users(
    id INTEGER PRIMARY KEY,
    name VARCHAR(100),
    sponsor_id INTEGER REFERENCES public.df_sponsor(id),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    question_table_query = """
    CREATE TABLE IF NOT EXISTS public.df_hr_qna(
    question_id INTEGER PRIMARY KEY,
    type VARCHAR(100),
    title VARCHAR(100),
    text VARCHAR(200),
    options VARCHAR(100),
    score REAL
    );
    """
    response_table_query = """
    CREATE TABLE IF NOT EXISTS public.df_hr_response(
    id INTEGER PRIMARY KEY,
    user_id INTEGER REFERENCES public.df_users(id),
    question_id INTEGER REFERENCES public.df_hr_qna(question_id),
    response VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """

    try:
        with engine.connect() as connection:
            logger.info("Database connection successful")

            # Execute table creation queries
            connection.execute(text(sponsor_table_query))
            logger.info("df_sponsor table created or already exists")
            
            connection.execute(text(user_table_query))
            logger.info("df_users table created or already exists")
            
            connection.execute(text(question_table_query))
            logger.info("df_hr_qna table created or already exists")
            
            connection.execute(text(response_table_query))
            logger.info("df_hr_response table created or already exists")
    
    except Exception as e:
        logger.error(f"Error creating tables: {e}")
    
    finally:
        logger.info("Closing database connection")
        connection.close()
        engine.dispose()

def clean_record(record):
    return {key: (None if pd.isna(value) else value) for key, value in record.items()}

def load_data(df_sponsor, df_user_data, df_hr_qna, df_hr_responses):
    engine = create_engine(db_url, echo=True)

    sponsor_insert_query = """
    INSERT INTO public.df_sponsor(id, name) VALUES (:id, :name) ON CONFLICT (id) DO NOTHING;
    """
    user_insert_query = """
    INSERT INTO public.df_users(id, name, sponsor_id, created_at) VALUES (:id, :name, :sponsor_id, :created_at) ON CONFLICT(id) DO NOTHING;
    """
    question_insert_query = """
    INSERT INTO public.df_hr_qna(question_id, type, title, text, options, score)
    VALUES (:question_id, :type, :title, :text, :options, :score)
    ON CONFLICT (question_id) DO NOTHING;
    """
    response_insert_query = """
    INSERT INTO public.df_hr_response(id, user_id, question_id, response, created_at) VALUES (:id, :user_id, :question_id, :response, :created_at) ON CONFLICT(id) DO NOTHING;
    """

    try:
        with engine.connect() as connection:
            logger.info("Database connection successful for data loading")

            for sponsor in df_sponsor.to_dict(orient='records'):
                connection.execute(text(sponsor_insert_query), sponsor)
            logger.info(f"Inserted {len(df_sponsor)} sponsor records")

            for user in df_user_data.to_dict(orient='records'):
                connection.execute(text(user_insert_query), user)
            logger.info(f"Inserted {len(df_user_data)} user records")

            for qna in df_hr_qna.to_dict(orient='records'):
                connection.execute(text(question_insert_query), clean_record(qna))
            logger.info(f"Inserted {len(df_hr_qna)} QnA records")

            for response in df_hr_responses.to_dict(orient='records'):
                connection.execute(text(response_insert_query), response)
            logger.info(f"Inserted {len(df_hr_responses)} response records")

    except Exception as e:
        logger.error(f"Error loading data: {e}")

    finally:
        logger.info("Closing database connection")
        connection.close()
        engine.dispose()

def merge_dataframe():
    engine = create_engine(db_url, echo=True)

    merge_query = """
    SELECT 
    resp.id AS response_id, resp.user_id, resp.question_id, resp.response, resp.created_at,
    qna.type, qna.title, qna.text, qna.options, qna.score,
    du.name AS user_name, du.sponsor_id,
    ds.name AS sponsor_name
    FROM df_sponsor ds
    JOIN df_users du ON ds.id = du.sponsor_id
    JOIN df_hr_response resp ON du.id = resp.user_id
    JOIN df_hr_qna qna ON resp.question_id = qna.question_id;
    """

    try:
        with engine.connect() as connection:
            logger.info("Database connection successful for merging data")
            df = pd.read_sql_query(text(merge_query), connection)
            logger.info("Data merged successfully into a new DataFrame")
            return df

    except Exception as e:
        logger.error(f"Error merging data: {e}")

    finally:
        logger.info("Closing database connection")
        connection.close()
        engine.dispose()

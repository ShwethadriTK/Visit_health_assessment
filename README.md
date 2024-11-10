# Visit_health_assessment
The project consists of ETL tasks performed on healthcare dataset provided by Visit Health. Also conducted analysis to identify trends and gain insights into the dataset.

## Data Flow / System  Diagram

The Below image consists of workflow diagram.

![Data Flow/Logical Diagram](WorkFlow_Log.jpg)

The document consists of a step by step process of ETL using Postgres.

1. Read the .csv files from local and check for
    * missing records
    * duplicates.
2. Once data is checked for its sanity, based on the available data develop an ER-Diagram as shown  
   below.  

![ER-Diagram](ER-diagram.jpg)

                    Fig. ER-Diagram representing relation between tables


# Imports
import os
import sys
import duckdb
import pandas as pd
from pathlib import Path

# Directories
database_directory = os.path.join("datas", "database")
source_directory = os.path.join('datas', 'source')

# Function to extract data
def extract_data(dataset_path):
    try:
        covid = pd.read_csv(dataset_path)
        return covid
    except FileNotFoundError:
        print('File Not Found')
    except pd.errors.EmptyDataError:
        print('Data Not Found')
    except pd.errors.ParserError:
        print('Parse Error')
    return None

# Function to transform data
def transform_data(df_list, statename):
    covid = pd.concat(df_list, ignore_index = True)

    if 'state' in covid.columns:
        covid = covid[covid['state'] == statename].copy()
    else:
        print(f'State Column Not Found in Database')
        return pd.DataFrame()
    
    if 'cases' not in covid.columns or 'deaths' not in covid.columns:
        print(f'Cases or Deaths Columsn Not Found in Database')
        return pd.DataFrame()
    
    covid = covid[['date', 'county', 'state', 'cases', 'deaths']]
    covid = covid[covid['cases'] > 1000]

    return covid

# Function to load data
def load_data(data, name, duckdb_con):
    name = name.replace(' ', '')

    # Connect Database and remove table if exist
    duckdb_con.execute(f'DROP TABLE IF EXISTS {name}')

    # Create table from DataFrame
    duckdb_con.from_df(data).create(name)

# Function to execute ETL Process
def execute_etl(statename):
    try:
        print(f'Running ETL To State ::: {statename}')

        # Extracting Data
        covid2020 = extract_data(os.path.join(source_directory, 'us-2020.csv'))
        covid2021 = extract_data(os.path.join(source_directory, 'us-2021.csv'))
        covid2022 = extract_data(os.path.join(source_directory, 'us-2022.csv'))
        covid2023 = extract_data(os.path.join(source_directory, 'us-2023.csv'))
        print(f'Extract Concluded')

        if any(df is None for df in [covid2020, covid2021, covid2022, covid2023]):
            print('Some dataset Was Not Found')
            return
        
        # Transforming Data
        covid = transform_data([covid2020, covid2021, covid2022, covid2023])
        print('Transform Concluded')

        if covid.empty:
            print(f'Dataframe is empty To {statename} state')
            return
        
        # Loading Dataset
        Path(database_directory).mkdir(parents= True, exist_ok= True)
        duckdb_file_path = os.path.join(database_directory, 'dbcovid.duckdb')
        duckdb_con = duckdb.connect(duckdb_file_path)

        load_data(covid, statename, duckdb_con)
        print('Load Data Concluded')

    except Exception as e:
        print(e, file = sys.stderr)

if __name__ == '__main__':
    print(f'\nRunning ETL Process...')

    state_names = [
        "Alaska", "Alabama", "Arkansas", "American Samoa", "Arizona", "California", 
        "Colorado", "Connecticut", "District of Columbia", "Delaware", "Florida", 
        "Georgia", "Guam", "Hawaii", "Iowa", "Idaho", "Illinois", "Indiana", "Kansas", 
        "Kentucky", "Louisiana", "Massachusetts", "Maryland", "Maine", "Michigan", 
        "Minnesota", "Missouri", "Mississippi", "Montana", "North Carolina", 
        "North Dakota", "Nebraska", "New Hampshire", "New Jersey", "New Mexico", 
        "Nevada", "New York", "Ohio", "Oklahoma", "Oregon", "Pennsylvania", 
        "Puerto Rico", "Rhode Island", "South Carolina", "South Dakota", "Tennessee", 
        "Texas", "Utah", "Virginia", "Virgin Islands", "Vermont", "Washington", 
        "Wisconsin", "West Virginia", "Wyoming"]
    
    for state in state_names:
        execute_etl(state)
    
    print(f'\nETL Process Concluded!\n')


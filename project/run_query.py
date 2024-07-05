# imports

import duckdb
import os
import pandas as pd

database_directory = os.path.join('datas', 'database')
duckdb_file_path = os.path.join(database_directory, 'dbcovid.duckdb')
output_directory = os.path.join(database_directory)

def generate_report(duckdb_con, statename):
    try:
        query = f"""SELECT
        strftime(CAST(date AS DATE), '%Y-%m') AS Month,
        ROUND(AVG(cases),2) AS Avg_cases,
        ROUNG(AVG(deaths),2) AS Avg_deaths
        FROM "{statename.replace(' ', '')}"
        GROUP BY Month
        ORDER BY Month"""

        result = duckdb_con.execute(query).fetchdf()

        csv_path = os.path.join(output_directory, f"{statename.replace(' ', '_')}_avg_cases_deaths.csv")
        result.to_csv(csv_path, index = False)

        return result
    
    except Exception as e:
        print(f'Error querying data for state {statename}: {e}')
        return pd.DataFrame()

duckdb_con = duckdb.connect(duckdb_file_path)

statenames = ['California', 'Florida']

for state in statenames:
    print(f'\n Average Cases and Deaths for Month (Since 2020 and 2023) for {state}:\n')
    result = generate_report(duckdb_con, state)
    print(result)
    print('\n' + '='*50 + '\n')
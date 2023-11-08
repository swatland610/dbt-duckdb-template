import os
import json
import requests 
import duckdb
import pandas as pd

### global variables
EIA_API_KEY = os.getenv('EIA_API_KEY')
METRIC_API_ROUTES = {
    'CO2_EMISSONS':'co2-emissions/co2-emissions-aggregates',
    'ELECTRIC_CAPACITY':'electricity/state-electricity-profiles/capability'
}

DUCK_DB_CONN = duckdb.connect('databases/raw.db')


def write_to_duck(df, metric_name, duck_conn=DUCK_DB_CONN):
    write_df = df
    duck_conn.sql('CREATE TABLE '+str(metric_name)+' AS SELECT * FROM write_df')
    print('{} table written!')



def extract_eia_metrics(metric_api_route, year_start, year_end):
    parent_data = pd.DataFrame()

    # init empy df
    for year in range(year_start, year_end+1): 
        print("extracting for year {} ...".format(year))
        year_data = _extract_eia_by_year(metric_api_route, year)
        print("year data extracted!")
        parent_data = pd.concat([parent_data, year_data])

    return parent_data

def _extract_eia_by_year(metric_api_route, year):
    response = requests.get("https://api.eia.gov/v2/"+metric_api_route+"/data/?api_key="+EIA_API_KEY+"&frequency=annual&data[0]=value&start="+str(year)+"&end="+str(year)+"&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000")
    print(response)
    results = json.loads(response.text)
    return pd.DataFrame(results['response']['data'])

if __name__ == '__main__':
    df = extract_eia_metrics(metric_api_route='co2-emissions/co2-emissions-aggregates', year_start=2010, year_end=2021)
    #write_to_duck(df=df, metric_name='CO2_EMISSIONS')
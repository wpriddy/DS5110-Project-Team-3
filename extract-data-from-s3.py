#%%

#! pip install requests
#! pip install pandas 

from io import BytesIO
import zipfile
import requests as r 
import pandas as pd

fips_map = pd.read_pickle('fips_map.pickle')

# Get Circuit Data Codes
circuit_data_links = {2000: 'N586CY',
                        2001: 'WFZX55',
                        2002: 'GXYOHX',
                        2003: 'SV3EH7',
                        2004: 'FLRMN6',
                        2005: '6CJB9U',
                        2006: 'VAVRRQ',
                        2007: 'T6SING',
                        2008: 'OMHOZT',
                        2009: 'ZZNGZS',
                        2010: 'LK0BMA',
                        2011: 'P928SU',
                        2012: 'Y6DBO7',
                        2013: '1FOA9N',
                        2014: 'P2L0Y4',
                        2015: '1WMI7Q',
                        2016: 'HMVIZY',
                        2017: 'B9BUKQ',
                        2018: '1Y613I',
                        2019: '8JMX4R',
                        2020: 'SQYRXB'}

# Get District Data Codes
district_data_links = {2010: '8XEDGA',
                       2011: '1JEKQB',
                       2012: 'F6G5KD',
                       2013: '5IBSOL',
                       2014: '5IDVZ7',
                       2015: 'WZ57WS',
                       2016: 'C4703U',
                       2017: 'QCO2KE',
                       2018: 'S72VOG',
                       2019: 'I24Z9B',
                       2020: 'PFP3JS'}

# Circuit Base URL
circuit_base_url = "https://s3.amazonaws.com/virginia-court-data/circuit_criminal_{}_anon_{}.zip"

# District Base URL
district_base_url = "https://s3.amazonaws.com/virginia-court-data/district_criminal_{}_anon_{}.zip"


def extract_data(code_dict: dict, base_url: str):
    """Extract the Data for each year from the S3 Bucket
       Concatenate values into single dictionary and return dataframe

    params: 
        code_dict: dictionary of year and unique code for the base URL to feed off of
        base_url: base S3 url for formatting

    return pd.DataFrame

    """
    
    # Temp Storage
    data_dict = {}

    # Iterate through each years data
    for year, code in code_dict.items():

        # Fetch Response
        temp_response = r.get(base_url.format(year, code))

        # Decompress and Look at Zip
        temp_zip_file = zipfile.ZipFile(BytesIO(temp_response.content))

        # Read as CSV and Store to Dictionary
        for name in temp_zip_file.namelist():
            data_dict[name] = pd.read_csv(temp_zip_file.open(name))

    return pd.concat(data_dict.values()).drop_duplicates()

# Pull Circuit Data
# TAKES ABOUT 2.5 MINUTES TO RUN CODE BELOW
circuit_data = extract_data(circuit_data_links, circuit_base_url)

# Add Locality
circuit_data['locality'] = circuit_data['locality'] = circuit_data['fips'].apply(lambda x: int('51' + str(x).zfill(3))).map(fips_map)

# Pull District Data
# TAKES ABOUT 15 MINUTES TO RUN CODE BELOW
district_data = extract_data(district_data_links, district_base_url)

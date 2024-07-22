import os
import pandas as pd

'''
Check: https://download.bls.gov/pub/time.series/jt/jt.txt
'''

# Get the list of files in the directory
file_list = os.listdir('./data/source/job_openings')

# Extract the unique suffixes from the file names
list_of_years = set([file.split('_')[-1].split('.')[0] for file in file_list])

for year in list_of_years:

    # Read the data from the file
    file_path = f'./data/source/job_openings/job_openings_{year}.csv'
    data = pd.read_csv(file_path)

    # Extract relevant information from the 'series_id' column
    data.rename(columns={ 'series_id                     ':'series_id'}, inplace=True)

    # Extract relevant information from the 'series_id' column
    data['seasonal'] = data['series_id'].str[2]
    data['industry_code'] = data['series_id'].str[3:9]
    data['state_code'] = data['series_id'].str[9:11]
    data['sizeclass_code'] = data['series_id'].str[16:18]
    data['dataelement_code'] = data['series_id'].str[18:20]
    data['ratelevel_code'] = data['series_id'].str[20]

    # Drop the 'series_id' column
    data = data.drop(columns=['series_id'])

    # Drop the 'footnote_codes' column
    data = data.drop(columns=['footnote_codes'])

    # Drop yearly observations
    data = data[data['period'] != 'M13']

    # Drop the 'M' character before month values and convert to integers
    data['period'] = data['period'].str.replace('M', '').astype(int)

    # Rename the 'period' column to 'month'
    data.rename(columns={'period':'month'}, inplace=True)

    # Select seasonally adjusted data
    data = data[data['seasonal'] == 'S']
    data = data.drop(columns=['seasonal'])

    # Take the total nonfarm data (but it can be interesting to see the differences)
    data = data[data['industry_code'] == '000000']
    data = data.drop(columns=['industry_code'])

    # Drop federal and four regions data
    data = data[~data['state_code'].isin(['00', 'MW', 'NE', 'SO', 'WE'])]

    # All size classes (but it can be interesting to see the differences)
    data = data[data['sizeclass_code'] == '00']
    data = data.drop(columns=['sizeclass_code'])


    # Define a dictionary to store the var_code and name combinations
    variables_levels = {
        'HI': 'hires',
        'JO': 'job_openings',
        'LD': 'layoffs',
        'OS': 'other_separations',
        'QU': 'quits',
        'TS': 'total_separations',
        'UO': 'unemployed_per_job_opening_ratio'
    }

    # Initialize an empty dictionary to store the filtered dataframes
    level_data = {}

    # Iterate over the dataelement_ratelevel dictionary
    for var_code, name in variables_levels.items():
        # Filter the data based on var_code and name
        level_split = data[(data['dataelement_code'] == var_code) & (data['ratelevel_code'] == 'L')].reset_index(drop=True)
        level_split = level_split.drop(columns=['dataelement_code', 'ratelevel_code'])
        level_data[f'{name}'] = level_split

    variables_levels_rates = {}
    for key, value in variables_levels.items():
        variables_levels_rates[key] = value + '_rate'

    # Initialize an empty dictionary to store the filtered dataframes
    rates_data = {}

    # Iterate over the dataelement_ratelevel dictionary
    for var_code, name in variables_levels_rates.items():
        # Filter the data based on var_code and name
        rate_split = data[(data['dataelement_code'] == var_code) & (data['ratelevel_code'] == 'R')].reset_index(drop=True)
        rate_split = rate_split.drop(columns=['dataelement_code', 'ratelevel_code'])
        rates_data[f'{name}'] = rate_split

    # Save the dataframes to CSV files
    for name in level_data.keys():
        if not level_data[name].empty:
            level_data[name].to_csv(f'./data/intermediate/job_openings/{name}_{year}.csv', index=False)

    for name in rates_data.keys():
        if not rates_data[name].empty:
            rates_data[name].to_csv(f'./data/intermediate/job_openings/{name}_{year}.csv', index=False)




import os
import pandas as pd

# Get the list of files in the directory
file_list = os.listdir('./data/source/labor_market_outcomes')

# Extract the unique suffixes from the file names
list_of_years = set([file.split('_')[-1].split('.')[0] for file in file_list])

# Load the identifiers
dfips = pd.read_csv(r'./data/source/fips/county_fips_master.csv',encoding = "ISO-8859-1")

# Select the desired columns from the 'dfips' DataFrame
dfips = dfips[['fips','county_name','state_abbr','state_name']]

# Convert the 'fips' column to string and pad with zeros to make it 5 characters long
dfips['fips'] = dfips['fips'].astype(str).str.rjust(5,'0')

for time in list_of_years:
    # Read the data 
    df = pd.read_csv(f'./data/source/labor_market_outcomes/lmos_data_{time}.csv')

    # Drop the 'footnote_codes' column from the DataFrame
    df = df.drop('footnote_codes', axis=1)

    # Rename the columns of the DataFrame
    df = df.rename(
        columns = {
            df.columns.values.tolist()[0]: 'series_id',
            df.columns.values.tolist()[1]: 'year',
            df.columns.values.tolist()[2]: 'period',
            df.columns.values.tolist()[3]: 'value'
        }
    )
    
    # Extract the state and county FIPS codes from the 'series_id' column
    df['state_fip'] = df['series_id'].str.slice(5, 7)
    df['county_fip'] = df['series_id'].str.slice(7, 10)

    # Create a new column 'fips' by concatenating the state and county FIPS codes
    df["fips"] = df["state_fip"] + df["county_fip"]

    # Extract the series code, which represents the type of labor market outcome
    df['series'] = df['series_id'].str.slice(18, 20)

    # Extract the month from the 'period' column
    df['month'] = df['period'].str.slice(1, 3)

    '''
    (meaning of the codes)

    measure_code	measure_text

    03	unemployment rate

    04	unemployment

    05	employment

    06	labor force

    07	employment-population ratio (empty)

    08	labor force participation rate (empty)

    09	civilian noninstitutional population

    https://users.nber.org/~notom/for_tal/LAUS/readme.txt 

    MXX=Monthly, M13=Annual

    '''

    # Filter the 'df' DataFrame to include only rows with series code '03' (unemployment rate) and exclude rows with period 'M13' (annual)
    df_unemp_rate = df.drop(df[(df.series != '03') | (df.period == 'M13')].index)

    # Filter the 'df' DataFrame to include only rows with series code '04' (unemployment) and exclude rows with period 'M13' (annual)
    df_unemp = df.drop(df[(df.series != '04') | (df.period == 'M13')].index)

    # Filter the 'df' DataFrame to include only rows with series code '05' (employment) and exclude rows with period 'M13' (annual)
    df_emp = df.drop(df[(df.series != '05') | (df.period == 'M13')].index)

    # Filter the 'df' DataFrame to include only rows with series code '06' (labor force) and exclude rows with period 'M13' (annual)
    df_lab_force = df.drop(df[(df.series != '06') | (df.period == 'M13')].index)

    # Create a dictionary to store the DataFrames for different labor market outcomes
    df_dict = {
        'unemployment_rate': df_unemp_rate,
        'unemployment': df_unemp,
        'employment': df_emp,
        'labor_force': df_lab_force
    }

    # Iterate over the dictionary items
    for k, i_df in df_dict.items():
        # Drop unnecessary columns from each DataFrame
        i_df = i_df.drop(['series_id', 'series', 'period', 'state_fip', 'county_fip'], axis=1)
        
        # Merge each DataFrame with the 'dfips' DataFrame based on the 'fips' column
        i_df_final = i_df.merge(dfips, on='fips')
        
        # Save the final DataFrame as a CSV file
        i_df_final.to_csv(f'./data/intermediate/labor_market_outcomes/{k}_{time}.csv', sep=',', index=False)

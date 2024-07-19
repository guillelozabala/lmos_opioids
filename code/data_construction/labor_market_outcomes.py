import os
import pandas as pd

# Get the list of files in the directory
file_list = os.listdir('./data/source/labor_market_outcomes')
file_list_states = os.listdir('./data/source/labor_market_outcomes_states')

# Extract the unique suffixes from the file names
list_of_years = set([file.split('_')[-1].split('.')[0] for file in file_list])
list_of_years_states = set([file.split('_')[-1].split('.')[0] for file in file_list_states])

# Load the identifiers
dfips = pd.read_csv(r'./data/source/fips/county_fips_master.csv',encoding = "ISO-8859-1")

# Select the desired columns from the 'dfips' DataFrame
dfips_ct = dfips[['fips','county_name','state_abbr','state_name']]
dfips_st = dfips[['state','state_name','state_abbr']]

# Convert the 'fips' column to string and pad with zeros to make it 5 characters long
dfips_ct['fips'] = dfips_ct['fips'].astype(str).str.rjust(5,'0')
dfips_st['state'] = dfips_st['state'].astype('Int64').astype(str).str.rjust(2,'0')

# Rename the columns of the 'dfips' DataFrame
dfips_st = dfips_st.rename(columns={'state': 'state_fip'})

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
        
        # Merge each DataFrame with the 'dfips_ct' DataFrame based on the 'fips' column
        i_df_final = i_df.merge(dfips_ct, on='fips')
        
        # Save the final DataFrame as a CSV file
        i_df_final.to_csv(f'./data/intermediate/labor_market_outcomes/{k}_{time}.csv', sep=',', index=False)


for time in list_of_years_states:

    # Read the data 
    df_st = pd.read_csv(f'./data/source/labor_market_outcomes_states/lmos_data_{time}.csv')

    # Drop the 'footnote_codes' column from the DataFrame
    df_st = df_st.drop('footnote_codes', axis=1)

    # Rename the columns of the DataFrame
    df_st = df_st.rename(
        columns = {
            df_st.columns.values.tolist()[0]: 'series_id',
            df_st.columns.values.tolist()[1]: 'year',
            df_st.columns.values.tolist()[2]: 'period',
            df_st.columns.values.tolist()[3]: 'value'
        }
    )

    # Extract the state and county FIPS codes from the 'series_id' column
    df_st['state_fip'] = df_st['series_id'].str.slice(5, 7)

    # Extract the series code, which represents the type of labor market outcome
    df_st['series'] = df_st['series_id'].str.slice(18, 20)

    # Extract the month from the 'period' column
    df_st['month'] = df_st['period'].str.slice(1, 3)

    # Drop Puerto Rico and Census Regions and Divisions
    df_st = df_st[~df_st['state_fip'].isin(['72', '80'])]

    # Filter the 'df_st' DataFrame to include only rows with series code '03' (unemployment rate) and exclude rows with period 'M13' (annual)
    df_st_unemp_rate = df_st.drop(df_st[(df_st.series != '03') | (df_st.period == 'M13')].index)

    # Filter the 'df_st' DataFrame to include only rows with series code '04' (unemployment) and exclude rows with period 'M13' (annual)
    df_st_unemp = df_st.drop(df_st[(df_st.series != '04') | (df_st.period == 'M13')].index)

    # Filter the 'df_st' DataFrame to include only rows with series code '05' (employment) and exclude rows with period 'M13' (annual)
    df_st_emp = df_st.drop(df_st[(df_st.series != '05') | (df_st.period == 'M13')].index)

    # Filter the 'df_st' DataFrame to include only rows with series code '06' (labor force) and exclude rows with period 'M13' (annual)
    df_st_lab_force = df_st.drop(df_st[(df_st.series != '06') | (df_st.period == 'M13')].index)

    # Create a dictionary to store the DataFrames for different labor market outcomes
    df_st_dict = {
        'unemployment_rate': df_st_unemp_rate,
        'unemployment': df_st_unemp,
        'employment': df_st_emp,
        'labor_force': df_st_lab_force
    }

    # Iterate over the dictionary items
    for k, i_df_st in df_st_dict.items():
        # Drop unnecessary columns from each DataFrame
        i_df_st = i_df_st.drop(['series_id', 'series', 'period'], axis=1)
        
        # Merge each DataFrame with the 'dfips_st' DataFrame based on the 'state_fip' column
        i_df_st_final = i_df_st.merge(dfips_st, on='state_fip')
        
        # Save the final DataFrame as a CSV file
        i_df_st_final.to_csv(f'./data/intermediate/labor_market_outcomes_states/{k}_{time}.csv', sep=',', index=False)
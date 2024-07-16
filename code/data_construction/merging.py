import os
import pandas as pd
import re

# Extract the unique preffixes from the file names (lower bar followed by number)
pattern = r'^(.*?)(?=_\d)'

# Define the initial and last years
initial_year = 2003
last_year = 2019

# Create a range of years from initial_year to last_year
# year_range = range(initial_year, last_year + 1)

# Get the list of files in the directory
file_list = os.listdir('./data/intermediate/labor_market_outcomes')

variables = set([re.search(pattern, file).group() for file in file_list])

labor_market_outcomes_years = {var: {} for var in variables}
for var in variables:
    pattern_find = rf"{var}_\d+"
    files_to_open = [re.search(pattern_find, file).group() for file in file_list if re.search(pattern_find, file)]
    for file in files_to_open:
        labor_market_outcomes_years[var][file] = pd.read_csv(f'./data/intermediate/labor_market_outcomes/{file}.csv')


labor_market_outcomes = {var: {} for var in variables}
for var in variables:
    df_names = []
    lmos_keys = labor_market_outcomes_years[var].keys()
    for var_keys in lmos_keys:
        temp_df = labor_market_outcomes_years[var][var_keys]
        df_names.append(temp_df)
    labor_market_outcomes[var] = pd.concat(df_names)




pattern_find = rf"{list(variables)[3]}_\d+"

[re.search(pattern_find, file).group() for file in file_list if re.search(pattern_find, file)]

column_names = ['fips', 'year', 'month', 'county_name', 'state_abbr', 'state_name']

# Create an empty dataframe to store the merged data
df_lmos = pd.DataFrame(columns = column_names)
for year in year_range:
    # Filter the files that contain the year
    filtered_files = [file for file in file_list if str(year) in file]
    df_dict = {}
    for file in filtered_files:
        # Read the file
        df = pd.read_csv(f'./data/intermediate/labor_market_outcomes/{file}')
        # Extract the variable from the file name
        variable = re.search(pattern, file).group()
        # Rename the 'value' column to the variable name
        df.rename(columns={'value': variable}, inplace=True)
        # Merge the data with the previous data
        df_dict[variable] = df
    df_lmos = pd.concat([df_lmos, df_merged], ignore_index=True)

df_lmos



filtered_files = [file for file in file_list if '2007' in file]
df1 = pd.read_csv(f'./data/intermediate/labor_market_outcomes/{filtered_files[2]}')
df2 = pd.read_csv(f'./data/intermediate/labor_market_outcomes/{filtered_files[1]}')
# Extract the variable from the file name
variable1 = re.search(pattern, filtered_files[2]).group()
variable2 = re.search(pattern, filtered_files[1]).group()
# Rename the 'value' column to the variable name
df1.rename(columns={'value': variable1}, inplace=True)
df2.rename(columns={'value': variable2}, inplace=True)
pd.merge(df1, df2, on = column_names)

df0 = pd.DataFrame(columns = column_names)
pd.merge(df1, df0, on = column_names)

df1.join(df0, on = column_names)

# Apply the pattern to each file in the filtered_files list
matched_strings = [re.search(pattern, file).group() if re.search(pattern, file) else '' for file in filtered_files]




merged_df = pd.DataFrame(columns=column_names)
for year in year_range:
    # Filter the files that contain the year
    filtered_files = [file for file in file_list if str(year) in file]
    for file in filtered_files:
        # Read the file
        df = pd.read_csv(f'./data/intermediate/labor_market_outcomes/{file}')
        # Extract the variable from the file name
        variable = re.search(pattern, file).group()
        # Rename the 'value' column to the variable name
        df.rename(columns={'value': variable}, inplace=True)
        # Merge the data with the previous data
        merged_df = pd.merge(merged_df, df, on=column_names)
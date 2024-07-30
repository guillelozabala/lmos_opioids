import os
import pandas as pd
import numpy as np
import re

# Define the initial and last years
initial_year = 2003
last_year = 2019

# Create a range of years from initial_year to last_year
year_range = range(initial_year, last_year + 1)

### Load the county identifiers ####################################################################

fips_details = pd.read_csv('./data/source/fips/county_fips_master.csv', encoding='ISO-8859-1')
unique_fips = sorted(fips_details['fips'].unique().tolist())

### Load the labor market outcomes data ############################################################

# Get the list of files in the directory
file_list_lmos = os.listdir('./data/intermediate/labor_market_outcomes')

# Extract the unique preffixes from the file names (lower bar followed by number)
pattern = r'^(.*?)(?=_\d)'

# Extract unique prefixes from the file names (lower bar followed by number)
variables_lmos = set([re.search(pattern, file).group() for file in file_list_lmos])

# Create a dictionary to store labor market outcomes data for each variable
labor_market_outcomes_years = {var: {} for var in variables_lmos}

# Iterate over each variable
for var in variables_lmos:
    # Define the pattern to find files related to the variable
    pattern_find = rf"{var}_\d+"
    # Find the files that match the pattern
    files_to_open = [re.search(pattern_find, file).group() for file in file_list_lmos if re.search(pattern_find, file)]
    # Iterate over each file
    for file in files_to_open:
        # Read the file and store the data in the dictionary
        labor_market_outcomes_years[var][file] = pd.read_csv(f'./data/intermediate/labor_market_outcomes/{file}.csv')

# Create a dictionary to store concatenated labor market outcomes data for each variable
labor_market_outcomes = {var: {} for var in variables_lmos}

# Iterate over each variable
for var in variables_lmos:
    # Create a list to store the dataframes for each file
    df_names = []
    # Get the keys (file names) for the variable
    lmos_keys = labor_market_outcomes_years[var].keys()
    # Iterate over each key
    for var_keys in lmos_keys:
        # Get the dataframe for the key
        temp_df = labor_market_outcomes_years[var][var_keys]
        # Rename the 'value' column to the variable name
        temp_df.rename(columns={'value': var}, inplace=True)
        # Append the dataframe to the list
        df_names.append(temp_df)
    # Concatenate the dataframes and store the result in the dictionary
    labor_market_outcomes[var] = pd.concat(df_names)

# Identify common columns
common_columns = set.intersection(*(set(df.columns) for df in labor_market_outcomes.values()))

# Convert set of common columns to a list
common_columns = list(common_columns)

# Merge all dataframes in the dictionary based on the common columns
merged_lmos = list(labor_market_outcomes.values())[0]
for df in list(labor_market_outcomes.values())[1:]:
    merged_lmos = pd.merge(merged_lmos, df, on=common_columns, how='outer')

# Select the rows with years between initial_year and last_year
merged_lmos = merged_lmos[(merged_lmos['year'] >= initial_year) & (merged_lmos['year'] <= last_year)]
merged_lmos[list(variables_lmos)] = merged_lmos[list(variables_lmos)].apply(pd.to_numeric, errors='coerce').astype(float)

### Load the county demographics data ###############################################################

# Load the county demographics data for each year and concatenate them into a single dataframe
demo_df = []
for year in year_range:
    demo_data = pd.read_csv(f'./data/intermediate/county_demographics/county_demographics_{year}.csv')
    demo_df.append(demo_data)
demographics = pd.concat(demo_df)

# Define the list of working ages
working_ages = []
for age_bin in range(5, 14):
    working_cohort = "age" + str(age_bin) + "_population_ratio"
    working_ages.append(working_cohort)

# Calculate the weighted working age population
demographics["working_age_pop_weight"] = demographics[working_ages].sum(axis=1, skipna=True)

# Calculate the working age population
demographics["working_age_pop"] = demographics["working_age_pop_weight"] * demographics["population"]

### Load the minimum wage data ######################################################################

# Load the minimum wage data
minwage = pd.read_csv(f'./data/intermediate/minimum_wage/minwage_clean_states.csv')

# Rename the columns to match the desired names
minwage = minwage.rename(columns={
    'State or otherjurisdiction': 'state_name',
    'Year': 'year',
    'Value': 'min_wage'
})

# Filter out the rows where the state is 'Federal (FLSA)'
minwage = minwage[minwage['state_name'] != 'Federal (FLSA)']

# Filter out the rows where the year is not within the specified range
minwage = minwage[(minwage['year'] >= initial_year) & (minwage['year'] <= last_year)]

### Load the PDMPs data ##############################################################################

pdmps = pd.read_csv(f'./data/source/pdmps/horwitz2020.csv')

# Select columns
pdmps_columns = ["state", "Prescriber must-query"]
pdmps = pdmps[pdmps_columns]

# Separate columns using delimiter "-"
pdmps[['pmq_month', 'pmq_year']] = pdmps['Prescriber must-query'].str.split('-', expand=True)
pdmps = pdmps.drop('Prescriber must-query', axis=1)

# Convert all columns except the first one to numeric (integer) values
cols_to_convert = pdmps.columns.difference(['state'])
pdmps[cols_to_convert] = pdmps[cols_to_convert].apply(pd.to_numeric).astype('Int64')

# Add time markers
pdmps['first_treatment_pmq'] = (pdmps['pmq_year'] - 1960) * 12 + pdmps['pmq_month']

# Rename state column
pdmps.rename(columns={'state': 'state_name'}, inplace=True)

### Load the sector composition data #################################################################

# Obtain the sector composition data for each year
sectors_df = []
for year in year_range:
    sector_data = pd.read_csv(f'./data/intermediate/sector_composition/sector_comp_{year}.csv')
    sector_data['year'] = year
    sectors_df.append(sector_data)
sector_shares = pd.concat(sectors_df)
# Drop the 'emp_ratio' column from the 'sector_shares' dataframe
sector_shares = sector_shares.drop('emp_ratio', axis=1)

# Convert the 'fips' column to a string and pad it with leading zeros to make it 5 digits long
sector_shares['fips'] = sector_shares['fips'].astype(str).str.rjust(5, '0')

# Extract the first two digits of the 'fips' column and create a new column 'state_fip'
sector_shares['state_fip'] = sector_shares['fips'].astype(str).str[:2]

# Group the data by 'naics', 'year', and 'fips' and sum the 'emp' columns
sector_shares = sector_shares.groupby(['naics', 'year', 'fips'])['emp'].sum().reset_index()

# Create a copy of the sector_shares dataframe (covariaes)
sector_shares_cov = sector_shares.copy()

# Extract the first two digits of the 'naics' column and create a new column 'naics_2digits'
sector_shares_cov['naics_2digits'] = sector_shares_cov['naics'].astype(str).str[:2]

# Group the data by 'naics_2digits', 'year', and 'fips' and sum the 'emp' and 'emp_ratio' columns
sector_shares_cov = sector_shares_cov.groupby(['naics_2digits', 'year', 'fips'])['emp'].sum().reset_index()

# Group the data by 'year' and 'fips' and sum the 'emp' column to get the total employment for each state
state_employment = sector_shares.groupby(['year', 'fips'])['emp'].sum().reset_index()

# Merge the sector_shares_cov dataframe with the state_employment dataframe on 'year' and 'fips'
sector_shares_cov = pd.merge(sector_shares_cov, state_employment, on=['year', 'fips'], suffixes=('', '_total'))

# Calculate the employment ratio by dividing the 'emp' column by the 'emp_total' column
sector_shares_cov['emp_ratio'] = (sector_shares_cov['emp'] / sector_shares_cov['emp_total']).round(4)

# Drop the 'emp' and 'emp_total' columns from the sector_shares_cov dataframe
sector_shares_cov = sector_shares_cov.drop(['emp', 'emp_total'], axis=1)

# Pivot the sector_shares_cov dataframe to have 'naics_2digits' as columns
sector_shares_cov = sector_shares_cov.pivot_table(index=['year', 'fips'], columns='naics_2digits')

# Drop the top level of the column index
sector_shares_cov.columns = sector_shares_cov.columns.droplevel().rename(None)

# Reset the index of the dataframe
sector_shares_cov = sector_shares_cov.reset_index()

# Rename the columns with a prefix 'emp_' and a suffix '_ratio'
sector_shares_cov.columns = 'emp_' + sector_shares_cov.columns + '_ratio'

# Rename the columns 'emp_year_ratio' and 'emp_fips_ratio' to 'year' and 'fips' respectively
sector_shares_cov = sector_shares_cov.rename({'emp_year_ratio': 'year', 'emp_fips_ratio': 'fips'}, axis=1)

# Extract the first three digits of the 'naics' column and create a new column 'naics_3digits'
sector_shares['naics_3digits'] = sector_shares['naics'].astype(str).str[:3] + '000'

# Group the data by 'naics_3digits', 'year', and 'fips' and sum the 'emp' column
sector_shares = sector_shares.groupby(['naics_3digits', 'year', 'fips'])[['emp']].sum().reset_index()

# Merge the sector_shares dataframe with the state_employment dataframe on 'year' and 'fips'
sector_shares = pd.merge(sector_shares, state_employment, on=['year', 'fips'], suffixes=('', '_total'))

# Calculate the employment ratio by dividing the 'emp' column by the 'emp_total' column
sector_shares['emp_ratio'] = (sector_shares['emp'] / sector_shares['emp_total']).round(4)

# Drop the 'emp_total' column from the sector_shares dataframe
sector_shares = sector_shares.drop(['emp_total'], axis=1)

# Rename the 'naics_3digits' column to 'naics'
sector_shares.rename(columns={'naics_3digits': 'naics'}, inplace=True)

### Load the industry wage distribution data ##########################################################

wage_dist_df = []
for year in year_range:
    # Load the industry wage distribution data for each year
    dist_data = pd.read_csv(f'./data/intermediate/industry_wage_distribution_three_codes/industry_wages{year}.csv')
    # Add the 'year' column to the dataframe
    dist_data['year'] = year
    # Filter the data to keep only the rows with the industry totals
    dist_data = dist_data[dist_data['occ_code'] == '00-0000']
    dist_data = dist_data.drop('occ_code', axis=1)
    # Rename the 'h_median' column to 'h_pct50'
    dist_data = dist_data.rename(columns={'h_median':'h_pct50'})
    # Append the dataframe to the list
    wage_dist_df.append(dist_data)
# Concatenate the dataframes in the list
industry_wage_dist = pd.concat(wage_dist_df)

# Make sure the 'naics' column is a string and pad it with leading zeros to make it 6 digits long
industry_wage_dist['naics'] = industry_wage_dist['naics'].astype(str).str.rjust(6, '0')

# Merge the industry wage distribution data with the sector shares data
county_wage_dist = pd.merge(industry_wage_dist, sector_shares, on=['naics', 'year'], how='outer')

# Transform the 'fips' column to an integer
county_wage_dist['fips'] = county_wage_dist['fips'].astype("Int64")

wage_distributions = []
for year in year_range:
    for fips in unique_fips:
        # Split the data by 'fips' and 'year'
        split_3naics = county_wage_dist[(county_wage_dist['fips'] == fips) & (county_wage_dist['year'] == year)]
        # Filter out the rows where 'emp' is equal to zero
        split_3naics = split_3naics[split_3naics['emp'] != 0]
        # Calculate the percentiles for the wage distribution weighted by employment
        if not split_3naics.empty:
            row = {
                'year': year,
                'fips': fips,
                'h_pct10':  np.nanpercentile(split_3naics['h_pct10'], 10, method='inverted_cdf',  weights=split_3naics['emp']),
                'h_pct25':  np.nanpercentile(split_3naics['h_pct25'], 25, method='inverted_cdf',  weights=split_3naics['emp']),
                'h_pct50':  np.nanpercentile(split_3naics['h_pct50'], 50, method='inverted_cdf',  weights=split_3naics['emp']),
                'h_pct75':  np.nanpercentile(split_3naics['h_pct75'], 75, method='inverted_cdf',  weights=split_3naics['emp']),
                'h_pct90':  np.nanpercentile(split_3naics['h_pct90'], 90, method='inverted_cdf',  weights=split_3naics['emp'])
            }
            # Append the row to the list
            wage_distributions.append(row)

# Create a dataframe from the list
fips_wage_distributions = pd.DataFrame(wage_distributions)

# Obtain the percentile columns
h_columns = [col for col in industry_wage_dist.columns if col.startswith('h_')]

# Convert the columns to numeric
fips_wage_distributions[h_columns] = fips_wage_distributions[h_columns].apply(pd.to_numeric, errors='coerce').astype(float)

### Load the overdose deaths data ####################################################################
od_deaths_total = pd.read_csv(f'./data/source/overdose_deaths_total/NCHS_Drug_Poisoning_Mortality_by_County_United_States.csv')

od_deaths_total = od_deaths_total[['FIPS', 'Year', 'Model-based Death Rate', 'Lower Confidence Limit', 'Upper Confidence Limit', 'Urban/Rural Category']]

od_deaths_total = od_deaths_total.rename(columns={
    'FIPS': 'fips',
    'Year': 'year',
    'Model-based Death Rate': 'model_death_rate',
    'Lower Confidence Limit': 'lbound_death_rate',
    'Upper Confidence Limit': 'ubound_death_rate',
    'Urban/Rural Category': 'urban_rural'
    })

print(od_deaths_total)

### Load the prescriptions data ######################################################################



# Merge the dataframes

# Merge the labor market outcomes data with the county demographics data
merged_data = pd.merge(merged_lmos, demographics, on=['fips', 'year'], how='inner')

# Obtain labor force part ratio
merged_data['lab_force_rate'] = (merged_data['labor_force'] / merged_data['working_age_pop'] * 100).round(4)

# Merge with the minimum wage data
merged_data = pd.merge(merged_data, minwage, on=['state_name', 'year'], how='inner')

# Merge with the PDMPs data
merged_data = pd.merge(merged_data, pdmps, on=['state_name'], how='inner')

# Merge with the sector composition data
sector_shares_cov['fips'] = sector_shares_cov['fips'].astype("Int64")
merged_data = pd.merge(merged_data, sector_shares_cov, on=['fips', 'year'], how='inner')

# Merge with the wage distribution data
merged_data = pd.merge(merged_data, fips_wage_distributions, on=['fips', 'year'], how='inner')

# Merge with the overdose deaths data
merged_data = pd.merge(merged_data, od_deaths_total, on=['fips', 'year'], how='inner')

# Obtain the log values
merged_data['log_minw'] = np.log(merged_data['min_wage'])
merged_data[['log_h_pct10', 'log_h_pct25', 'log_h_pct50', 'log_h_pct75', 'log_h_pct90']] = np.log(merged_data[['h_pct10', 'h_pct25', 'h_pct50', 'h_pct75', 'h_pct90']])

# Calculate the Kaitz index for different percentiles
merged_data['kaitz_pct10'] = merged_data['log_minw'] - merged_data['log_h_pct10']
merged_data['kaitz_pct25'] = merged_data['log_minw'] - merged_data['log_h_pct25']
merged_data['kaitz_pct50'] = merged_data['log_minw'] - merged_data['log_h_pct50']
merged_data['kaitz_pct75'] = merged_data['log_minw'] - merged_data['log_h_pct75']
merged_data['kaitz_pct90'] = merged_data['log_minw'] - merged_data['log_h_pct90']

# Save the merged data to a CSV file
merged_data.to_csv('./data/processed/merged_data.csv', index=False)


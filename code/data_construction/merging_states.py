import os
import pandas as pd
import numpy as np
import re

# Define the initial and last years
initial_year = 2003
prescriptions_initial_year = 2006
last_year = 2019

# Create a range of years from initial_year to last_year
year_range = range(initial_year, last_year + 1)
year_range_prescriptions = range(prescriptions_initial_year, last_year + 1)

### Load the county identifiers ####################################################################

fips_details = pd.read_csv('./data/source/fips/county_fips_master.csv', encoding='ISO-8859-1')
fips_details['state'] = fips_details['state'].astype('Int64').astype(str).str.rjust(2,'0')
fips_details = fips_details.rename(columns={'state': 'state_fip'})
unique_fips = sorted(fips_details['state_fip'].unique().tolist())

### Load the labor market outcomes data ############################################################

# Get the list of files in the directory
file_list_lmos = os.listdir('./data/intermediate/labor_market_outcomes_states')

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
        labor_market_outcomes_years[var][file] = pd.read_csv(f'./data/intermediate/labor_market_outcomes_states/{file}.csv')

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

print(merged_lmos)

### Load the county demographics data ###############################################################

# Load the county demographics data for each year and concatenate them into a single dataframe
demo_df = []
for year in year_range:
    demo_data = pd.read_csv(f'./data/intermediate/state_demographics/state_demographics_{year}.csv')
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

print(demographics)

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

print(minwage)

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

print(pdmps)

### Load the sector composition data #################################################################

# Obtain the sector composition data for each year
sectors_df = []
for year in year_range:
    sector_data = pd.read_csv(f'./data/intermediate/sector_composition/sector_comp_{year}.csv')
    sector_data['year'] = year
    sectors_df.append(sector_data)
sector_shares = pd.concat(sectors_df)

sector_shares = sector_shares.drop('emp_ratio',axis=1)

sector_shares['fips'] = sector_shares['fips'].astype(str).str.rjust(5, '0')
sector_shares['state_fip'] = sector_shares['fips'].astype(str).str[:2]

# Group the data by 'naics', 'year', and 'state_fip' and sum the 'emp' columns
sector_shares = sector_shares.groupby(['naics', 'year', 'state_fip'])['emp'].sum().reset_index()

print(sector_shares)

# Create a copy of the sector_shares dataframe (covariaes)
sector_shares_cov = sector_shares.copy()

# Extract the first two digits of the 'naics' column and create a new column 'naics_2digits'
sector_shares_cov['naics_2digits'] = sector_shares_cov['naics'].astype(str).str[:2]

# Group the data by 'naics_2digits', 'year', and 'fips' and sum the 'emp' and 'emp_ratio' columns
sector_shares_cov = sector_shares_cov.groupby(['naics_2digits', 'year', 'state_fip'])['emp'].sum().reset_index()

state_employment = sector_shares.groupby(['year', 'state_fip'])['emp'].sum().reset_index()

sector_shares_cov = pd.merge(sector_shares_cov, state_employment, on=['year', 'state_fip'], suffixes=('', '_total'))

sector_shares_cov['emp_ratio'] = (sector_shares_cov['emp'] / sector_shares_cov['emp_total']).round(4)

sector_shares_cov = sector_shares_cov.drop(['emp','emp_total'],axis=1)

# Pivot the sector_shares_cov dataframe to have 'naics_2digits' as columns
sector_shares_cov = sector_shares_cov.pivot_table(index=['year','state_fip'], columns='naics_2digits')

# Drop the top level of the column index
sector_shares_cov.columns = sector_shares_cov.columns.droplevel().rename(None)

# Reset the index of the dataframe
sector_shares_cov = sector_shares_cov.reset_index()

# Rename the columns with a prefix 'emp_' and a suffix '_ratio'
sector_shares_cov.columns = 'emp_' + sector_shares_cov.columns + '_ratio'

# Rename the columns 'emp_year_ratio' and 'emp_fips_ratio' to 'year' and 'state_fip' respectively
sector_shares_cov = sector_shares_cov.rename({'emp_year_ratio' : 'year', 'emp_state_fip_ratio' : 'state_fip'}, axis=1)

print(sector_shares_cov)

# Extract the first two digits of the 'naics' column and create a new column 'naics_4digits'
sector_shares['naics_3digits'] = sector_shares['naics'].astype(str).str[:3] + '000'

# Group the data by 'naics_4digits', 'year', and 'state_fip' and sum the 'emp' and 'emp_ratio' columns
sector_shares = sector_shares.groupby(['naics_3digits', 'year', 'state_fip'])[['emp']].sum().reset_index()

sector_shares = pd.merge(sector_shares, state_employment, on=['year', 'state_fip'], suffixes=('', '_total'))

sector_shares['emp_ratio'] = (sector_shares['emp'] / sector_shares['emp_total']).round(4)

sector_shares = sector_shares.drop(['emp_total'],axis=1)

sector_shares.rename(columns={'naics_3digits': 'naics'}, inplace=True)

print(sector_shares)

### Load the vacancies data ##########################################################################

# Get the list of files in the directory
file_list_jobs = os.listdir('./data/intermediate/job_openings')

# Extract unique prefixes from the file names (lower bar followed by number)
variables_jobs = set([re.search(pattern, file).group() for file in file_list_jobs])

# Create a dictionary to store labor market outcomes data for each variable
job_openings_years = {var: {} for var in variables_jobs}

# Iterate over each variable
for var in variables_jobs:
    # Define the pattern to find files related to the variable
    pattern_find = rf"{var}_\d+"
    # Find the files that match the pattern
    files_to_open = [re.search(pattern_find, file).group() for file in file_list_jobs if re.search(pattern_find, file)]
    # Iterate over each file
    for file in files_to_open:
        # Read the file and store the data in the dictionary
        job_openings_years[var][file] = pd.read_csv(f'./data/intermediate/job_openings/{file}.csv')

# Create a dictionary to store concatenated labor market outcomes data for each variable
job_openings = {var: {} for var in variables_jobs}

# Iterate over each variable
for var in variables_jobs:
    # Create a list to store the dataframes for each file
    df_names = []
    # Get the keys (file names) for the variable
    jobs_keys = job_openings_years[var].keys()
    # Iterate over each key
    for var_keys in jobs_keys:
        # Get the dataframe for the key
        temp_df = job_openings_years[var][var_keys]
        # Rename the 'value' column to the variable name
        temp_df.rename(columns={'       value': var}, inplace=True)
        # Append the dataframe to the list
        df_names.append(temp_df)
    # Concatenate the dataframes and store the result in the dictionary
    job_openings[var] = pd.concat(df_names)

# Identify common columns
common_columns = set.intersection(*(set(df.columns) for df in job_openings.values()))

# Convert set of common columns to a list
common_columns = list(common_columns)

# Merge all dataframes in the dictionary based on the common columns
merged_jobs = list(job_openings.values())[0]
for df in list(job_openings.values())[1:]:
    merged_jobs = pd.merge(merged_jobs, df, on=common_columns, how='outer')

# Select the rows with years between initial_year and last_year
merged_jobs = merged_jobs[(merged_jobs['year'] >= initial_year) & (merged_jobs['year'] <= last_year)]
merged_jobs[list(variables_jobs)] = merged_jobs[list(variables_jobs)].apply(pd.to_numeric, errors='coerce').astype(float)

print(merged_jobs)

### Load the wage distribution data ##################################################################

# Load the industry wage distribution data
wage_dist_df = []
for year in year_range:
    dist_data = pd.read_csv(f'./data/intermediate/industry_wage_distribution_states/industry_wages{year}.csv')
    dist_data['year'] = year
    dist_data = dist_data[dist_data['occ_code'] == '00-0000']
    dist_data = dist_data.drop('occ_code', axis=1)
    dist_data = dist_data.rename(columns={'h_median':'h_pct50'})
    if 'area_title' in dist_data.columns:
        dist_data.rename(columns={'area_title': 'state'}, inplace=True)
    wage_dist_df.append(dist_data)
state_wage_dist = pd.concat(wage_dist_df)

# Drop rows where state is Guam, Puerto Rico, or Virgin Islands
state_wage_dist = state_wage_dist[~state_wage_dist['state'].isin(['Guam', 'Puerto Rico', 'Virgin Islands'])]

h_columns = [col for col in state_wage_dist.columns if col.startswith('h_')]
state_wage_dist[h_columns] = state_wage_dist[h_columns].apply(pd.to_numeric, errors='coerce').astype(float)

print(state_wage_dist)

### Load the overdose deaths data ####################################################################

###

### Load the prescriptions data ######################################################################

state_fips_arcos = pd.read_csv('./data/source/fips/county_fips_arcos.csv')
state_fips_arcos['countyfips'] = state_fips_arcos['countyfips'].astype(str).str.rjust(5, '0')
state_fips_arcos['state_fip'] = state_fips_arcos['countyfips'].str[:2]

arcos_data = []
for year in year_range_prescriptions:
    # Load the prescription data for each year
    prescriptions_data = pd.read_csv(f'./data/source/prescriptions/prescriptions_{year}.csv')
    # Merge the prescription data with the county fips data
    prescriptions_data = pd.merge(prescriptions_data, state_fips_arcos, on=['BUYER_COUNTY','BUYER_STATE'], how='inner')
    # Group the data by county and year-month and sum dosage unit, MME conversion factor, and base weight in grams
    prescriptions_data = prescriptions_data.groupby(['state_fip', 'year', 'month'])[['DOSAGE_UNIT', 'MME_CONVERSION_FACTOR', 'CALC_BASE_WT_IN_GM']].sum().reset_index()
    # Append the dataframe to the list
    arcos_data.append(prescriptions_data)
# Concatenate the dataframes in the list
prescriptions = pd.concat(arcos_data)

# Rename the columns
prescriptions = prescriptions.rename(columns={
    'DOSAGE_UNIT': 'dosage_unit',
    'MME_CONVERSION_FACTOR': 'mme_conversion_factor',
    'CALC_BASE_WT_IN_GM': 'base_weight'
    })


# Merge the dataframes

# Merge the labor market outcomes data with the county demographics data

merged_data = pd.merge(merged_lmos, demographics, on=['state_fip', 'year'], how='inner')
set.intersection(set(merged_lmos.columns), set(demographics.columns))

merged_data['lab_force_rate'] = (merged_data['labor_force'] / merged_data['working_age_pop'] * 100).round(4)

merged_data = pd.merge(merged_data, minwage, on=['state_name', 'year'], how='inner')
set.intersection(set(merged_data.columns), set(minwage.columns))

merged_data = pd.merge(merged_data, pdmps, on=['state_name'], how='inner')
set.intersection(set(merged_data.columns), set(pdmps.columns))

merged_jobs.rename(columns={'state_code': 'state_fip'}, inplace=True)
merged_data = pd.merge(merged_data, merged_jobs, on=['state_fip', 'year', 'month'], how='inner')
set.intersection(set(merged_data.columns), set(merged_jobs.columns))

merged_data['state_fip'] = merged_data['state_fip'].astype(str).str.rjust(2, '0')
merged_data = pd.merge(merged_data, sector_shares_cov, on=['state_fip', 'year'], how='inner')
set.intersection(set(merged_data.columns), set(sector_shares_cov.columns))

state_wage_dist.rename(columns={'area': 'state_fip'}, inplace=True)
state_wage_dist['state_fip'] = state_wage_dist['state_fip'].astype(str).str.rjust(2, '0')
merged_data = pd.merge(merged_data, state_wage_dist, on=['state_fip', 'year'], how='inner')
set.intersection(set(merged_data.columns), set(state_wage_dist.columns))

merged_data['log_minw'] = np.log(merged_data['min_wage'])
merged_data['log_h_pct10'] = np.log(merged_data['h_pct10'])
merged_data['log_h_pct25'] = np.log(merged_data['h_pct25'])
merged_data['log_h_pct50'] = np.log(merged_data['h_pct50'])
merged_data['log_h_pct75'] = np.log(merged_data['h_pct75'])
merged_data['log_h_pct90'] = np.log(merged_data['h_pct90'])

merged_data['kaitz_pct10'] = merged_data['log_minw'] - merged_data['log_h_pct10']
merged_data['kaitz_pct25'] = merged_data['log_minw'] - merged_data['log_h_pct25']
merged_data['kaitz_pct50'] = merged_data['log_minw'] - merged_data['log_h_pct50']
merged_data['kaitz_pct75'] = merged_data['log_minw'] - merged_data['log_h_pct75']
merged_data['kaitz_pct90'] = merged_data['log_minw'] - merged_data['log_h_pct90']

# Convert 'year' and 'month' columns to integers
merged_data['year'] = merged_data['year'].astype('Int64')
merged_data['month'] = merged_data['month'].astype('Int64')

print(merged_data)

# Save the merged data to a CSV file
merged_data.to_csv('./data/processed/merged_data_states.csv', index=False)


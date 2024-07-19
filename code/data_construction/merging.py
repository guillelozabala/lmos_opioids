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

print(merged_lmos)

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

print(sector_shares)

# Create a copy of the sector_shares dataframe (covariaes)
sector_shares_cov = sector_shares.copy()

# Drop the 'emp' column
sector_shares_cov = sector_shares_cov.drop('emp',axis=1)

# Extract the first two digits of the 'naics' column and create a new column 'naics_2digits'
sector_shares_cov['naics_2digits'] = sector_shares_cov['naics'].astype(str).str[:2]

# Group the data by 'naics_2digits', 'year', and 'fips' and sum the 'emp' and 'emp_ratio' columns
sector_shares_cov = sector_shares_cov.groupby(['naics_2digits', 'year', 'fips'])['emp_ratio'].sum().reset_index()

# Pivot the sector_shares_cov dataframe to have 'naics_2digits' as columns
sector_shares_cov = sector_shares_cov.pivot_table(index=['year','fips'], columns='naics_2digits')

# Drop the top level of the column index
sector_shares_cov.columns = sector_shares_cov.columns.droplevel().rename(None)

# Reset the index of the dataframe
sector_shares_cov = sector_shares_cov.reset_index()

# Rename the columns with a prefix 'emp_' and a suffix '_ratio'
sector_shares_cov.columns = 'emp_' + sector_shares_cov.columns + '_ratio'

# Rename the columns 'emp_year_ratio' and 'emp_fips_ratio' to 'year' and 'fips' respectively
sector_shares_cov = sector_shares_cov.rename({'emp_year_ratio' : 'year', 'emp_fips_ratio' : 'fips'}, axis=1)

print(sector_shares_cov)

# Extract the first two digits of the 'naics' column and create a new column 'naics_4digits'
sector_shares['naics_3digits'] = sector_shares['naics'].astype(str).str[:3] + '000'

# Group the data by 'naics_4digits', 'year', and 'fips' and sum the 'emp' and 'emp_ratio' columns
sector_shares = sector_shares.groupby(['naics_3digits', 'year', 'fips'])[['emp','emp_ratio']].sum().reset_index()
# lo de arriba esta mal, los ratios no se suman, se promedian

sector_shares.rename(columns={'naics_3digits': 'naics'}, inplace=True)

print(sector_shares)

### Load the industry wage distribution data ##########################################################

# Load the industry wage distribution data
wage_dist_df = []
for year in year_range:
    dist_data = pd.read_csv(f'./data/intermediate/industry_wage_distribution/industry_wages{year}.csv')
    dist_data['year'] = year
    dist_data = dist_data[dist_data['occ_code'] == '00-0000']
    dist_data = dist_data.drop('occ_code', axis=1)
    dist_data = dist_data.rename(columns={'h_median':'h_pct50'})
    wage_dist_df.append(dist_data)
industry_wage_dist = pd.concat(wage_dist_df)

print(industry_wage_dist)

industry_wage_dist['naics_3digits'] = industry_wage_dist['naics'].astype(str).str[:3] + '000'
industry_wage_dist['tot_emp'] = pd.to_numeric(industry_wage_dist['tot_emp'], errors='coerce')
unique_naics = industry_wage_dist['naics_3digits'].unique().tolist()

h_columns = [col for col in industry_wage_dist.columns if col.startswith('h_')]

ind_wage_dist_3naics = []
for year in year_range:
    for cod in unique_naics:
        split_4naics = industry_wage_dist[(industry_wage_dist['naics_3digits'] == cod) & (industry_wage_dist['year'] == year)]
        if not split_4naics.empty:
            row = {
                'year': year,
                'naics': cod,
                'h_pct10':  np.percentile(split_4naics['h_pct10'], 10, method='inverted_cdf',  weights=split_4naics['tot_emp']),
                'h_pct25':  np.percentile(split_4naics['h_pct25'], 25, method='inverted_cdf',  weights=split_4naics['tot_emp']),
                'h_pct50':  np.percentile(split_4naics['h_pct50'], 50, method='inverted_cdf',  weights=split_4naics['tot_emp']),
                'h_pct75':  np.percentile(split_4naics['h_pct75'], 75, method='inverted_cdf',  weights=split_4naics['tot_emp']),
                'h_pct90':  np.percentile(split_4naics['h_pct90'], 90, method='inverted_cdf',  weights=split_4naics['tot_emp'])
            }
            ind_wage_dist_3naics.append(row)

ind_wage_dist_final = pd.DataFrame(ind_wage_dist_3naics)


missing_naics_sshares = [naic for naic in sector_shares['naics'].unique().tolist() if naic not in ind_wage_dist_final['naics'].unique().tolist()]
missing_naics_sshares
missing_naics_wdist =  [naic for naic in ind_wage_dist_final['naics'].unique().tolist() if naic not in sector_shares['naics'].unique().tolist()]
missing_naics_wdist

common_naics = set.intersection(set(sector_shares['naics'].unique()), set(ind_wage_dist_final['naics'].unique()))
common_naics

county_wage_dist = pd.merge(ind_wage_dist_final, sector_shares, on=['naics', 'year'], how='outer')
county_wage_dist

county_wage_dist['fips'] = county_wage_dist['fips'].astype("Int64")

wage_distributions = []
for year in year_range:
    for fips in unique_fips:
        split_3naics = county_wage_dist[(county_wage_dist['fips'] == fips) & (county_wage_dist['year'] == year)]
        split_3naics = split_3naics[split_3naics['emp'] != 0]
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
            wage_distributions.append(row)

fips_wage_distributions = pd.DataFrame(wage_distributions)

fips_wage_distributions[h_columns] = fips_wage_distributions[h_columns].apply(pd.to_numeric, errors='coerce').astype(float)


print(fips_wage_distributions)

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

# Merge the dataframes

# Merge the labor market outcomes data with the county demographics data

# Identify the missing FIPS codes
missing_fips_lmos = [fips for fips in unique_fips if fips not in merged_lmos['fips'].unique()]
missing_counties_lmos = fips_details[fips_details['fips'].isin(missing_fips_lmos)].iloc[:, [1,3]]

# Identify the missing FIPS codes
missing_fips_demo = [fips for fips in unique_fips if fips not in demographics['fips'].unique()]
missing_counties_demo = fips_details[fips_details['fips'].isin(missing_fips_demo)].iloc[:, [1,3]]

# missing_fips_demo_1 = [fips for fips in demographics['fips'].unique().tolist() if fips not in unique_fips]
#
# https://seer.cancer.gov/seerstat/variables/countyattribs/ruralurban.html
# 2201 -> Prince of Wales - Outer Ketchickan Census Area
# 2232 -> Skagway-Hoonah-Angoon Census Area
# 2280 -> Wrangell-Petersburg Census Area
# 51917 -> grouped county, Bedford City (51515) and County (51019)
# 99999 -> Unknown/missing

missing_fips_lmos + missing_fips_demo

merged_data = pd.merge(merged_lmos, demographics, on=['fips', 'year'], how='inner')

merged_data['lab_force_rate'] = (merged_data['labor_force'] / merged_data['working_age_pop'] * 100).round(4)

# obtain labor force part ratio etc

sorted(minwage['state_name'].unique()) == sorted(fips_details['state_name'].unique())
sorted(merged_lmos['state_name'].unique()) == sorted(fips_details['state_name'].unique())

merged_data = pd.merge(merged_data, minwage, on=['state_name', 'year'], how='inner')

sorted(merged_data['state_name'].unique()) == sorted(pdmps['state_name'].unique())

merged_data = pd.merge(merged_data, pdmps, on=['state_name'], how='inner')

missing_fips_sshares = [fips for fips in unique_fips if fips not in sector_shares_cov['fips'].unique()]
missing_counties_sshares = fips_details[fips_details['fips'].isin(missing_fips_sshares)].iloc[:, [1,3]]

[fips for fips in sector_shares_cov['fips'].unique().tolist() if fips not in unique_fips]
# thats a lot, check

merged_data = pd.merge(merged_data, sector_shares_cov, on=['fips', 'year'], how='inner')
merged_data = pd.merge(merged_data, fips_wage_distributions, on=['fips', 'year'], how='inner')
merged_data = pd.merge(merged_data, od_deaths_total, on=['fips', 'year'], how='inner')

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

print(merged_data)

# Save the merged data to a CSV file
merged_data.to_csv('./data/processed/merged_data.csv', index=False)


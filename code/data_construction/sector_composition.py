import pandas as pd

'''
From 1997 to 1998, change from SIC codes to NAICS codes
'''

# Define column names for SIC and NAICS dataframes
df_columns_sic = ['fips', 'sic', 'emp']
df_columns_naic = ['fips', 'naics', 'emp']

'''
# Read the SIC to NAICS crosswalk data
sic_to_naics = df = pd.read_csv(r'./data/source/sic_to_naics/tabula-NAICS-to-SIC-Crosswalk.csv', header = 1)

# CHECK that this works:
sic_to_naics = sic_to_naics[['SIC', 'NAICS']].copy()
sic_to_naics['SIC'] = sic_to_naics['SIC'].astype(str).str.rjust(4, '0')
sic_to_naics['NAICS'] = sic_to_naics['NAICS'].astype(str).str.rjust(6, '0')
sic_to_naics = sic_to_naics.rename(columns={'SIC': 'sic', 'NAICS': 'naics'})
# CHECK
len(sic_to_naics['sic']) == len(sic_to_naics['sic'].unique())
len(sic_to_naics['naics']) == len(sic_to_naics['naics'].unique())
# none of the is true (drop randomly?) ... fucking mess
'''

# Loop through the years from 1990 to 2022
for year in range(1990, 2023):
    file_number = str(year)[2:]
    
    # Read the sector composition data for the current year
    df = pd.read_csv(f'./data/source/sector_composition/cbp{file_number}co.zip')
    
    # ??
    if year == 2015:
        df = df.rename(
            columns = {
                'FIPSTATE': 'fipstate',
                'FIPSCTY': 'fipscty',
                'NAICS': 'naics',
                'EMP':'emp'
            }
        )

    # Format the fips column by padding with leading zeros
    df['fipstate'] = df['fipstate'].astype(str).str.rjust(2, '0')
    df['fipscty'] = df['fipscty'].astype(str).str.rjust(3, '0')
    df['fips'] = df['fipstate'] + df['fipscty']
    
    if year < 1998:
        # Process SIC data
        
        # Select only the required columns
        df = df[df_columns_sic]
        
        # Pad the SIC codes with leading zeros
        df['sic'] = df['sic'].astype(str).str.rjust(4, '0')
        
        # Remove rows with invalid SIC codes
        df = df[~df['sic'].str.contains('-')]

        # Calculate emp_ratio for each unique fips value
        df['emp_ratio'] = (df['emp'] / df.groupby(['fips'])['emp'].transform('sum')).round(4)

    else:
        # Process NAICS data
        
        # Select only the required columns
        df = df[df_columns_naic]
        
        # Pad the NAICS codes with leading zeros
        df['naics'] = df['naics'].astype(str).str.rjust(6, '0')

        # Remove rows with invalid SIC codes
        df = df[~df['naics'].str.contains('-|/')]

        # Calculate emp_ratio for each unique fips value
        df['emp_ratio'] = (df['emp'] / df.groupby(['fips'])['emp'].transform('sum')).round(4)
    
    # Save the processed data to a CSV file
    df.to_csv(f'./data/intermediate/sector_composition/sector_comp_{year}.csv', sep=',', index=False)
import pandas as pd
import zipfile
import io
import os
import re

# Define the columns to add
columns_to_add = ['h_pct10', 'h_pct25', 'h_pct75', 'h_pct90']

# Get the current folder path
folder_path = os.getcwd()

# Get the list of files in the current folder
file_names = os.listdir(folder_path + '\\data\\source\\industry_wage_distribution')

for files in file_names:
    # Extract the year from the file name
    year = re.findall(r'(?<!\d)\d{2}(?!\d)', files)[0]
    if year in ['97', '98', '99', '00', '01']:
        # Open the zip file for the corresponding year
        file = zipfile.ZipFile(f'./data/source/industry_wage_distribution/oes'+ year +'in3.zip')
        # Get the name of the file inside the zip
        file_name = file.namelist()[0]
        # Determine the number of rows to skip based on the year
        skipper = 31*(year == '97' or year == '98') + 35*(year == '99') + 33*(year == '00') + 0
        # Determine the header row based on the year
        head = 1*(year != '01')
        # Read the Excel file inside the zip, skipping the specified number of rows and using the specified header row
        df = pd.read_excel(io.BytesIO(file.open(file_name).read()), skiprows=skipper, header=head)
        # Select the desired columns from the DataFrame
        df = df[['sic', 'occ_code', 'tot_emp', 'h_median']]
        # Add empty columns for the specified columns to add
        df[columns_to_add] = ""
        # Determine the complete year based on the year
        if year in ['97', '98', '99']:
            complete_year = '19' + year
        else:
            complete_year = '20' + year
        # Save the DataFrame to a CSV file with the complete year in the file name
        df.to_csv(f'./data/intermediate/industry_wage_distribution/industry_wages'+ complete_year + '.csv', sep=',', index=False)
    elif year in ['12', '13']:
        # Open the zip files for the corresponding year
        file_1 = zipfile.ZipFile(f'./data/source/industry_wage_distribution/oesm'+ year +'in4_1.zip')
        file_2 = zipfile.ZipFile(f'./data/source/industry_wage_distribution/oesm'+ year +'in4_2.zip')
        # Get the name of the file inside the zips
        file_name_1 = file_1.namelist()[0]
        file_name_2 = file_2.namelist()[0]
        # Read the Excel files inside the zips
        df_1 = pd.read_excel(io.BytesIO(file_1.open(file_name_1).read()))
        df_2 = pd.read_excel(io.BytesIO(file_2.open(file_name_2).read()))
        # Join the two DataFrames
        df = pd.concat([df_1, df_2])
        # Lowercase the column names
        df.columns = map(str.lower, df.columns)
        # Select the desired columns from the DataFrame
        df = df[['naics', 'occ_code', 'tot_emp', 'h_median'] + columns_to_add]
        # Save the DataFrame to a CSV file with the complete year in the file name
        complete_year = '20' + year
        df.to_csv(f'./data/intermediate/industry_wage_distribution/industry_wages'+ complete_year + '.csv', sep=',', index=False)
    else:
        # Open the zip file for the corresponding year
        if year == '02':
            file = zipfile.ZipFile(f'./data/source/industry_wage_distribution/oes'+ year +'in4.zip')
        else:
            file = zipfile.ZipFile(f'./data/source/industry_wage_distribution/oesm'+ year +'in4.zip')
        # Get the name of the file inside the zip
        file_name = file.namelist()[0]
        # Read the Excel file inside the zip
        df = pd.read_excel(io.BytesIO(file.open(file_name).read()))
        # Lowercase the column names
        df.columns = map(str.lower, df.columns)
        # Select the desired columns from the DataFrame
        df = df[['naics', 'occ_code', 'tot_emp', 'h_median'] + columns_to_add]
        # Save the DataFrame to a CSV file with the complete year in the file name
        complete_year = '20' + year
        df.to_csv(f'./data/intermediate/industry_wage_distribution/industry_wages'+ complete_year + '.csv', sep=',', index=False)
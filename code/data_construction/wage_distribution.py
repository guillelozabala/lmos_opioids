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

# Initialize an empty dictionary to store the wage data

for files in file_names:
    # Extract the year from the file name
    year = re.findall(r'(?<!\d)\d{2}(?!\d)', files)[0]
    if year in ['97', '98', '99', '00', '01']:
        file =zipfile.ZipFile(f'./data/source/industry_wage_distribution/oes'+ year +'in3.zip')
        file_name = file.namelist()[0]
        skipper = 31*(year == '97' or year == '98') + 35*(year == '99') + 33*(year == '00') + 0
        head = 1 - 1*(year == '01')
        df = pd.read_excel(io.BytesIO(file.open(file_name).read()),skiprows=skipper, header=head)
        df = df[['sic', 'occ_code', 'tot_emp', 'h_median']]
        df[columns_to_add] = ""
        if year in ['97', '98', '99']:
            complete_year = '19' + year
        else:
            complete_year = '20' + year
        df.to_csv(f'./data/intermediate/industry_wage_distribution/industry_wages'+ complete_year + '.csv', sep=',', index=False)
    elif year in ['12', '13']:
        file_1 = zipfile.ZipFile(f'./data/source/industry_wage_distribution/oesm'+ year +'in4_1.zip')
        file_2 = zipfile.ZipFile(f'./data/source/industry_wage_distribution/oesm'+ year +'in4_2.zip')
        
        file_name_1 = file_1.namelist()[0]
        file_name_2 = file_2.namelist()[0]

        df_1 = pd.read_excel(io.BytesIO(file_1.open(file_name_1).read()))
        df_2 = pd.read_excel(io.BytesIO(file_2.open(file_name_2).read()))

        df = pd.concat([df_1, df_2])

        df.columns = map(str.lower, df.columns)
        df = df[['naics', 'occ_code', 'tot_emp', 'h_median']+ columns_to_add]

        complete_year = '20' + year
        df.to_csv(f'./data/intermediate/industry_wage_distribution/industry_wages'+ complete_year + '.csv', sep=',', index=False)
    else:
        if year == '02':
            file = zipfile.ZipFile(f'./data/source/industry_wage_distribution/oes'+ year +'in4.zip')
        else:
            file = zipfile.ZipFile(f'./data/source/industry_wage_distribution/oesm'+ year +'in4.zip')
        file_name = file.namelist()[0]
        df = pd.read_excel(io.BytesIO(file.open(file_name).read()))
        df.columns = map(str.lower, df.columns)
        df = df[['naics', 'occ_code', 'tot_emp', 'h_median'] + columns_to_add]

        complete_year = '20' + year
        df.to_csv(f'./data/intermediate/industry_wage_distribution/industry_wages'+ complete_year + '.csv', sep=',', index=False)
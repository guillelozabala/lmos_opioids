import subprocess
import os
#import pandas as pd, and installi it and so on

# Get the current working directory
folder_path = os.getcwd()

# Define file paths for different Python scripts
file_path_demographics = os.path.join(folder_path, 'code\\data_construction\\', 'demographics.py')
file_path_labor_market = os.path.join(folder_path, 'code\\data_construction\\', 'labor_market_outcomes.py')
file_path_sector_comp = os.path.join(folder_path, 'code\\data_construction\\', 'sector_composition.py')
file_path_min_wage = os.path.join(folder_path, 'code\\data_construction\\', 'minwage_cleaning.py')

# Run the scripts
subprocess.run(['python', file_path_demographics])
subprocess.run(['python', file_path_labor_market])
subprocess.run(['python', file_path_sector_comp])
subprocess.run(['python', file_path_min_wage])
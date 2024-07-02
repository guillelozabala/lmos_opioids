import subprocess
import os
#import pandas as pd

# Get the current working directory
folder_path = os.getcwd()

# Define file paths for different Python scripts
file_path_demographics = os.path.join(folder_path, 'code\\data_construction\\', 'demographics.py')
file_path_labor_market = os.path.join(folder_path, 'code\\data_construction\\', 'labor_market_outcomes.py')

# Run the scripts
subprocess.run(['python', file_path_demographics])
subprocess.run(['python', file_path_labor_market])
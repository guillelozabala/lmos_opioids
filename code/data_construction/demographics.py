import os
import pandas as pd

# Get the list of files in the directory
file_list = os.listdir('./data/source/county_demographics')

# Extract the unique suffixes from the file names
list_of_years = set([file.split('_')[-1].split('.')[0] for file in file_list])

for time in list_of_years:
    # Read the data from the file
    file_path = f'./data/source/county_demographics/demo_data_{time}.csv'
    data = pd.read_csv(file_path)

    # Extract relevant information from the 'value' column
    data['year'] = data['value'].str[:4]
    data['state'] = data['value'].str[4:6]
    data['state_fip'] = data['value'].str[6:8]
    data['county_fip'] = data['value'].str[8:11]
    data['registry'] = data['value'].str[12:14]
    data['race'] = data['value'].str[13:14]
    data['origin'] = data['value'].str[14:15]
    data['sex'] = data['value'].str[15:16]
    data['age'] = data['value'].str[16:18]
    data['population'] = data['value'].str[18:26]

    # Create a new column 'fips' by combining 'state_fip' and 'county_fip'
    data['fips'] = data['state_fip'] + data['county_fip']

    # Drop unnecessary columns
    data = data.drop(columns=['value', 'county_fip', 'state'])

    # Convert columns to integer type
    data = data.astype(int)

    # Split the data into two dataframes: one for counties and one for states
    data_county = data.drop(columns=['state_fip'])
    data_states = data.drop(columns=['fips'])

    # Drop the 'registry' column
    data_county = data_county.groupby(['year', 'race', 'origin', 'sex', 'age', 'fips'])['population'].sum().reset_index()
    data_states = data_states.groupby(['year', 'race', 'origin', 'sex', 'age', 'state_fip'])['population'].sum().reset_index()

    # Filter population data by race
    race_columns = ['w_population', 'b_population', 'na_population', 'a_population']
    race_values = [1, 2, 3, 4]

    '''
    1 = White
    2 = Black
    3 = American Indian/Alaska Native
    4 = Asian or Pacific Islander
    '''

    race_data_counties = []
    race_data_states = []

    for column, value in zip(race_columns, race_values):
        # Group by year and fips for each race
        data_county_byrace = data_county[data_county['race'] == value].groupby(['year', 'fips'])['population'].sum().reset_index()
        data_state_byrace = data_states[data_states['race'] == value].groupby(['year', 'state_fip'])['population'].sum().reset_index()
        # Rename the columns
        data_county_byrace = data_county_byrace.rename(columns={'population': column})
        data_state_byrace = data_state_byrace.rename(columns={'population': column})
        # Append the data to the list
        race_data_counties.append(data_county_byrace)
        race_data_states.append(data_state_byrace)
        
    # Filter population data by origin
    origin_columns = ['nh_population', 'hi_population']
    origin_values = [0, 1]

    '''
    0 = Non-Hispanic
    1 = Hispanic
    9 = Not applicable in 1969+ W,B,O files (EMPTY)
    '''

    origin_data_counties = []
    origin_data_states = []

    for column, value in zip(origin_columns, origin_values):
        # Group by year and fips for each origin
        data_county_byorigin = data_county[data_county['origin'] == value].groupby(['year', 'fips'])['population'].sum().reset_index()
        data_state_byorigin = data_states[data_states['origin'] == value].groupby(['year', 'state_fip'])['population'].sum().reset_index()
        # Rename the columns
        data_county_byorigin = data_county_byorigin.rename(columns={'population': column})
        data_state_byorigin = data_state_byorigin.rename(columns={'population': column})
        # Append the data to the list
        origin_data_counties.append(data_county_byorigin)
        origin_data_states.append(data_state_byorigin)

    # Filter population data by sex
    sex_columns = ['male_population', 'female_population']
    sex_values = [1, 2]

    sex_data_counties = []
    sex_data_states = []

    for column, value in zip(sex_columns, sex_values):
        # Group by year and fips for each sex
        data_county_bysex = data_county[data_county['sex'] == value].groupby(['year', 'fips'])['population'].sum().reset_index()
        data_state_bysex = data_states[data_states['sex'] == value].groupby(['year', 'state_fip'])['population'].sum().reset_index()
        # Rename the columns
        data_county_bysex = data_county_bysex.rename(columns={'population': column})
        data_state_bysex = data_state_bysex.rename(columns={'population': column})
        # Append the data to the list
        sex_data_counties.append(data_county_bysex)
        sex_data_states.append(data_state_bysex)

    # Filter population data by age (19 groups)
    age_data_counties = []
    age_data_states = []

    unique_age_values = data['age'].unique()

    for age_value in unique_age_values:
        # Group by year and fips for each age group
        data_county_byage = data_county[data_county['age'] == age_value].groupby(['year', 'fips'])['population'].sum().reset_index()
        data_state_byage = data_states[data_states['age'] == age_value].groupby(['year', 'state_fip'])['population'].sum().reset_index()
        # Rename the columns
        data_county_byage = data_county_byage.rename(columns={'population': f'age{age_value}_population'})
        data_state_byage = data_state_byage.rename(columns={'population': f'age{age_value}_population'})
        # Append the data to the list
        age_data_counties.append(data_county_byage)
        age_data_states.append(data_state_byage)

    # Aggregate everything at the year-fip and year-state level
    demo_county = data_county.groupby(['year', 'fips'])['population'].sum().reset_index()
    demo_states = data_states.groupby(['year', 'state_fip'])['population'].sum().reset_index()

    # Outer join everything
    for df in race_data_counties + origin_data_counties + sex_data_counties + age_data_counties:
        demo_county = demo_county.merge(df, on=['year', 'fips'], how='outer')

    for data in race_data_states + origin_data_states + sex_data_states + age_data_states:
        demo_states = demo_states.merge(data, on=['year', 'state_fip'], how='outer')

    # Correct the null values
    demo_county = demo_county.fillna(0)
    demo_states = demo_states.fillna(0)

    # Obtain the ratios
    all_columns = race_columns + origin_columns + sex_columns
    all_columns += [f'age{age_value}_population' for age_value in unique_age_values]
    #ratio_columns = [s + '_ratio' for s in ratio_columns]

    for column in all_columns:
        numerator = column
        denominator = 'population'
        demo_county[column + '_ratio'] = round(demo_county[numerator] / demo_county[denominator], 5)
        demo_states[column + '_ratio'] = round(demo_states[numerator] / demo_states[denominator], 5)

    # Drop the columns without the suffix '_ratio' except for year, fips, and population
    demo_county = demo_county[['year', 'fips', 'population'] + [col for col in demo_county.columns if col.endswith('_ratio')]]
    demo_states = demo_states[['year', 'state_fip', 'population'] + [col for col in demo_states.columns if col.endswith('_ratio')]]

    # Save the data
    demo_states.to_csv(f'./data/intermediate/county_demographics/county_demographics_{time}.csv',sep=',',index=False)
    demo_county.to_csv(f'./data/intermediate/state_demographics/state_demographics_{time}.csv',sep=',',index=False)



import pandas as pd

# See minwage.py
df = pd.read_csv(r'./data/source/minimum_wage/minwage_raw.csv', sep=',')

# Drop non-relevant observations
df = df.drop(['1968 (a)', '1970 (a)', '1972', '1976 (a)', '1979', '1980', '1981', '1988'], axis=1)
df = df[df['State or otherjurisdiction'] != 'Guam']
df = df[df['State or otherjurisdiction'] != 'Puerto Rico']
df = df[df['State or otherjurisdiction'] != 'U.S. Virgin Islands']

# Remove dollar sign from the values
df = df.replace({'\$': ''}, regex=True)

'''
> from: https://www.debt.org/jobs/minimum-wage/#:~:text=What%20Happens%20if%20a%20State's,they%20can%20be%20paid%20less.
>
> **Do States Have to Follow the Federal Minimum Wage?**
>
> States are required to follow federal minimum wage law. 
> States can pass their own laws to make the wage higher, 
> equal to or lower than the federal law, but they can’t make 
> other changes that overrule the federal law, for instance, 
> who is exempt or how many hours constitutes a work week.
>
> Federal minimum wage overrides state if the state wage is lower. 
> It doesn’t if the state wage is higher. Hourly workers who come 
> under Fair Labor Standard Act guidelines, which most hourly 
> workers do, always get the higher wage. Some cities have higher 
> minimum wage laws than their state wage. In those cases, too, 
> the higher wage prevails.
>
> If a state has no minimum wage law, the federal wage prevails.

'''

# Apply effective minimum wage
for i in range(len(df.iloc[:,0])):
    for j in range(len(df.columns)):
        if df.iloc[i,j] == '...':
            df.iloc[i,j] = df.iloc[0,j]

'''
> [c] - Rates applicable to employers of four or more.
>
> (d) - Rates applicable to employers of six or more. 
> In West Virginia, applicable to employers of six or more 
> in one location.
>
> (e) Rates applicable to employers of two or more.

'''

# Select higher minimum wage
df = df.replace({'\[c\]':''}, regex=True).replace({'\(d\)':''}, regex=True).replace({'\(e\)':''}, regex=True)

'''
> (g) - Minnesota sets a lower rate for enterprises with 
> annual receipts of less than $500,000 ($4.90, January 1, 
> 1998-January 1, 2005). The dollar amount prior to September 
> 1, 1997 was $362,500 ($4.00 - January 1, 1991-January 1, 
> 1997); Montana sets a lower rate for businesses with gross 
> annual sales of $110,000 or less ($4.00 - January 1, 1992-January 
> 1, 2005); Ohio sets a lower rate for employers with gross 
> annual sales from $150,000 to $500,000 ($3.35 - January 1, 
> 1991-January 1, 2005) and for employers with gross annual 
> sales under $150,000 ($2.50 - January 1, 1991-January 1, 2005); 
> Oklahoma sets a lower rate for employers of fewer than 10 
> full-time employees at any one location and for those with 
> annual gross sales of less than $100,000 ($2.00, January 1, 
> 1991-January 1, 2005); and the U.S. Virgin Islands sets a 
> lower rate for businesses with gross annual receipts of less 
> than $150,000 ($4.30, January 1, 1991-January 1, 2005).
>
> (h) - In the District of Columbia, wage orders were replaced 
> by a statutory minimum wage on October 1, 1993. A $5.45 minimum 
> rate remained in effect for the laundry and dry cleaning industry 
> as the result of the grandfather clause.

'''

# Select higher minimum wage
df = df.replace({'\(g\)':''}, regex=True).replace({'\(h\)':''}, regex=True)

# There are States with two different minimum wages (not clarified, maybe FLSA (?))
for i in range(len(df.iloc[:,0])):
    for j in range(len(df.columns)):
        if '-' in df.iloc[i,j]:
            df.iloc[i,j] = df.iloc[i,j].split('-')[1].lstrip().split(' ')[0]
        elif '/' in df.iloc[i,j]:
            df.iloc[i,j] = df.iloc[i,j].split('/')[1].lstrip().split(' ')[0]

# Remove blank spaces, strings to floats
df.iloc[:,1:] = df.iloc[:,1:].replace({' ':''}, regex=True)
df.iloc[:,1:-1] = df.iloc[:,1:-1].astype(float)
df = df.melt(id_vars="State or otherjurisdiction", 
        var_name="Year", 
        value_name="Value")


'''
> from: https://www.debt.org/jobs/minimum-wage/#:~:text=What%20Happens%20if%20a%20State's,they%20can%20be%20paid%20less.
>
> **What Happens if a State’s Minimum Wage Is Lower Than the Federal Minimum Wage?**
>
> In places where the state minimum wage is lower than the federal wage, workers who 
> come under the Fair Labor Standards Act earn the federal wage.
>
> If workers don’t come under the FLSA, they can be paid less. Georgia, Oklahoma and 
> Wyoming have minimum wages lower than the federal wage.
>
> Georgia and Wyoming’s are both $5.15 an hour. In Oklahoma employers pay the federal 
> minimum wage if their business is a certain size that’s more generous than federal 
> requirements. If not, workers get $2 an hour.

'''

# Apply effective fed min wage
new_df = pd.DataFrame()
for i in df['Year'].unique():
    year_data = df[df['Year'] == i].copy()
    fed_value = year_data.loc[year_data['State or otherjurisdiction'] == 'Federal (FLSA)', 'Value'].values[0]
    year_data['Value'] = year_data['Value'].apply(lambda x: max(x, fed_value))
    new_df = pd.concat([new_df,year_data])
new_df = new_df.reset_index(drop=True)

# Save to CSV
new_df.to_csv(r'./data/intermediate/minimum_wage/minwage_clean_states.csv',sep=',',index=False)


'''
> from: https://www.dol.gov/agencies/whd/compliance-assistance/handy-reference-guide-flsa
>
> All employees of certain enterprises having workers engaged in interstate commerce, 
> producing goods for interstate commerce, or handling, selling, or otherwise working 
> on goods or materials that have been moved in or produced for such commerce by any 
> person, are covered by the **FLSA**.
>
> A covered enterprise is the related activities performed through unified operation 
> or common control by any person or persons for a common business purpose and —
>> 
>> whose annual gross volume of sales made or business done is not less than $500,000 
>> (exclusive of excise taxes at the retail level that are separately stated); or
>>
>> is engaged in the operation of a hospital, an institution primarily engaged in 
>> the care of the sick, the aged, or the mentally ill who reside on the premises; 
>> a school for mentally or physically disabled or gifted children; a preschool, an
>> elementary or secondary school, or an institution of higher education (whether 
>> operated for profit or not for profit); or
>>
>> is an activity of a public agency. Any enterprise that was covered by the FLSA on 
>> March 31, 1990, and that ceased to be covered because of the revised $500,000 test, 
>> continues to be subject to the overtime pay, child labor and recordkeeping provisions 
>> of the FLSA.
>

'''
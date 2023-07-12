import pandas as pd

# Read the datasets
dataset1 = pd.read_csv('dataset1.csv')
dataset2 = pd.read_csv('dataset2.csv')

# Perform the join operation
joined_data = dataset1.merge(dataset2, on='counter_party')

# Group by legal_entity, counter_party, and tier
grouped_data = joined_data.groupby(['legal_entity', 'counter_party', 'tier'])

# Calculate the required aggregations
aggregated_data = grouped_data.agg({
    'rating': 'max',
    'value': [('ARAP_sum', lambda x: x.loc[x['status'] == 'ARAP', 'value'].sum()),
              ('ACCR_sum', lambda x: x.loc[x['status'] == 'ACCR', 'value'].sum())]
})

# Rename the columns
aggregated_data.columns = ['max(rating by counterparty)', 'sum(value where status=ARAP)', 'sum(value where status=ACCR)']

# Add the 'Total' rows
total_rows = aggregated_data.groupby(level=[0, 1]).sum()
total_rows['tier'] = 'Total'
total_rows = total_rows.reset_index()
total_rows['legal_entity'] = 'Total'
total_rows['counter_party'] = 'Total'
total_rows['max(rating by counterparty)'] = 'calculated_value'
total_rows['sum(value where status=ARAP)'] = 'calculated_value'
total_rows['sum(value where status=ACCR)'] = 'calculated_value'

# Concatenate the aggregated data with the total rows
final_data = pd.concat([aggregated_data, total_rows])

# Reset the index
final_data = final_data.reset_index(drop=True)

# Save the output to a file
final_data.to_csv('output.csv', index=False)

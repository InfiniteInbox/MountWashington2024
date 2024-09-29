import pandas as pd

df1 =  pd.read_csv(r"C:\Users\yjain\Desktop\mtwash\Mt Wash Data\last_bit_of_2022_auto_road.csv", sep=',')
df3 =  pd.read_csv(r"C:\Users\yjain\Desktop\mtwash\Mt Wash Data\last_bit_of_2022_rh.csv", sep = ',')
df2 = pd.read_csv(r"C:\Users\yjain\Desktop\mtwash\Mt Wash Data\last_bit_of_2022_summit_pressure.csv", sep = ',')
df4 = pd.read_csv(r"C:\Users\yjain\Desktop\mtwash\Mt Wash Data\last_bit_of_2022_summit_temp.csv", sep = ',')

df1['date'] = pd.to_datetime(df1['date'])
df2['date'] = pd.to_datetime(df2['date'])
df3['date'] = pd.to_datetime(df3['date'])
df4['date'] = pd.to_datetime(df4['date'])

# Merge the dataframes on 'date' column
merged_df1 = pd.merge(df1, df2, on='date', how='outer')
merged_df2 = pd.merge(df3, df4, on='date', how='outer')
merged_df = pd.merge(merged_df1, merged_df2, on='date', how='outer')
merged_df.pop('AR43Temperature')
# Sort the merged dataframe by 'date'
merged_df.sort_values(by='date', inplace=True)

print(merged_df.head())

merged_df.to_csv(r'C:\Users\yjain\Desktop\mtwash\Final Mt Wash\combined_summit_temp_pressure_vert_temp_rh_no43_last_day_of_2022.csv')
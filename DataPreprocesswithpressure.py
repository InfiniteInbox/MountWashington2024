import pandas as pd
import csv

def read_csv_skip_bad_rows(file_path):
    """
    This function reads a CSV and ommits all of the bad rows, giving us a clean csv

    Parameters:
    file_path(str): This is the file path of your "dirty" CSV

    Returns:
    df(DataFrame): This is a Pandas Dataframe with the "clean" CSV
    """
    good_rows = []
    
    with open(file_path, mode='r', encoding='utf-8') as file:
        reader = csv.reader(file)
        headers = next(reader)  # Read the header row
        num_columns = len(headers)
        
        for i, row in enumerate(reader, start=2):  # Start from line 2 since we skipped the header
            if len(row) == num_columns:
                good_rows.append(row)
            else:
                print(f"Skipping row {i}: Incomplete row with {len(row)} columns")
    
    # Create DataFrame from good rows
    df = pd.DataFrame(good_rows, columns=headers)
    return df

def combine_csvs(csv1_path, csv2_path, output_path):
    """
    This function combines CSV's on the same axis(date) while deleting overlap 

    Parameters:
    csv1_path(str): The file path of one csv
    csv2_path(str): The file path of the other csv
    output_path(str): The output file path of combined CSV's

    Returns:
    Nothing
    """
    # Load the CSV files into DataFrames, skipping bad rows
    df1 = read_csv_skip_bad_rows(csv1_path)
    df2 = read_csv_skip_bad_rows(csv2_path)
    
    if df1.empty or df2.empty:
        print("One of the CSV files could not be read or is empty.")
        return

    # Convert the datetime column to pandas datetime for proper comparison
    df1['date'] = pd.to_datetime(df1['date'])
    df2['date'] = pd.to_datetime(df2['date'])
    
    # Find overlapping datetime entries
    overlap = df1['date'].isin(df2['date'])
    
    # Remove overlapping entries from the second DataFrame
    df2_no_overlap = df2[~df2['date'].isin(df1['date'])]
    
    # Combine the DataFrames
    combined_df = pd.concat([df1, df2_no_overlap], ignore_index=True)
    
    # Sort the combined DataFrame by datetime if needed
    combined_df.sort_values(by='date', inplace=True)
    
    # Save the combined DataFrame to a new CSV file
    combined_df.to_csv(output_path, index=False)


def count_null_rows(csv_path):
    """
    This function tells you how many null rows exist in a CSV

    Parameters:
    csv_path(str): the path of the csv

    Returns:
    null_rows_count(int): How many null rows exist
    """
    # Read the CSV file into a DataFrame
    df = pd.read_csv(csv_path)
    
    # Count rows with any 'NULL' values
    null_rows_count = df.isnull().any(axis=1).sum()
    
    return null_rows_count


def remove_column_from_csv(input_csv_path, output_csv_path, column_to_remove):
    """
    This function allows you to remove a spesific column from a csv

    Parameters:
    input_csv_path(str): the path of the input csv
    output_csv_path(str): the path of the output csv
    column_to_remove(str): the column you wanna remove

    Returns:
    Nothing
    """
    # Read the CSV file into a DataFrame
    df = pd.read_csv(input_csv_path)
    
    # Check if the column exists in the DataFrame
    if column_to_remove not in df.columns:
        print(f"Column '{column_to_remove}' does not exist in the CSV file.")
        return
    
    # Remove the specified column
    df.drop(columns=[column_to_remove], inplace=True)
    
    # Save the modified DataFrame to a new CSV file
    df.to_csv(output_csv_path, index=False)
    print(f"Column '{column_to_remove}' has been removed and the new CSV file has been saved as '{output_csv_path}'.")

# Example usage
#combine_csvs(r'C:\Users\yjain\Desktop\mtwash\Mt Wash Data\2021_partial_2022_auto_road_by_minute.csv', r'C:\Users\yjain\Desktop\mtwash\Mt Wash Data\2021-02-11_and_beyond.csv', r'C:\Users\yjain\Desktop\mtwash\combined_output_2022-1-1_to_2021-02-11.csv')
#combine_csvs(r'C:\Users\yjain\Desktop\mtwash\combined_output_2022-1-1_to_2021-04-17.csv', r'C:\Users\yjain\Desktop\mtwash\Mt Wash Data\2021-04-17_and_beyond.csv', r'C:\Users\yjain\Desktop\mtwash\combined_auto_road_2022-1-1_to_2021-04-17_and_beyond.csv')
#combine_csvs(r"C:\Users\yjain\Desktop\mtwash\combined_auto_road_2022-1-1_to_2021-07-28.csv", r"C:\Users\yjain\Desktop\mtwash\Mt Wash Data\2021-07-28_and_beyond.csv", r"C:\Users\yjain\Desktop\mtwash\combined_auto_road_2022-1-1_to_2021-07-28_and_beyond.csv")
#combine_csvs(r"C:\Users\yjain\Desktop\mtwash\combined_auto_road_2022-1-1_to_2021-11-12.csv",r"C:\Users\yjain\Desktop\mtwash\Mt Wash Data\2021-11-12_and_beyond.csv", r"C:\Users\yjain\Desktop\mtwash\combined_auto_road_2022-1-1_to_2022-03-21.csv")
#combine_csvs(r"C:\Users\yjain\Desktop\mtwash\combined_auto_road_2022-1-1_to_2022-03-21.csv", r"C:\Users\yjain\Desktop\mtwash\Mt Wash Data\2022-03-21_and_beyond.csv",  r"C:\Users\yjain\Desktop\mtwash\combined_auto_road_2022-1-1_to_2022-07-01.csv")
#combine_csvs(r"C:\Users\yjain\Desktop\mtwash\combined_auto_road_2022-1-1_to_2022-07-01.csv",r"C:\Users\yjain\Desktop\mtwash\Mt Wash Data\2022-07-01_and_beyond.csv",r"C:\Users\yjain\Desktop\mtwash\combined_auto_road_2022-1-1_to_2022-10-14.csv")
#combine_csvs(r"C:\Users\yjain\Desktop\mtwash\combined_auto_road_2022-1-1_to_2022-10-14.csv",r"C:\Users\yjain\Desktop\mtwash\Mt Wash Data\2022-10-14_and_beyond.csv",r"C:\Users\yjain\Desktop\mtwash\combined_auto_road_vert_temp_2021-2022.csv")
#print(count_null_rows(r"C:\Users\yjain\Desktop\mtwash\combined_auto_road_vert_temp_2021-2022.csv"))
#remove_column_from_csv(r"C:\Users\yjain\Desktop\mtwash\combined_auto_road_vert_temp_2021-2022.csv", r"C:\Users\yjain\Desktop\mtwash\combined_auto_road_vert_temp_no_4300_2021-2022.csv",'AR43Temperature')
#combine_csvs(r"C:\Users\yjain\Desktop\mtwash\Mt Wash Data\2021_partial_2022_summit_by_minute.csv",r"C:\Users\yjain\Desktop\mtwash\Mt Wash Data\2021-06-03_and_beyond_summit.csv",r"C:\Users\yjain\Desktop\mtwash\combined_summit_temp_2021-1-1_to_2022-05-07.csv" )
#combine_csvs(r"C:\Users\yjain\Desktop\mtwash\combined_summit_temp_2021-1-1_to_2022-05-07.csv",r"C:\Users\yjain\Desktop\mtwash\Mt Wash Data\2022-05-07_and_beyond_summit.csv",r"C:\Users\yjain\Desktop\mtwash\combined_summit_temp_2021-2022.csv" )
#combine_csvs(r"C:\Users\yjain\Desktop\mtwash\Mt Wash Data\2021_partial_2022_summit_rh.csv",r"C:\Users\yjain\Desktop\mtwash\Mt Wash Data\2021-09-13_and_beyond_rh.csv", r"C:\Users\yjain\Desktop\mtwash\combined_summit_rh_2021-1-1_to_2022_11_28.csv")
#combine_csvs(r"C:\Users\yjain\Desktop\mtwash\combined_summit_rh_2021-1-1_to_2022_11_28.csv", r"C:\Users\yjain\Desktop\mtwash\Mt Wash Data\2022-11-28_and_beyond_rh.csv", r"C:\Users\yjain\Desktop\mtwash\combined_summit_rh_2021-2022.csv")
#combine_csvs(r"C:\Users\yjain\Desktop\mtwash\Mt Wash Data\2021_partial_2022_summit_pressure.csv", r"C:\Users\yjain\Desktop\mtwash\Mt Wash Data\2021-09-15_and_beyond_pressure.csv", r"C:\Users\yjain\Desktop\mtwash\combined_summit_pressure_2021-1-1_to_2022-05-22.csv")
#combine_csvs(r"C:\Users\yjain\Desktop\mtwash\combined_summit_pressure_2021-1-1_to_2022-05-22.csv", r"C:\Users\yjain\Desktop\mtwash\Mt Wash Data\2022-05-22_and_beyond_pressure.csv", r"C:\Users\yjain\Desktop\mtwash\combined_summit_pressure_2021-2022.csv")
#remove_column_from_csv(r"C:\Users\yjain\Desktop\mtwash\Final Mt Wash\combined_summit_pressure_vert_temp_rh_no43_2021-2022.csv", r"C:\Users\yjain\Desktop\mtwash\Final Mt Wash\combined_summit_pressure_vert_temp_rh_no43_2021-2022.csv", 'Unnamed: 0.1')
#combine_csvs(r"C:\Users\yjain\Desktop\mtwash\Final Mt Wash\combined_summit_temp_pressure_vert_temp_rh_no43_2021-2022.csv",r"C:\Users\yjain\Desktop\mtwash\Final Mt Wash\combined_summit_temp_pressure_vert_temp_rh_no43_last_day_of_2022.csv",r"C:\Users\yjain\Desktop\mtwash\Final Mt Wash\combined_summit_temp_pressure_vert_temp_rh_no43_2021-2022_full.csv")
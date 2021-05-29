import pandas as pd
import numpy as np


def parse_csv(path, columns, numeric_columns, sep=';', chunk_size=5, head_size=4, tail_columns_count=0):
    data = []
    with open(path) as file:
        for line in file:
            measurement = line.split(sep)
            head = measurement[:head_size]

            if chunk_size > 0:
                if len(measurement[head_size:]) % chunk_size != tail_columns_count:
                    raise Exception('Row size incorrect for line: ' + line)
                for chunk_num in range(len(measurement[head_size:]) // chunk_size):
                    data.append(head + measurement[
                                       head_size + chunk_size * chunk_num:head_size + chunk_size * chunk_num + chunk_size])
            else:
                data.append(head)

    df = pd.DataFrame(data, columns=columns)
    # convert numeric columns
    df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric)
    return df


def normalize_epoch_time(df: pd.DataFrame, column: str):
    return df.assign(epoch_time_id=lambda x: x[column].round(-3))

def normalize_lat_long(df: pd.DataFrame, lat_column: str = 'Latitude', long_column: str = 'Longitude'):
    return df.assign(latlong=lambda x: x[[lat_column, long_column]].apply(tuple, axis=1))


# Fill NA values in dataframe
# change columns - 0, other columns - forward and backward
def fill_na(df):   
    columns = df.columns
    change_columns = [col for col in columns if 'change' in col]       
    df = df.replace([np.inf, -np.inf], np.nan)    
    df[change_columns] = df[change_columns].fillna(value=0)    
    df = df.fillna(method='ffill', axis=0) \
           .fillna(method='bfill', axis=0)    
    return df


# Add columns with abs values for selected columns of dataframe
def calculate_abs_values(df, columns):
    df[[f'abs_{col}' for col in columns]] = df[[col for col in columns]].abs()


# Return diff dataframe with renamed columns     
def calculate_change(df):   
    columns = df.columns
    new_columns = [col + '_change' for col in columns]
    rename_dict = {item[0]: item[1] for item in zip(columns, new_columns)}    
    df = df.diff().rename(columns=rename_dict)
    return fill_na(df)


# Add window columns
def calculate_window(df, functions, window_sizes, window_center=True, columns=None):    
    if columns is None:
        columns = df.columns[1:]   
    for window_size in window_sizes:            
        for column in columns:
            for func in functions:
                if func == 'mean': 
                    df[column + '_window_' + str(window_size) + '_' + func] = df[column].rolling(window=window_size, center=window_center).mean()
                elif func == 'std':      
                    df[column + '_window_' + str(window_size) + '_' + func] = df[column].rolling(window=window_size, center=window_center).std()    
    return fill_na(df)

    
# Add columns with shifted values
def calculate_shift(df, periods, columns_pattern):    
    columns = [col for col in df.columns if columns_pattern in col]
    for period in periods:
        df[[f'{col}_shift_{abs(period)}_{"future" if period < 0 else "past"}' for col in columns]] = fill_na(df[[col for col in columns]].shift(periods=period))        
    return df
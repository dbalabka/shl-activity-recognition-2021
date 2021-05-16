import pandas as pd


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

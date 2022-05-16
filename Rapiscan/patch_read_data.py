import pandas as pd
import sdw4_mapping
import sdw4_mapping.common.utils
import io


def read_data(data_path, usecols, helper=lambda x: x):
    if data_path is not None:
        try:
            df = pd.read_csv(helper(data_path), 
                        usecols = usecols,
                        converters =
                            sdw4_mapping.common.utils.StringConverter())

        except Exception as e:
            if isinstance(e, FileNotFoundError):
                logging.error(f"Wrong data path: {data_path}")
                df = None
    else:
        df = None
    return df


def eof_line_helper(data_path):
    if pd.core.dtypes.inference.is_file_like(data_path):
        # Hopefully, an io.BytesIO or similar
        data = data_path.read()
    elif pd.core.dtypes.inference.iterable_not_string(data_path):
        # Hopefully, a data structure which can be read into a dataframe
        return data_path
    else:
        # Hopefully, a file name
        with open(data_path, "rb") as fh:
            data = fh.read()

    if isinstance(data, bytes):
        eof = b"EOF"
        eols = list(b"\r\n")
        spaces = list(b" ")
    else: #string
        eof = "EOF"
        eols = list("\r\n")
        spaces = list(" ")

    eols_and_spaces = eols + spaces

    n = len(data) - 1
    truncate_size = None
    while n >= 0 and data[n] in eols_and_spaces:
        n -= 1
    if n >= 2 and data[n-2:n+1].upper() == eof:
        n -= 3
        while n >= 0 and data[n] in spaces:
            n -= 1
        if n < 0 or data[n] in eols:
            truncate_size = n + 1

    if isinstance(data, bytes):
        rslt = io.BytesIO(data)
        if truncate_size is not None:
            rslt.truncate(truncate_size)
    else: #string
        if truncate_size is not None:
            data = data[:truncate_size]
        rslt = io.StringIO(data)

    return rslt
        




print("Patching sdw4_mapping.common.utils.read_data.")
def patched_read_data(data_path, usecols):
    return read_data(data_path, usecols,
       helper=eof_line_helper)

sdw4_mapping.common.utils.read_data = patched_read_data

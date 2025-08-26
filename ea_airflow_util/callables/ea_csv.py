import os
import pandas as pd


def txt_to_csv(
    file_in,
    file_out=None,
    delimiter=',',
    column_names=None,
    delete_txt=False
):
    """Convert a txt file to a csv.
    
    Args:
    - file_in (str): A path to a txt file.
    - file_out (str): A path to a csv file. If 'None', then the input file path
        is used.
    - delimiter (str): A txt file delimiter.
    - column_names (list[str]): An ordered list of column names to use in the
        output csv. If 'None', assumes that the first line in the input file
        contains column headers.
    - delete_txt (bool): If True, delete the input txt file.

    Returns:
    - file_out (str): A csv file path.
    """
    
    if column_names == None:
        # Force str dtype, otherwise pandas will do things like cast int's to
        # floats
        df = pd.read_csv(file_in, delimiter=delimiter, dtype=str)
    else:
        df = pd.read_csv(file_in, delimiter=delimiter, dtype=str, header=None)
        df.columns = column_names

    if file_out == None:
        file_out = file_in[:-4] + '.csv'

    df.to_csv(file_out, index=False)

    if delete_txt:
        os.remove(file_in)

    return file_out


def txt_files_to_csv(
    path_in,
    path_out=None,
    delimiter=',',
    column_names=None,
    delete_txt=False
):
    """Convert all txt files in a directory to csv files. Also works with a 
    single txt file path.
    
    Args:
    - path_in (str): A file or directory path containing zero or more txt files.
    - path_out (str): A file or directory path to write csv file(s) to. If
        'None', then the input path is used. Note that a file will retain its
        original names except with a .csv extension.
    - delimiter (str): A txt file delimiter. Note that this function assumes
        that all txt files in an input directory use the same delimiter.
    - column_names (list[str]): An ordered list of column names to use in the
        output csv(s). If 'None', assumes that the first line in an input file
        contain a column header. Otherwise, this function expects all input
        files in a directory to have the same ordered column headers.
    - delete_txt (bool): If True, delete all of the input txt files.

    Returns:
    - path_out (str): A file or directory path containing the output csv
        file(s).
    """

    # Check for and process a single file first
    if path_in[-4:] == '.txt':

        path_out = txt_to_csv(
            file_in=path_in,
            file_out=path_out,
            delimiter=delimiter,
            column_names=column_names,
            delete_txt=delete_txt
        )

        return path_out

    # Otherwise, process a whole directory
    if path_out == None:
        path_out = path_in

    for file in os.listdir(path_in):
        
        # Only process txt files
        if file[-4:] != '.txt':
            continue
        
        filepath_in = os.path.join(path_in, file)

        filename_out = file[:-4] + '.csv'
        filepath_out = os.path.join(path_out, filename_out)

        txt_to_csv(
            file_in=filepath_in,
            file_out=filepath_out,
            delimiter=delimiter,
            column_names=column_names,
            delete_txt=delete_txt
        )
        
    return path_out

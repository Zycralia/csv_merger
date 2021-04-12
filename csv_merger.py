import os
import glob
import pandas as pd
import numpy as np
from sklearn.impute import KNNImputer
import statistics 
from statistics import mode 

def rename_collumns(df,pattern):
    filename = str(df)
    filename = filename.removesuffix('.csv')   
    filename = filename.removeprefix(pattern)
    file = pd.read_csv(df, delimiter = '\t')
    col = file.columns
    newcol= ['timestamp','TimeUS']
    for c in col:
        if c != 'timestamp':
            if c != 'TimeUS':
                newcol.append(filename + "_" + c)
    file.columns = newcol
    return file

def get_most_common_length(all_filenames,length_list):
    for f in all_filenames:
        db = pd.read_csv(f, delimiter = '\t')
        length_list.append(len(db))
    mostcommonlength = mode(length_list)
    return mostcommonlength

def sort_data(all_filenames,mostcommonlength,length_list,interpolation_files,datareduce_files,okay_files):
    for f in all_filenames:
        db = pd.read_csv(f, delimiter = '\t')
        length_list.append(len(db))
        if (len(db) < mostcommonlength):
            interpolation_files.append(f)
        elif (len(db) > mostcommonlength):
            datareduce_files.append(f)
        else:
            okay_files.append(f)

def merge_standardlength_files(okay_files,pattern):
    for f in okay_files:
        file = rename_collumns(f,pattern)
        if 'combined_csv' in locals():
            del file['timestamp']
            combined_csv = pd.merge(combined_csv, file, how='left', on=['TimeUS'])
        else:
            combined_csv = file
    return combined_csv

def reduce_files(combined_csv,datareduce_files,pattern):
    for f in datareduce_files: 
        file = rename_collumns(f,pattern)
        indexlist = []
        for index, row in combined_csv.iterrows():
            timest = row['TimeUS']
            index = abs(file['TimeUS'] - timest).idxmin()
            indexlist.append(index)
        datareduced_file = file[file.index.isin(indexlist)]
        del datareduced_file['timestamp']
        del datareduced_file['TimeUS']
        datareduced_file.reset_index(drop=True, inplace=True)
        combined_csv = pd.merge(combined_csv, datareduced_file, how='outer', left_index=True, right_index=True)
        return combined_csv

def interpolate_shorter_files(combined_csv,interpolation_files,pattern):
    for f in interpolation_files: 
        indexlist = []
        interp_file = rename_collumns(f,pattern)
        del interp_file['timestamp']
        for index, row in interp_file.iterrows():
            timest = row['TimeUS']
            index = abs(combined_csv['TimeUS'] - timest).idxmin()
            indexlist.append(index)

        interp_file['new_index'] = indexlist
        interp_file.reset_index(inplace=True, drop = True)
        interp_file.set_index('new_index', inplace=True)
        del interp_file['TimeUS']
    
        combined_csv = pd.merge(combined_csv, interp_file, how='outer', left_index=True, right_index=True)

    combined_csv = combined_csv.interpolate(method = 'linear', limit_direction ='forward')
    combined_csv = combined_csv.interpolate(method = 'linear', limit_direction ='backward', limit = 1)
    return combined_csv

def main():
    #Enter Path to folder with all the csv files
    os.chdir(r"C:\Users\lisa_\Desktop\missiondata\mission12")
    #If there is a name pattern like my no_attacks_ you can enter here
    pattern = 'no_attacks_'
    extension = 'csv'

    all_filenames = [i for i in glob.glob('*.{}'.format(extension))]
    okay_files = []
    interpolation_files = []
    datareduce_files = []
    length_list= []
    
    mostcommonlength = get_most_common_length(all_filenames,length_list)
    sort_data(all_filenames,mostcommonlength,length_list,interpolation_files,datareduce_files,okay_files)
    glob_combined_csv = merge_standardlength_files(okay_files,pattern)
    glob_combined_csv = reduce_files(glob_combined_csv,datareduce_files,pattern)
    glob_combined_csv = interpolate_shorter_files(glob_combined_csv,interpolation_files,pattern)
    glob_combined_csv.to_csv(pattern + "combined_csv_interp.csv",sep='\t', index=False, encoding='utf-8-sig')

if __name__ == "__main__":
    main()

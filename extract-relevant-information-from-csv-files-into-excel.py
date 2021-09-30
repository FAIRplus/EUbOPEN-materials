import glob
import os

import pandas


path = "hek 0h"

os.chdir(path)
filenames = glob.glob(f"*Stats*.csv")
print(filenames)


keys_of_files = [i[:-4] for i in filenames]

## check for titles longer than 31 characters -- some applications may not be able to read the file
keys_of_files_shortened = list( key[:31] for key in keys_of_files )
if len(set(keys_of_files_shortened)) < len(keys_of_files):
    raise Exception


df_collect_all = None
for i, (filename_basename, filename_shortened) in enumerate(zip(keys_of_files, keys_of_files_shortened), start=1):
    filename = filename_basename + ".csv"
    
    print(f"Acting on file {i} of {len(keys_of_files)} ({filename})...")

    df = pandas.read_csv(filename)

    RECOGNIZE_RELEVANT_COLUMN_WITH_THIS_STRING = '] Count'
    column_names_which_contain_the_word_count = [col for col in df.columns if RECOGNIZE_RELEVANT_COLUMN_WITH_THIS_STRING in col]
    assert len(column_names_which_contain_the_word_count) == 1

    NEW_NAME_OF_RELEVANT_COLUMN = f"Cell_Count_{filename_shortened}"
    df_renamed = df.rename(columns={'[Cell] Count': NEW_NAME_OF_RELEVANT_COLUMN})
    print(df_renamed)

    MERGE_IF_THOSE_COLUMNS_ARE_EXACT_MATCHES = [
        #"ID" is not the same in all files...
        "WellID",
        "Row",
        "Column",
        "RowName",
        "ColumnName",
        "WellName",
        "DateTime",
        "Timepoint",
        "ElapsedTime",
        "Description",
    ]

    KEEP_THOSE_COLUMNS_INITIALLY = [
        #"ID" is not the same in all files...
        "WellID",
        "Row",
        "Column",
        "RowName",
        "ColumnName",
        "WellName",
        "DateTime",
        "Timepoint",
        "ElapsedTime",
        "Description"
    ]

    if df_collect_all is None:
        df_collect_all = df_renamed[KEEP_THOSE_COLUMNS_INITIALLY]

    for col in MERGE_IF_THOSE_COLUMNS_ARE_EXACT_MATCHES:
        for x,y in zip(df_collect_all[col].values, df_renamed[col].values):
            if pandas.isna(x) and pandas.isna(y):
                continue
            assert x == y, f"I expected that all tables would have the exactly same structure, but this is not the case: '{x}' != '{y}' "

    assert not NEW_NAME_OF_RELEVANT_COLUMN in df_collect_all.columns
    df_collect_all[NEW_NAME_OF_RELEVANT_COLUMN] = df_renamed[NEW_NAME_OF_RELEVANT_COLUMN]
    





print("Writing the file...")
df_collect_all.to_excel("outputcellcount.xlsx", index=False)
print("...done.")
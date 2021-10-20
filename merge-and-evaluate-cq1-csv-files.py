import glob
import os


import pandas 

WHICH_IMAGING = "CQ1-ctf011-t24"
DO_I_HAVE_TO_MERGE_FILES_FIRST = True
NAME_OF_COMPOUND_WHICH_IS_CONTROL = "DMSO"


def gather_csv_data_into_one_file(path_to_csv_files, output_filename = "output"):
    filenames = glob.glob(f"{path_to_csv_files}/*Stats*.csv")
    print(filenames)
    filenames = list([os.path.basename(f) for f in filenames])
    print(filenames)

    keys_of_files = [i[:-4] for i in filenames]

    ## check for titles longer than 31 characters -- some applications may not be able to read the file
    keys_of_files_shortened = list(key[:31] for key in keys_of_files)
    if len(set(keys_of_files_shortened)) < len(keys_of_files):
        raise Exception


    df_collect_all = None
    for i, (filename_basename, filename_shortened) in enumerate(zip(keys_of_files, keys_of_files_shortened), start=1):
        filename = filename_basename + ".csv"

        print(f"Acting on file {i} of {len(keys_of_files)} ({filename})...")

        df = pandas.read_csv(os.path.join(path_to_csv_files, filename))

        RECOGNIZE_RELEVANT_COLUMN_WITH_THIS_STRING = '] Count'
        column_names_which_contain_the_word_count = [col for col in df.columns if
                                                    RECOGNIZE_RELEVANT_COLUMN_WITH_THIS_STRING in col]
        assert len(column_names_which_contain_the_word_count) == 1

        #print(column_names_which_contain_the_word_count)

        WHAT_TO_PUT_IN_FRONT_OF_NEW_NAME_OF_RELEVANT_COLUMN = "Cell_Count_"
        new_name_of_relevant_column = f"{WHAT_TO_PUT_IN_FRONT_OF_NEW_NAME_OF_RELEVANT_COLUMN}{filename_shortened}"
        df_renamed = df.rename(columns={ column_names_which_contain_the_word_count[0]: new_name_of_relevant_column })
        #print(df_renamed)

        MERGE_IF_THOSE_COLUMNS_ARE_EXACT_MATCHES = [
            # "ID" is not the same in all files...
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
            # "ID" is not the same in all files...
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
            df_collect_all["well name"] = df_renamed["WellName"].str.replace("-","")


        for col in MERGE_IF_THOSE_COLUMNS_ARE_EXACT_MATCHES:
            for x, y in zip(df_collect_all[col].values, df_renamed[col].values):
                if pandas.isna(x) and pandas.isna(y):
                    continue
                assert x == y, f"I expected that all tables would have the exactly same structure, but this is not the case: '{x}' != '{y}' "

        assert not new_name_of_relevant_column in df_collect_all.columns
        df_collect_all[new_name_of_relevant_column] = df_renamed[new_name_of_relevant_column]


    print("Writing the file...")
    df_collect_all.to_excel(output_filename, index=False)
    print("...done.")

    return df_collect_all





#### --- GET THE COMPOUND IDENTIFIERS ---
df_identifier = pandas.read_excel("20210920 Overview CG plates and compounds.xlsx", sheet_name="ScarabEubopen ID")
#print(df.head())
#print("")

## store expanded compound maps
print("expanding the compound maps...")
df_experiments = pandas.read_excel("20210920 Overview CG plates and compounds.xlsx", sheet_name="experiment description")
compound_map_dict = {}
for see, _ in df_experiments.groupby("compound map see corresponding excel table"):
    print(f"Expanding compound map '{see}'...")

    df_compound_map = pandas.read_excel("20210920 Overview CG plates and compounds.xlsx", sheet_name=f"compound map {see}")
    df_compound_map.merge(df_identifier, how="left", on="compound name")
    #print(df_compound_map)

    # ### quality control: this block can be used to find non-matching compound names
    #  
    # for i, s in df_compound_map.iterrows():
    #     #print(i)
    #     #print(s.compound_name)
    #     result = df_identifier.query("`compound name` == '{compound_name}'".format(compound_name= s["compound name"]))
    #     if type(s["compound name"]) == int:
    #         result = df_identifier.query("`compound name` == {compound_name}".format(compound_name= s["compound name"]))
    #     #print(result)
    #     if len(result) != 1:
    #         print("ERROR: couldn't lookup the compound name '{compound_name}'".format(compound_name= s["compound name"]))

    compound_map_dict.update( {see: df_compound_map})


####

df_imagings = pandas.read_excel("20210920 Overview CG plates and compounds.xlsx", sheet_name="imaging campaigns")

## select only a subset of imagings to evaluate...
## comment out to act on all entries (or change to act on different subset)
df_imagings = df_imagings[df_imagings["imaging ID"] == WHICH_IMAGING]


df_imagings = df_imagings.merge(df_experiments, on="experiment ID")

df_collector = []
for groupname, groupentries in df_imagings.groupby("experiment ID"):
    print(groupname)

    print("processing the imagings...")
    for i, s in groupentries.iterrows():
        see = s["compound map see corresponding excel table"]
        df_compound_map = compound_map_dict[see].copy()

        ## append all metadata columns but specified ones to all entries
        exclude_columns= [  "compound map see corresponding excel table", 
                            "imaged in instrument",
                            "raw data available in zip file",
                            "processed images available in folder",
                            "csv files available in folder",
                            ]
        for col in s.index:
            if not col in exclude_columns:
                df_compound_map.loc[:, col ]       = s[col]
        
        assert not pandas.isna(s["csv files available in folder"])
        
        merged_filename = os.path.join(s["csv files available in folder"], "merged.xlsx")
        
        ## select whether or not you want to do this:
        if DO_I_HAVE_TO_MERGE_FILES_FIRST:
            where_are_the_csv_files = os.path.join(s["csv files available in folder"], "Reports")
            gather_csv_data_into_one_file(where_are_the_csv_files, merged_filename)

        ## continue here:
        df = pandas.read_excel(merged_filename)
        df = df.merge(df_compound_map, how="left", left_on="WellID", right_on="well ID")

        df_rows_which_are_control_wells = df[ df["compound name"] == NAME_OF_COMPOUND_WHICH_IS_CONTROL ]
        mean_values_for_control_rows = cf = df_rows_which_are_control_wells.mean()

        for x, y in [["calc", df], ["ctrl", cf]] :
            df[f"{x}1a"] = y["Cell_Count_Cell_Stats_HighIntObj"]          / y["Cell_Count_Cell_Stats"]
            df[f"{x}1b"] = y["Cell_Count_Cell_Stats_Normal"]              / y["Cell_Count_Cell_Stats"]
         
            df[f"{x}2a"] = y["Cell_Count_Cell_Stats_HealthyNuc"]          / y["Cell_Count_Cell_Stats"]
            df[f"{x}2b"] = y["Cell_Count_Cell_Stats_FragNuc"]             / y["Cell_Count_Cell_Stats"]
            df[f"{x}2c"] = y["Cell_Count_Cell_Stats_PyknoNuc"]            / y["Cell_Count_Cell_Stats"]
         
            df[f"{x}3a"] = y["Cell_Count_Cell_Stats_mitosis"]             / y["Cell_Count_Cell_Stats"]
            df[f"{x}3b"] = y["Cell_Count_Cell_Stats_apoptosis"]           / y["Cell_Count_Cell_Stats"]

            df[f"{x}4a"] = y["Cell_Count_Cell_Stats_membrane intact"]     / y["Cell_Count_Cell_Stats"]
            df[f"{x}4b"] = y["Cell_Count_Cell_Stats_membrane permeab"]    / y["Cell_Count_Cell_Stats"]
  
            df[f"{x}5a"] = y["Cell_Count_Cell_Stats_mito mass high"]      / y["Cell_Count_Cell_Stats"]
            df[f"{x}5b"] = y["Cell_Count_Cell_Stats_mito mass normal"]    / y["Cell_Count_Cell_Stats"]
            
            df[f"{x}6a"] = y["Cell_Count_Cell_Stats_tubulin effect"]      / y["Cell_Count_Cell_Stats"]
            df[f"{x}6b"] = y["Cell_Count_Cell_Stats_tubulin normal"]      / y["Cell_Count_Cell_Stats"]


        df.to_excel(merged_filename + "_evaluated.xlsx", index=False)
        
        df_collector.append(df)

print("Writing all results into one file...")
pandas.concat(df_collector).to_excel("cq1_evaluated_all.xlsx", index=False)

print("done.")

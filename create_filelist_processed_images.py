import pandas

ROOT_FOLDERNAME_WITHIN_SUBMISSION_SYSTEM = ""



df_identifier = pandas.read_excel("20210920 Overview CG plates and compounds.xlsx", sheet_name="ScarabEubopen ID")
#print(df.head())
#print("")

## Quality Control
df = df_identifier
for col in df.columns:
    if df[col].count() != df[col].nunique():
        print(f"There are duplications in the {col} column:")
        for name in df[col].unique():
            entries_matching_name = df[df[col] == name]
            if len(entries_matching_name.index) != 1:
                #print(name)
                #print(entries_matching_name)
                pass


df_experiments = pandas.read_excel("20210920 Overview CG plates and compounds.xlsx", sheet_name="experiment description")
## do only CQ1
df_experiments = df_experiments[df_experiments["experiment ID"].str.contains("CQ1-ctf")]


## store expanded compound maps
print("expanding the compound maps...")
compound_map_dict = {}
for see, _ in df_experiments.groupby("compound map see corresponding excel table"):

    print(f"Checking the compound map '{see}'...")

    df_compound_map = pandas.read_excel("20210920 Overview CG plates and compounds.xlsx", sheet_name=f"compound map {see}")

    ## expand with lookup-ed ID
    for i, s in df_compound_map.iterrows():
        #print(i)
        #print(s.compound_name)

        if pandas.isna(s["compound name"]):
            continue 

        result = df_identifier.query("`compound name` == '{compound_name}'".format(compound_name= s["compound name"]))
        if type(s["compound name"]) == int:
            result = df_identifier.query("`compound name` == {compound_name}".format(compound_name= s["compound name"]))
        #print(result)
        #assert len(result) == 1, (s, result)
        if len(result) == 1:
            #print(dff.loc[i])
            df_compound_map.loc[i, "SGC ID"]        = result.squeeze()["SGC ID"]
            df_compound_map.loc[i, "EUbOPEN ID"]    = result.squeeze()["EUbOPEN ID"]
            df_compound_map.loc[i, "SMILES"]        = result.squeeze()["SMILES"]
        else:
            print("ERROR: couldn't lookup the compound name '{compound_name}'".format(compound_name= s["compound name"]))

    compound_map_dict.update( {see: df_compound_map})



df_imagings = pandas.read_excel("20210920 Overview CG plates and compounds.xlsx", sheet_name="imaging campaigns")
## do only CQ1
df_imagings = df_imagings[df_imagings["experiment ID"].str.contains("CQ1-ctf")]

df_imagings = df_imagings.merge(df_experiments, on="experiment ID")

df_collector = []
for groupname, groupentries in df_imagings.groupby("experiment ID"):
    print(groupname)

    print("processing the imagings...")
    for i, s in groupentries.iterrows():
        assert not pandas.isna(s["processed images available in folder"])
        
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
                

        for x, y in df_compound_map.iterrows():
            
            df_compound_map.loc[x, "Files"]  =   ROOT_FOLDERNAME_WITHIN_SUBMISSION_SYSTEM +      \
                                        s["processed images available in folder"] + "/" +   \
                                        y["well name"][0] + "-" + y["well name"][1:] + "_"+ \
                                        "F0001_T0001_Z0001.png"
            

        ## first column has to be "Files"
        columns = list(df_compound_map.columns)
        columns.remove("Files")
        columns.insert(0, "Files")
        df_compound_map = df_compound_map[columns]

        df_collector.append(df_compound_map)

df_output = pandas.concat(df_collector)

print("writing...")
df_output.to_csv(  f"filelist_processed_images.csv",  index=False)
df_output.to_excel(f"filelist_processed_images.xlsx", index=False)

print("done.")
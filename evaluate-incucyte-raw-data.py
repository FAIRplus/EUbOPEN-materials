#evaluate-incucate-raw-data.py
import numpy 
import pandas

FILENAME_OF_RAW_DATA = "incucyte_raw_data/cv001.csv"
SHEETNAME_OF_COMPOUND_MAP = "compound map Incucyte1"
FILENAME_OF_META_DATA = "20210920 Overview CG plates and compounds.xlsx"
SEPARATOR_CHARACTER = ";"
DECIMAL_SEPARATOR   = ","





## quality control
with open(FILENAME_OF_RAW_DATA) as f:
    assert f.readline().replace(SEPARATOR_CHARACTER,"") != "\n"
    assert f.readline().replace(SEPARATOR_CHARACTER,"") == "\n"
    assert f.readline().split(":")[0] == "Vessel Name"
    assert f.readline().split(":")[0] == "Metric"
    assert f.readline().split(":")[0] == "Cell Type"
    assert f.readline().split(":")[0] == "Passage"
    assert f.readline().split(":")[0] == "Notes"
    assert f.readline().split(":")[0] == "Analysis"
    assert f.readline().replace(SEPARATOR_CHARACTER,"") == "\n"
    assert f.readline().replace(SEPARATOR_CHARACTER,"").startswith("Date TimeElapsed")
    remaining_lines = f.readlines()
    

## read
name_of_file = pandas.read_csv(FILENAME_OF_RAW_DATA, sep=SEPARATOR_CHARACTER, decimal=DECIMAL_SEPARATOR, nrows=1, names=["name"], usecols=[0])
metadata     = pandas.read_csv(FILENAME_OF_RAW_DATA, sep=SEPARATOR_CHARACTER, decimal=DECIMAL_SEPARATOR, skiprows=2, nrows=6, names=["meta"], usecols=[0])
df           = pandas.read_csv(FILENAME_OF_RAW_DATA, sep=SEPARATOR_CHARACTER, decimal=DECIMAL_SEPARATOR, skiprows=9)

well_ids_with_data = list([col for col in df.columns if col != "Date Time" and col != "Elapsed"])
number_of_timepoints = len(df)
timepoint_columns = list([x for x in range(number_of_timepoints)])
#print(well_ids_with_data)


## read
df_compound_map = pandas.read_excel(FILENAME_OF_META_DATA, sheet_name=SHEETNAME_OF_COMPOUND_MAP)
#print(df_compound_map)


## treat

## "second" measurement is actual t0; the point before was a "blank"
df["Elapsed"] = df["Elapsed"] - df["Elapsed"].iloc[1]

## transpose for better access
df = df.transpose()
df_timings = df.loc[["Date Time", "Elapsed"],:]
df = df.drop(index = ["Date Time", "Elapsed"])
df["well name"] = df.index

## deeper quality control
# catch value == 0, because this would yield invalid log2 transforms...
problem = df[timepoint_columns].applymap(lambda x: True if x == 0 else False)
if problem.any(axis="columns").any():
    print("---------------------------------")
    print("THERE IS A PROBLEM ('datapoint==0') IN YOUR DATA: ")
    print(df[problem.any(axis="columns")])
    print("---------------------------------")


## map compounds
df = df.merge(df_compound_map, on="well name", how="left")


## create control = base line values
selector = df["compound name"] == "DMSO"
assert (df[selector]["concentration"] == 10.).all()
control     = df[selector][timepoint_columns].mean()
control_std = df[selector][timepoint_columns].std()


## calculate growth via Amelie's formula
df_vs_blank = df[timepoint_columns].div(df[0], axis="index")
df_vs_blank_log2 = df_vs_blank.applymap(numpy.log2)
#print(df_vs_blank_log2)

control_vs_blank = control.div(control[0])
control_vs_blank_log2 = control_vs_blank.apply(numpy.log2)
#print(control_vs_blank_log2)

df_vs_blank_log2_div_control_vs_blank_log2 = df_vs_blank_log2.div(control_vs_blank_log2)
df_vs_blank_log2_div_control_vs_blank_log2_derived = df_vs_blank_log2_div_control_vs_blank_log2.applymap(lambda x: 2**x-1)
#print(df_vs_blank_log2_div_control_vs_blank_log2_derived)


final = df_vs_blank_log2_div_control_vs_blank_log2_derived.copy()
final[ [c for c in df.columns if not c in timepoint_columns] ] = df[ [c for c in df.columns if not c in timepoint_columns] ] 

print(final)
final.to_excel("incucyte-output.xlsx")

## groupby 
#for groupname, groupseries in final.groupby(["eubopen ID", "concentration"]):
#    print(groupname)


## look into negative control performance (DMSO)
# print(final[ final["compound name"]=="DMSO" ])
# import matplotlib.pyplot as plt 
# for i, s in final[ final["compound name"]=="DMSO" ].iterrows():
#     s[timepoint_columns].plot(label=s["well name"])
# plt.legend()
# plt.show()


## look into a specific compound by EUbOPEN ID
# eubopen_id = "EUB0000502a"
# print(final[ final["eubopen ID"]==eubopen_id ])
# import matplotlib.pyplot as plt 
# for i, s in final[ final["eubopen ID"]==eubopen_id ].iterrows():
#     s[timepoint_columns].plot(label=s["well name"])
# plt.legend()
# plt.show()

## look into all compounds
# import matplotlib.pyplot as plt 
# for i, s in final.iterrows():
#     s[timepoint_columns].plot() #label=s["well name"])
# ax = plt.gca()
# ax.set_ylim(0,2)
# plt.show()

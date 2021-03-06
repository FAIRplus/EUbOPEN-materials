import csv
import datetime
import glob
import os

import openpyxl
import lxml # actually, the import of lxml is not necessary for the code to work, but is necessary to really enable the write_only function in the next line. If lxml is not present, the memory-optimization effect of write_only does not happen, but openpyxl works as if the write_only flag was not set.


path = "hek 0h"

os.chdir(path)
filenames = glob.glob(f"*.csv")
print(filenames)

print("Counting lines...")
linecount_total = 0
for f in filenames:
    print(f"Counting lines in file {f}...")
    file = open(f, "r")
    for line in file:
        linecount_total += 1
    file.close()

keys_of_files = [i[:-4] for i in filenames]


wb = openpyxl.Workbook(write_only=True)
processed_lines = 0
notify_x_times = 100

notification_stops = [int(n*1/float(notify_x_times)*float(linecount_total)) for n in range(1,notify_x_times+1) ]
notification_stops += [1]

## check for titles longer than 31 characters -- some applications may not be able to read the file
keys_of_files_shortened = list( key[:31] for key in keys_of_files )
if len(set(keys_of_files_shortened)) < len(keys_of_files):
    raise Exception

for i, (key, title) in enumerate(zip(keys_of_files, keys_of_files_shortened), start=1):
    print(f"Acting on file {i} of {len(keys_of_files)} ({key})...")
    f = open(f'{key}.csv')
    reader = csv.reader(f)

    ws = wb.create_sheet(title=title)
   
    for this_row in reader:

        row = []

        for element in this_row:
            assert type(element) == str 
            
            can_be_int_ed = None
            can_be_float_ed = None 
            can_be_datetime_ed = None 

            try:
                int(element)
                can_be_int_ed = True
            except ValueError:
                can_be_int_ed = False
            
            try:
                float(element)
                can_be_float_ed = True
            except ValueError:
                can_be_float_ed = False

            if "-" in element and ":" in element and ":" in element:
                try:
                    datetime.datetime.fromisoformat(element)
                    can_be_datetime_ed = True 
                except ValueError:
                    can_be_datetime_ed = False

            if can_be_int_ed and can_be_float_ed:
                if str(int(element)) == element:
                    element = int(element) 
                else:
                    element = float(element)

            if can_be_float_ed and not can_be_int_ed:
                element = float(element)
            
            if can_be_datetime_ed:
                pass

            row.append(element)

        ws.append(row)
        processed_lines += 1

        if processed_lines in notification_stops:
            print(f"Processed {processed_lines} of {linecount_total} lines ({int(float(processed_lines)/linecount_total*100)}%)...")

print("Writing the file...")
wb.save(filename = f"output.xlsx")
print("...done.")
# This scripts is designed to identify missing or incomplete information in the existing OPIC spreadsheet-based database
# Given the missing information, the database can be optimized for storage in a SQL-based database

import pandas as pd

# load CSV into a DF
master_csv = pd.read_csv('OGS_OCDB/DB_master.csv', low_memory = False)
df_master = pd.DataFrame(master_csv)

# Select needed columns: many are not needed for this purpose
df_clean = df_master[['File #', 'Box', 'Total', 'API', 'Operator', 
                    'Lease', 'Well #', 'Top', 'Bottom', 'Type', 'Comments']]

# FLAG: wells with missing API
bad_api = [[], []]

# FLAG: wells with incomplete level box data
no_boxes = []


# Extract file #s with bad API entries
for file in df_clean['File #'].unique():
	sub_df = df_clean[df_clean['File #'] == file]

	if not (sub_df['API'].astype(str).iloc[0].isdecimal()):
		bad_api[0].append(sub_df['File #'].iloc[0])
		bad_api[1].append(sub_df['API'].iloc[0])




#################################### OUTPUT

# DF for all bad APIs
print("Bad API:")

data1 = {'file' : bad_api[0], 'api entry' : bad_api[1]}
api_df = pd.DataFrame(data = data1)


with pd.option_context('display.max_rows', None):
	print(api_df)

# Grouped DF
# replace "changed to..." values for better grouping
# '#' prefix to indicate entry values not originally present
api_df['api entry'].replace(to_replace = ".hanged.*", value = "#Reassigned",
							regex = True, inplace = True)
api_df['api entry'].replace(to_replace = ".*(Combined|.dded).*", value = "#Combined into other file",
							regex = True, inplace = True)
api_df['api entry'].replace(to_replace = ".*isposal.*", value = "#disposal: no API",
							regex = True, inplace = True)

print("\nBad API Summary: ")
print(api_df['api entry'].value_counts())

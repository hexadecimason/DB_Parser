# This script is designed to identify missing or incomplete information in the existing OPIC spreadsheet-based database
# Given the missing information, the database can more effectively be parsed into a PgSQL database

import pandas as pd
pd.set_option('display.max_columns', 20)
pd.set_option('display.width', 1000)

# load CSV into a DF
master_csv = pd.read_csv('OGS_OCDB/DB_master.csv', low_memory = False)
df_master = pd.DataFrame(master_csv)

# Select needed columns: many are not needed for this purpose
df_clean = df_master[['File #', 'Box', 'Total', 'API', 'Operator', 
                    'Lease', 'Well #', 'Top', 'Bottom', 'Type', 'Comments']]

##############################################################################
# FLAG: wells with missing API
# [0] = array of file #s
# [1] = array of APIs/API descriptors where no API exists
bad_api = [[], []]
good_api_files = []

# Extract file #s with bad API entries
for file in df_clean['File #'].unique():
	sub_df = df_clean[df_clean['File #'] == file]

	# find bad apis and their corresponding file
	if not (sub_df['API'].astype(str).iloc[0].isdecimal()):
		bad_api[0].append(sub_df['File #'].iloc[0])
		bad_api[1].append(sub_df['API'].iloc[0])
	# filter out good api files
	else:
		good_api_files.append(sub_df['File #'].iloc[0])


# OUTPUT for bad APIs

print("Bad API:")

# DF for all bad APIs
data1 = {'file' : bad_api[0], 'api entry' : bad_api[1]}
bad_api_df = pd.DataFrame(data = data1)


with pd.option_context('display.max_rows', None):
	print(bad_api_df)

# Grouped DF
# replace "changed to..." values for better grouping
# '#' prefix to indicate entry values not originally present
bad_api_df['api entry'].replace(to_replace = ".hanged.*", value = "## Reassigned",
							regex = True, inplace = True)
bad_api_df['api entry'].replace(to_replace = ".*(Combined|.dded).*", value = "## Combined into other file",
							regex = True, inplace = True)
bad_api_df['api entry'].replace(to_replace = ".*isposal.*", value = "## disposal: no API",
							regex = True, inplace = True)
bad_api_df['api entry'].fillna('## empty field/NaN', inplace=True)

print("\nBad API Summary: ")
print(bad_api_df['api entry'].value_counts())
print("TOTAL: ", sum(bad_api_df['api entry'].value_counts()))
##############################################################################

# Clean DB so each row is a box, APIs aren't missing, and file numbers are appropriate
# Note: several global variables are used for DataFrames to allow for main() and other functions to share references

import pandas as pd
import numpy as np
pd.set_option('display.max_columns', 20)
pd.set_option('display.width', 1000)

cleanMasterDates = False
resetAPIs = False

##############################################################################

def main():

	# load CSV into a DF
	df_master = pd.read_csv('data/DB_master.csv', low_memory = False)

	# Note: some global variables are used for DataFrames to allow 
	# for main() and other functions to share references without extra parameters
	global sub_df # df for subsetting by file
	global cleanfile # a cleaned whole file for appending to the final DataFrame

	# Excel auto-formatting can cut off API values
	# this saves a corrected file
	global resetAPIs
	if resetAPIs:
		ocdb_df = pd.read_csv('data/XLS_backup.csv', low_memory = False)

		for file in df_master['File #'].unique():
			print("fixing API value: ", file)
			api = ocdb_df[ocdb_df['File #'] == file]['API'].iloc[0]
			df_master.loc[df_master['File #'] == file, 'API'] = api

		df_master.to_csv('data/DB_master_cleanAPIs.csv', index = False)
		print('saved new file with fixed auto-formatted API values')
		print('verify corrections and reassign this file to be the new master file')
		exit()

	# dates in master file could become auto-formatted
	# this saves a corrected file
	global cleanMasterDates
	if cleanMasterDates:
		print('fixing well number formatting...')
		df_master['Well #'] = df_master['Well #'].apply(parseWellNum)
		df_master.to_csv('data/DB_master_cleandates.csv', index = False)

		print("saved new file with fixed auto-formatted well numbers")
		print('verify corrections and reassign this file to be the new master file')
		exit()

	# FILTER DF FOR GOOD APIs
	df_master = df_master.dropna(subset = 'API') # drop empty values
	filtered = df_master[df_master['API'].str.isdecimal()] # drop text entries

	# FILTER FOR "A/C" files
	filtered = filtered[filtered['File #'] != 'A/C']

	# save df where box fields are null
	nullboxes_df = filtered[filtered['Box'].isna() & filtered['Total'].isna()]
	nullboxes_df.to_csv('data/nullboxes.csv', index = False)
	print('saved null box data')

	# FILTER FOR DOUBLE-NULLS
	filtered = filtered.dropna(subset = ['Box', 'Total'])

	# VERIFY TYPES - capital prefixes (Int vs int) allow for pd.NaN values
	filtered['Box'] = filtered['Box'].astype('Int32')
	filtered['Total'] = filtered['Total'].astype('Int32')
	filtered['API'] = filtered['API'].astype('Int64')
	filtered['Sec'] = filtered['Sec'].astype('Int32')
	filtered['Tw'] = filtered['Tw'].astype('Int32')
	filtered['Rg'] = filtered['Rg'].astype('Int32')

	# FIX MISSING BOX DATA

	# List of files to unpack
	file_list = filtered['File #'].unique()

	clean_df = pd.DataFrame(columns = filtered.columns.values.tolist())

	for file in file_list: #'file' runs through each file

		# subset for each file, set up a blank DF fro cleaned file
		sub_df = filtered[filtered['File #'] == file]
		cleanfile = pd.DataFrame(columns = sub_df.columns.values.tolist())
		boxes_added = 0 # tracks boxes in a file

		# index through rows of each row in sub_df
		# file_row represents either a box or a set of boxes
		for file_row in range(len(sub_df)):

			boxes_in_row = -1
			boxes_in_file = -1
			row_top = sub_df['Top'].iloc[file_row]
			row_bottom = sub_df['Bottom'].iloc[file_row]

			boxNull = pd.isnull(sub_df['Box'].iloc[file_row]) and pd.notnull(sub_df['Total'].iloc[file_row])
			totalNull = pd.isnull(sub_df['Total'].iloc[file_row]) and pd.notnull(sub_df['Box'].iloc[file_row])
			noNull = pd.notnull(sub_df['Box'].iloc[file_row]) and pd.notnull(sub_df['Total'].iloc[file_row])
			
			# determine number of boxes represented by row		
			if boxNull: boxes_in_row = sub_df['Total'].iloc[file_row] 
			elif totalNull: boxes_in_row = sub_df['Box'].iloc[file_row] 
			elif noNull: boxes_in_row = 1

			# adds all boxes represented by individual row
			for box in range(boxes_in_row):

				if box == 0: boxtop = row_top
				else: boxtop = np.NaN

				if box == boxes_in_row - 1: boxbottom = row_bottom
				else: boxbottom = np.NaN

				addBox(file_row, boxes_added + 1, boxes_in_file, boxtop, boxbottom)
				boxes_added += 1

		# determine total boxes in file
		boxes_in_file = len(cleanfile)
		cleanfile = cleanfile.assign(Total = boxes_in_file)

		# add cleanfile to the aggreagate cleaned Dataframe
		print("cleaned file: ", cleanfile['File #'].iloc[0])
		clean_df = pd.concat([clean_df, cleanfile], ignore_index = True)

	# SAVE TO FILE
	print("saving CSV file...")
	clean_df.to_csv('data/cleaned.csv', index = False)

##############################################################################

# Autoformatted number as date -> original format
def parseWellNum(value):
	wellNum = ''
	terms = str(value).split('-')

	if len(terms) != 2: return value

	months = {'Jan':1, 'Feb':2, 'Mar':3, 'Apr':4, 'May':5, 'Jun':6,
	'Jul':7, 'Aug':6, 'Sep':9, 'Oct':10, 'Nov':11, 'Dec':12}

	if terms[0] in months:
		wellNum = str(months[terms[0]]) + '-' + str(terms[1])
	elif terms[1] in months:
		wellNum = str(months[terms[1]]) + '-' + str(terms[0])

	return wellNum

# must insert escape characters to comments where " or ' exist
def parseComment(com):
	s = str(com)
	s = s.replace('"', '\\\"').replace("'", "\\\'")
	return s

# given an index in sub_df and box-level specifics,
# adds box-level data to the cleaned file DataFrame
def addBox(i, boxNum, boxTotal, boxTop, boxBottom):

	global sub_df
	global cleanfile

	# zip columns 
	row_add = dict(zip(sub_df.columns.values.tolist(),
		[sub_df['File #'].iloc[i],
		boxNum, # 'Box'
		boxTotal, # 'Total'
		sub_df['Location'].iloc[i],
		sub_df['API'].iloc[i],
		sub_df['Operator'].iloc[i],
		sub_df['Lease'].iloc[i],
		sub_df['Well #'].iloc[i],
		sub_df['Sec'].iloc[i],
		sub_df['Tw'].iloc[i],
		sub_df['TwD'].iloc[i],
		sub_df['Rg'].iloc[i],
		sub_df['RgD'].iloc[i],
		sub_df['Quarter'].iloc[i],
		sub_df['Latitude'].iloc[i],
		sub_df['Longitude'].iloc[i],
		sub_df['County'].iloc[i],
		sub_df['State'].iloc[i],
		sub_df['Formation'].iloc[i],
		sub_df['Field'].iloc[i],
		boxTop, #'Top'
		boxBottom, #'Bottom'
		sub_df['Type'].iloc[i],
		sub_df['Box Type'].iloc[i],
		sub_df['Condition'].iloc[i],
		sub_df['Diameter'].iloc[i],
		sub_df['Restrictions'].iloc[i],
		parseComment(sub_df['Comments'].iloc[i]) ]))

	cleanfile = pd.concat([cleanfile, pd.DataFrame([row_add])], ignore_index = True)

##############################################################################

main()
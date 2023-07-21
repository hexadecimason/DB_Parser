# Clean DB so each row is a box, APIs aren't missing, and file numbers are appropriate
# Note: several global variables are used for DataFrames to allow for main() and other functions to share references

import pandas as pd
import numpy as np
pd.set_option('display.max_columns', 20)
pd.set_option('display.width', 1000)

# load CSV into a DF
master_csv = pd.read_csv('data/DB_master.csv', low_memory = False)
df_master = pd.DataFrame(master_csv)

sub_df = pd.DataFrame()
clean_df = pd.DataFrame(columns = df_master.columns.values.tolist())
cleanfile = pd.DataFrame(columns = df_master.columns.values.tolist())
nullboxes_df = pd.DataFrame(columns = df_master.columns.values.tolist())

cleanMasterDates = False
separateNullBoxes = False

##############################################################################

def main():

	# Note: several global variables are used for DataFrames to allow for main() and other functions to share references
	global df_master # original CSV as a DF
	global sub_df # df for expanding each potential cluster of boxes
	global cleanfile # a cleaned whole file for appending to clean_df
	global clean_df # final cleaned data
	global nullboxes_df # separate file for wells with null box entries: most are chips

	# dates in master file could become auto-formatted
	# this saves a corrected file if we need it
	global cleanMasterDates
	if cleanMasterDates:
		df_master['Well #'] = df_master['Well #'].apply(parseWellNum)
		df_master.to_csv('data/DB_master_cleandates.csv', index = False)

	# FILTER DF FOR GOOD APIs
	df_master = df_master.dropna(subset = 'API') # drop empty values
	filtered = df_master[df_master['API'].str.isdecimal()] # drop text entries

	# FILTER FOR "A/C files"
	filtered = filtered[filtered['File #'] != 'A/C']

	# save df where box fields are null
	if separateNullBoxes:
		nullboxes_df = filtered[filtered['Box'].isna() & filtered['Total'].isna()]
		nullboxes_df.to_csv('data/nullboxes.csv', index = False)
		print('saved null box data')

	# FILTER FOR DOUBLE-NULLS
	filtered = filtered.dropna(subset = ['Box', 'Total'])

	# CONVERT SOME TO INT
	filtered['Box'] = filtered['Box'].astype('Int64')
	filtered['Total'] = filtered['Total'].astype('Int64')
	filtered['API'] = filtered['API'].astype('Int64')
	filtered['Sec'] = filtered['Sec'].astype('Int32')
	filtered['Tw'] = filtered['Tw'].astype('Int32')
	filtered['Rg'] = filtered['Rg'].astype('Int32')

	# FIX MISSING BOX DATA

	# List of files to unpack
	files = filtered['File #'].unique()

	for f in range(len(files)): #'f' runs through each file

		# subset for each file
		sub_df = filtered[filtered['File #'] == files[f]]
		cleanfile = pd.DataFrame(columns = cleanfile.columns.values.tolist())
		boxes_added = 0 # tracks boxes in a file

		# sort through rows of each sub_df
		for b in range(len(sub_df)): #'b' runs through each line (either a box, or cluster of boxes to be unpacked)

			boxes_to_add = -1
			file_total = -1
			_top = sub_df['Top'].iloc[b]
			_bottom = sub_df['Bottom'].iloc[b]

			boxNull = pd.isnull(sub_df['Box'].iloc[b]) and pd.notnull(sub_df['Total'].iloc[b])
			totalNull = pd.isnull(sub_df['Total'].iloc[b]) and pd.notnull(sub_df['Box'].iloc[b])
			noNull = pd.notnull(sub_df['Box'].iloc[b]) and pd.notnull(sub_df['Total'].iloc[b])
		
			if boxNull: boxes_to_add = sub_df['Total'].iloc[b] 
			elif totalNull: boxes_to_add = sub_df['Box'].iloc[b] 
			elif noNull: boxes_to_add = 1

			for box in range(boxes_to_add):

					if box == 0: boxtop = _top
					else: boxtop = np.NaN

					if box == boxes_to_add - 1: boxbottom = _bottom
					else: boxbottom = np.NaN

					#print("box: ", boxes_added + 1)
					addBox(b, boxes_added + 1, file_total, boxtop, boxbottom)
					boxes_added += 1

		file_total = len(cleanfile)
		cleanfile = cleanfile.assign(Total = file_total)
		print("cleaned file: ", cleanfile['File #'].iloc[0])
		clean_df = pd.concat([clean_df, cleanfile], ignore_index = True)

	# SAVE TO FILE
	clean_df.to_csv('data/cleaned.csv', index = False)

##############################################################################

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


def addBox(i, boxNum, boxTotal, boxTop, boxBottom):

	global sub_df
	global cleanfile

	# file|box|total|loc|API|op|lease|Well#|S|T|Td|R|Rd|Q|
	row_add = dict(zip(sub_df.columns.values.tolist(),
		[sub_df['File #'].iloc[i],
		boxNum, # 'Box'
		boxTotal, # 'Total'
		sub_df['Location'].iloc[i],
		sub_df['API'].iloc[i],
		sub_df['Operator'].iloc[i],
		sub_df['Lease'].iloc[i],
		parseWellNum(sub_df['Well #'].iloc[i]),
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
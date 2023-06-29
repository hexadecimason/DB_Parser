# Clean DB so each row is a box, APIs aren't missing, and file numbers are appropriate

import pandas as pd
import numpy as np
pd.set_option('display.max_columns', 20)
pd.set_option('display.width', 1000)


# load CSV into a DF
master_csv = pd.read_csv('OGS_OCDB/DB_master_modified.csv', low_memory = False)
df_master = pd.DataFrame(master_csv)



'''
situations to sort through:

	- 'API' is not an API'
			> omit: these entries are of no use
	- 'file #' is == 'A/C', indicating core is actually in the Amoco collection
			> nothing to do about these until core is physically moved
	- 'total boxes' are in 'box #' and 'total boxes' is NaN
			> reassign to 'total', create rows for every box and give it a number
	- 'total boxes' present and 'box #' contains a range'
			> use regex to determine 
'''


# FILTER DF FOR GOOD APIs
df_master = df_master.dropna(subset = 'API') # drop empty values
filtered_apis = df_master[df_master['API'].str.isdecimal()] # drop text entries

# FILTER FOR "A/C files"
filtered_ac = filtered_apis[filtered_apis['File #'] != 'A/C']

# CONVERT 'Box' TO INT
filtered_ac['Box'] = filtered_ac['Box'].astype('Int64')
filtered_ac['Total'] = filtered_ac['Total'].astype('Int64')
filtered_ac['API'] = filtered_ac['API'].astype('Int64')


# FIX MISSING BOX DATA
# create a new DF and add data row-by-row, creating new rows when needed to individualize ranges of boxes
clean_df = pd.DataFrame(columns = df_master.columns.values.tolist())

# List of files to unpack
files = filtered_ac['File #'].unique()

for f in range(len(files)): #'f' runs through each file

	# subset for each file
	sub_df = filtered_ac[filtered_ac['File #'] == files[f]]
	boxes_added = 0 # tracks boxes in a file

	# sort through rows of each sub_df
	for b in range(len(sub_df)): #'b' runs through each line (either a box, or cluster of boxes to be unpacked)

		### CASE: 'Box' entry is null, 'Total' contains boxes boxes per formation
		
		if pd.isnull(sub_df['Box'].iloc[b]) and pd.notnull(sub_df['Total'].iloc[b]):

			boxes_to_add = sub_df['Total'].astype(int).iloc[b] # boxes to add per Fm
			file_total = sum(sub_df['Total']) # boxes for each file
			fm_top = sub_df['Top'].iloc[b] # known top for the formation
			fm_bottom = sub_df['Bottom'].iloc[b] # known bottom for the formation

			for box in range(boxes_to_add):

				if box == 0: boxtop = fm_top
				else: boxtop = np.NaN

				if box == boxes_to_add - 1: boxbottom = fm_bottom
				else: boxbottom = np.NaN

				# file|box|total|loc|API|op|lease|Well#|S|T|Td|R|Rd|Q|
				row_add = dict(zip(sub_df.columns.values.tolist(),
					[sub_df['File #'].iloc[b],
					boxes_added + 1, # 'Box'
					file_total, # 'Total'
					sub_df['Location'].iloc[b],
					sub_df['API'].iloc[b],
					sub_df['Operator'].iloc[b],
					sub_df['Lease'].iloc[b],
					sub_df['Well #'].iloc[b],
					sub_df['Sec'].iloc[b],
					sub_df['Tw'].iloc[b],
					sub_df['TwD'].iloc[b],
					sub_df['Rg'].iloc[b],
					sub_df['RgD'].iloc[b],
					sub_df['Quarter'].iloc[b],
					sub_df['Latitude'].iloc[b],
					sub_df['Longitude'].iloc[b],
					sub_df['County'].iloc[b],
					sub_df['State'].iloc[b],
					sub_df['Formation'].iloc[b],
					sub_df['Field'].iloc[b],
					boxtop, #'Top'
					boxbottom, #'Bottom'
					sub_df['Type'].iloc[b],
					sub_df['Box Type'].iloc[b],
					sub_df['Condition'].iloc[b],
					sub_df['Diameter'].iloc[b],
					sub_df['Restrictions'].iloc[b],
					sub_df['Comments'].iloc[b] ]))

				df_add = pd.DataFrame([row_add])
				clean_df = pd.concat([clean_df, df_add], ignore_index = True)
				boxes_added += 1

				########################################################
				print("added row: ", row_add['File #'], ' | ', row_add['Box'], ' | ', row_add['Top'], ' | ', row_add['Bottom'])
				########################################################
				


		### CASE: 'Total' is null and 'Box' is integer
		elif pd.isnull(sub_df['Total'].iloc[b]) and pd.notnull(sub_df['Box'].iloc[b]):

			boxes_to_add = sub_df['Box'].iloc[b] # boxes to add per Fm
			file_total = sum(sub_df['Box']) # boxes for each file
			fm_top = sub_df['Top'].iloc[b] # known top for the formation
			fm_bottom = sub_df['Bottom'].iloc[b] # known bottom for the formation

			for box in range(boxes_to_add):

				if box == 0: boxtop = fm_top
				else: boxtop = np.NaN

				if box == boxes_to_add - 1: boxbottom = fm_bottom
				else: boxbottom = np.NaN

				# file|box|total|loc|API|op|lease|Well#|S|T|Td|R|Rd|Q|
				row_add = dict(zip(sub_df.columns.values.tolist(),
					[sub_df['File #'].iloc[b],
					boxes_added + 1, # 'Box'
					file_total, # 'Total'
					sub_df['Location'].iloc[b],
					sub_df['API'].iloc[b],
					sub_df['Operator'].iloc[b],
					sub_df['Lease'].iloc[b],
					sub_df['Well #'].iloc[b],
					sub_df['Sec'].iloc[b],
					sub_df['Tw'].iloc[b],
					sub_df['TwD'].iloc[b],
					sub_df['Rg'].iloc[b],
					sub_df['RgD'].iloc[b],
					sub_df['Quarter'].iloc[b],
					sub_df['Latitude'].iloc[b],
					sub_df['Longitude'].iloc[b],
					sub_df['County'].iloc[b],
					sub_df['State'].iloc[b],
					sub_df['Formation'].iloc[b],
					sub_df['Field'].iloc[b],
					boxtop, #'Top'
					boxbottom, #'Bottom'
					sub_df['Type'].iloc[b],
					sub_df['Box Type'].iloc[b],
					sub_df['Condition'].iloc[b],
					sub_df['Diameter'].iloc[b],
					sub_df['Restrictions'].iloc[b],
					sub_df['Comments'].iloc[b] ]))

				df_add = pd.DataFrame([row_add])
				clean_df = pd.concat([clean_df, df_add], ignore_index = True)
				boxes_added += 1

				########################################################
				print("added row: ", row_add['File #'], ' | ', row_add['Box'], ' | ', row_add['Top'], ' | ', row_add['Bottom'])
				########################################################

		elif pd.notnull(sub_df['Box'].iloc[b]) and pd.notnull(sub_df['Total'].iloc[b]):
			print("good file: ", sub_df["File #"].iloc[b])





# if 'box # has value' AND 'total boxes is NaN'
# or the other way around ...
# ... subset for the file
# ... set or verify value for 'total'
# ... create new boxes (first 'top' and last 'bottom' will be known for each group)

# if both fields are present...
# ... fill in row by copying values over; no formatting needed


# SAVE CLEANED DATA













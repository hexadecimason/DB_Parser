# Clean DB so each row is a box, APIs aren't missing, and file numbers are appropriate

import pandas as pd
pd.set_option('display.max_columns', 20)
pd.set_option('display.width', 1000)

# load CSV into a DF
master_csv = pd.read_csv('OGS_OCDB/DB_master.csv', low_memory = False)
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
filtered_apis = df_master[df_master['API'].astype(str).str.isdecimal()]

# FILTER FOR "A/C files"
filtered_ac = df_master[df_master['File #'] != 'A/C']

# FIX MISSING BOX DATA
# create a new DF and add data row-by-row, creating new rows when needed to individualize ranges of boxes




# SAVE CLEANED DATA
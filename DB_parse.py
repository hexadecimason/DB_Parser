import pandas as pd
import numpy as np
import time
import psycopg2 as ppg2
from psycopg2.extensions import register_adapter, AsIs

# DATABASE VARS
db_name = 'opic_core'

# include nullboxes.csv?
nullboxes = True

# helper function: pandas types and PG types don't interact very well by default
def adapt_np64(val):
	return(AsIs(val))
register_adapter(np.int64, adapt_np64)

# PARSE CSV
master_df = pd.read_csv("data/cleaned.csv")

if nullboxes:
	nullboxes_csv = pd.read_csv("data/nullboxes.csv")	
	nullboxes_df = pd.DataFrame(nullboxes_csv)
	nullboxes_df['Box'] = nullboxes_df['Box'].fillna(np.nan).replace([np.nan], None)

	master_df = pd.concat([master_df, nullboxes_df])

#empty list of wells
well_list = []

# structure wells with boxes
print("\nParsing CSV...")
time_i = time.perf_counter()
for file in master_df["File #"].unique():

	# separate file
	sub_df = master_df[master_df["File #"] == file]

	# create well structure
	well = {'file' : file, 
           'total_boxes' : 0,#(sub_df['Total'].iloc[0]), 
           'api' : sub_df['API'].iloc[0], 
           'operator' : sub_df['Operator'].iloc[0],
           'lease' : sub_df['Lease'].iloc[0],
           'well #' : sub_df['Well #'].iloc[0],
           'sec' : sub_df['Sec'].iloc[0],
           'tw' : sub_df['Tw'].iloc[0],
           'tw_d' : sub_df['TwD'].iloc[0],
           'rg' : sub_df['Rg'].iloc[0],
           'rg_d' : sub_df['RgD'].iloc[0],
           'qq' : sub_df['Quarter'].iloc[0],
           'lat' : sub_df['Latitude'].iloc[0],
           'long' : sub_df['Longitude'].iloc[0],
           'county' : sub_df['County'].iloc[0],
           'state' : sub_df['State'].iloc[0],
           'field' : sub_df['Field'].iloc[0],
           'boxes' : []}

	# add boxes
	for line in range(len(sub_df['File #'])):
		box_dict = {'box #' : sub_df['Box'].iloc[line],
						'top' : sub_df['Top'].iloc[line],
						'bottom' : sub_df['Bottom'].iloc[line],
						'fm' : sub_df['Formation'].iloc[line],
						'dia' : sub_df['Diameter'].iloc[line],
						'bType' : sub_df['Box Type'].iloc[line],
						'sType' : sub_df['Type'].iloc[line],
						'cond' : sub_df['Condition'].iloc[line],
						'rest' : sub_df['Restrictions'].iloc[line],
						'com' : sub_df['Comments'].iloc[line]}

		well['boxes'].append(box_dict) # append box to list of well boxes
		well['total_boxes'] += 1 # update well-level box total

	# add to list of well objects
	well_list.append(well)

time_f = time.perf_counter()
print("Total time to parse into python structures: ", time_f - time_i)

# CONNECT TO PG
con = ppg2.connect("user=postgres password=p@ssw0rd")
con.autocommit = True
cursor = con.cursor()
cursor.execute(f"""DROP DATABASE IF EXISTS {db_name};""")
# can't create a table if an old version already
cursor.execute("DROP TABLE IF EXISTS wells;")
cursor.execute("DROP TYPE IF EXISTS public.box;")
cursor.execute(f"""CREATE DATABASE {db_name};""")

# CONNECT TO DB
con = ppg2.connect(f"dbname={db_name} user=postgres password=p@ssw0rd")
cursor = con.cursor() # reset cursor to new DB
# can't create a table if an old version already
cursor.execute("DROP TABLE IF EXISTS wells;") 
cursor.execute("DROP TYPE IF EXISTS public.box;")

# SET UP DB
cursor.execute("""CREATE TYPE box AS (
				num			int,
				fm			text,
				top			text,
				bottom		text,
				diameter	text,
				boxType		text,
				sampleType	text,
				condition	text,
				restrictions	text,
				comments	text );"""	)

cursor.execute("""CREATE TABLE wells (
				file		text,
				api			bigint,
				boxcount	int,
				operator	text,
				lease		text,
				well_num	text,
				sec			int,
				town		int,
				town_d		text,
				range		int,
				range_d		text,
				QQ			text,
				latitude	double precision,
				longitude	double precision,
				county		text,
				state		text,
				field		text,
				boxes		public.box[],

				PRIMARY KEY(file));"""	)

# STORE STRUCTURED DATA
print(f"Adding wells to Postgres DB: {db_name}")
for well in well_list:

	addWell_query = """INSERT INTO wells VALUES
				(%s, %s, %s, %s, %s, %s, %s, %s, %s,
				%s, %s, %s, %s, %s, %s, %s, %s);"""

	addWell_tuple = (well['file'], well['api'], well['total_boxes'],
					well['operator'], well['lease'], well['well #'],
					well['sec'], well['tw'], well['tw_d'],
					well['rg'], well['rg_d'], well['qq'],
					well['lat'], well['long'],
					well['county'], well['state'], well['field'])

	cursor.execute(addWell_query, addWell_tuple)

	# add boxes
	addBox_query = """UPDATE wells SET boxes = ( ARRAY_APPEND(boxes, 
						(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)::public.box) 
						) WHERE file = %s;"""


	for bx in well['boxes']:

		addBox_tuple = (bx['box #'], bx['fm'], bx['top'],
						bx['bottom'], bx['dia'], bx['bType'],
						bx['sType'], bx['cond'], bx['rest'],
						bx['com'], well['file'])

		cursor.execute(addBox_query, addBox_tuple)


# COMMIT
con.commit()

# DISCONNECT
cursor.close()
con.close()
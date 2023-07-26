import pandas as pd
import numpy as np
import psycopg2 as ppg2
from psycopg2.extensions import register_adapter, AsIs
from OPIC_Well import OPIC_Well

# DATABASE VARS
db_name = 'opic_core'

# include nullboxes.csv?
nullboxes = True

# helper function: pandas types and PG types don't interact very well by default

def adapt_np64(val):
	return(AsIs(val))
register_adapter(np.int64, adapt_np64)

# PARSE CSV

master_csv = pd.read_csv("data/cleaned.csv")
master_df = pd.DataFrame(master_csv)

if nullboxes:
	nullboxes_csv = pd.read_csv("data/nullboxes.csv")	
	nullboxes_df = pd.DataFrame(nullboxes_csv)
	nullboxes_df['Box'] = nullboxes_df['Box'].fillna(np.nan).replace([np.nan], None)

	master_df = pd.concat([master_df, nullboxes_df])

well_list = []

# structure wells with boxes
for file in master_df["File #"].unique():

	# separate file
	sub_df = master_df[master_df["File #"] == file]

	# create well object
	well = OPIC_Well(file, 
           (sub_df['Total'].iloc[0]), 
           sub_df['API'].iloc[0], 
           sub_df['Operator'].iloc[0],
           sub_df['Lease'].iloc[0],
           sub_df['Well #'].iloc[0],
           sub_df['Sec'].iloc[0],
           sub_df['Tw'].iloc[0],
           sub_df['TwD'].iloc[0],
           sub_df['Rg'].iloc[0],
           sub_df['RgD'].iloc[0],
           sub_df['Quarter'].iloc[0],
           sub_df['Latitude'].iloc[0],
           sub_df['Longitude'].iloc[0],
           sub_df['County'].iloc[0],
           sub_df['State'].iloc[0],
           sub_df['Field'].iloc[0])

	# add boxes
	for line in range(len(sub_df['File #'])):
		boxNum = sub_df['Box'].iloc[line]
		top = sub_df['Top'].iloc[line]
		bottom = sub_df['Bottom'].iloc[line]
		fm = sub_df['Formation'].iloc[line]
		dia = sub_df['Diameter'].iloc[line]
		bType = sub_df['Box Type'].iloc[line]
		sType = sub_df['Type'].iloc[line]
		cond = sub_df['Condition'].iloc[line]
		rest = sub_df['Restrictions'].iloc[line]
		com = sub_df['Comments'].iloc[line]

		well.addBox(boxNum, top, bottom, fm, dia, sType, bType, cond, rest, com)

	# add to list of well objects
	well_list.append(well)

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
for well in well_list:

	# add well
	print("adding to Postgres database: ", well.fileNumber)

	addWell_query = """INSERT INTO wells VALUES
				(%s, %s, %s, %s, %s, %s, %s, %s, %s,
				%s, %s, %s, %s, %s, %s, %s, %s);"""

	addWell_tuple = (well.fileNumber, well.api, well.boxCount,
					well.operator, well.leaseName, well.wellNum,
					well.STR[0], well.STR[1], well.STR[2],
					well.STR[3], well.STR[4], well.QQ,
					well.latLong[0], well.latLong[1],
					well.county, well.state, well.field)

	cursor.execute(addWell_query, addWell_tuple)

	# add boxes
	addBox_query = """UPDATE wells SET boxes = ( ARRAY_APPEND(boxes, 
						(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)::public.box) 
						) WHERE file = %s;"""


	for bx in well.boxes:

		addBox_tuple = (bx.boxNumber, bx.formation, bx.top,
						bx.bottom, bx.diameter, bx.boxType,
						bx.sampleType, bx.condition, bx.restrictions,
						bx.comments, well.fileNumber)

		cursor.execute(addBox_query, addBox_tuple)

# COMMIT
con.commit()

# DISCONNECT
cursor.close()
con.close()
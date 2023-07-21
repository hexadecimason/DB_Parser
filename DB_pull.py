# data has been cleaned and parsed
# now we can create a script to test querying

import psycopg2 as ppg2

db_name = "opic_core"
file_example = input("enter file number: ")

con = ppg2.connect(f"""dbname = {db_name} user=postgres password=p@ssw0rd""")
cursor = con.cursor()

# well-level VIEW
cursor.execute("DROP VIEW IF EXISTS wellview;")
cursor.execute("""CREATE VIEW wellview AS SELECT
					file, api, boxcount, operator, lease, well_num,
					sec, town, town_d, range, range_d, QQ, latitude, longitude,
					county, state, field
				FROM wells;""")


def getWell(file_num):
	cursor.execute(f"""SELECT * FROM wellview WHERE file = '{file_num}';""")
	q = cursor.fetchall()

	if len(q) == 0:
		print("file not in database")
		return

	print("File #: \t\t" + str(q[0][0]))
	print("API: \t\t\t" + str(q[0][1]))
	print("Operator: \t\t" + str(q[0][3]))
	print("Lease: \t\t\t" + str(q[0][4]))
	print("Well Number: \t\t" + str(q[0][5]))
	print("STR: \t\t\t" + str(q[0][6]) + "-" + str(q[0][7]) + str(q[0][8])
			+ "-" + str(q[0][9]) + str(q[0][10]))
	print("QQ: \t\t\t" + str(q[0][11]))
	print("[Lat, Long]: \t\t" + "[" + str(q[0][12]) + ", " + str(q[0][13]) + "]")
	print("County: \t\t" + str(q[0][14]))
	print("State: \t\t\t" + str(q[0][15]))
	print("Field: \t\t\t" + str(q[0][16]))
	print("Boxes: \t\t\t" + str(q[0][2]))



def getBoxes(file_num, types = False):
	cursor.execute(f"""WITH bxs AS (SELECT UNNEST(boxes) AS bx 
								FROM wells WHERE file = '{file_num}')
					SELECT (bx).* from bxs;""")
	q = cursor.fetchall()

	if len(q) == 0:
		print("file not in database")
		return

	print("File #: " + file_num)

	for box_num in range(0, len(q)):
		print("Box #: " + str(q[box_num][0]), end = "\t")
		print("Fm: " + str(q[box_num][1]), end = "\t")
		print("\t" + str(q[box_num][2]) + " - " + str(q[box_num][3]), end = "\n")
		print("\t" + str(q[box_num][4]) + "\" ", end = "\t")
		if types: 
			print("Box Type: " + str(q[box_num][5]), end = "\t")
			print("Sample Type: " + str(q[box_num][6]), end = "\t")
		print("Cond: " + str(q[box_num][7]), end = "\t")
		if str(q[box_num][8]) != "NaN":
			print("Restrictions: " + str(q[box_num][8]), end = "")
		print("\n\tComments: " + str(q[box_num][9]), end = "\n")



def getBoxesBasic(file_num):

	cursor.execute(f"""WITH bxs AS (SELECT UNNEST(boxes) AS bx 
								FROM wells WHERE file = '{file_num}')
					SELECT (bx).* from bxs;""")
	q = cursor.fetchall()

	if len(q) == 0:
		print("file not in database")
		return

	for box_num in range(0, len(q)):
		print("Box #: " + str(q[box_num][0]), end = "\t")
		print("Fm: " + str(q[box_num][1]), end = "\t")
		print(str(q[box_num][2]) + "\t-  " + str(q[box_num][3]), end = "\n")

def getWellInfo(file_num):
	print("-----------------------------------------------------")
	getWell(file_num)
	print('')
	getBoxesBasic(file_num)
	print("-----------------------------------------------------")

getWellInfo(file_example)

cursor.close()
con.close()
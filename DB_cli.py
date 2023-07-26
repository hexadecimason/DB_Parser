import psycopg2 as ppg2
from psycopg2.extras import RealDictCursor

db_name = "opic_core"
con = ppg2.connect(f"""dbname = {db_name} user=postgres password=p@ssw0rd""")
cursor = con.cursor(cursor_factory=RealDictCursor)

# well-level VIEW
cursor.execute("DROP VIEW IF EXISTS wellview;")
cursor.execute("""CREATE VIEW wellview AS SELECT
					file, api, boxcount, operator, lease, well_num,
					sec, town, town_d, range, range_d, QQ, latitude, longitude,
					county, state, field
				FROM wells;""")


def parse_well(q_well):

	wells = []

	for w in range(len(q_well)):
		well = {'file' : q_well[w]['file'],
				'API' : q_well[w]['api'],
				'operator' : q_well[w]['operator'],
				'lease' : q_well[w]['lease'],
				'well_num' : q_well[w]['well_num'],
				'str' : [q_well[w]['sec'], q_well[w]['town'], q_well[w]['town_d'], q_well[w]['range'], q_well[w]['range_d']],
				'qq' : q_well[w]['qq'],
				'll' : [q_well[w]['latitude'], q_well[w]['longitude']],
				'county' : q_well[w]['county'],
				'state' : q_well[w]['state'],
				'field' : q_well[w]['field'],
				'boxcount' : q_well[w]['boxcount'],
				'boxes' : []}

		file_num = well['file']
		cursor.execute(f"""WITH bxs AS (SELECT UNNEST(boxes) AS bx 
								FROM wells WHERE file = '{file_num}')
					SELECT (bx).* from bxs;""")
		q = cursor.fetchall()

		for b in range(len(q)):
			bx = {'#' : q[b]['num'],
				'top' : q[b]['top'],
				'bottom' : q[b]['bottom'],
				'fm' : q[b]['fm'],
				'diameter' : q[b]['diameter'],
				'box type' : q[b]['boxtype'],
				'sample type' : q[b]['sampletype'],
				'condition' : q[b]['condition'],
				'restrictions' : q[b]['restrictions'],
				'comments' : q[b]['comments']
			}

			well['boxes'].append(bx)
		wells.append(well)

	return wells

def parse_query(q):
	if len(q) == 0:
		print("not in database")
		return -1

	wells = parse_well(q)
	return wells

def print_output(wells, include_boxes):
	if(wells != -1):
		for well in wells:
			print("----------------------------------------------")
			print("File #: \t\t" + str(well['file']))
			print("API: \t\t\t" + str(well['API']))
			print("Operator: \t\t" + str(well['operator']))
			print("Lease: \t\t\t" + str(well['lease']))
			print("Well #: \t\t" + str(well['well_num']))
			print("STR: \t\t\t" + str(well['str']))
			print("QQ: \t\t\t" + str(well['qq']))
			print("[Lat, Long]: \t\t" + str(well['ll']))
			print("County: \t\t" + str(well['county']))
			print("State: \t\t\t" + str(well['state']))
			print("Field: \t\t\t" + str(well['field']))
			print("Boxes: \t\t\t" + str(well['boxcount']))

			if(include_boxes):
				print('\n')
				for box in well['boxes']:
					print('Box #: ' + str(box['#']), end = "\t")
					print(str(box['top']) + ' - ' + str(box['bottom']), end='\t')
					if(box['fm'] == "NaN"): print('Fm: ' + str(box['fm']), end = '\t\t')
					else: print('Fm: ' + str(box['fm']), end = '\t')
					print(str(box['box type']) + "/" + str(box['diameter']) + "\"/" 
							+ str(box['sample type']), end = '\t\t')
					print("Condition: " + str(box['condition']), end = '\t\t')
					print("Restrictions: " + str(box['restrictions']), end = '\t')
					print("Comments: " + str(box['comments']))

			print("----------------------------------------------")

def main():

	user_choice = input("enter query type: FILE / API / OPERATOR / LEASE (Q to quit)\n").upper()

	match user_choice:
		case "FILE":
			include = input("include box-level data? (Y to confirm): ").upper()
			include_boxes = True if include == "Y" else False
			file_num = input("enter file number: ")
			cursor.execute(f"""SELECT * FROM wellview WHERE file = '{file_num}';""")
		case "API":
			include = input("include box-level data? (Y to confirm): ").upper()
			include_boxes = True if include == "Y" else False
			api = input("enter API: ")
			cursor.execute(f"""SELECT * FROM wellview WHERE api = '{api}';""")
		case "OPERATOR":
			include_boxes = False
			operator = input("enter operator: ")
			cursor.execute(f"""SELECT * FROM wellview WHERE LOWER(operator) LIKE LOWER('%{operator}%');""")
		case "LEASE":
			include_boxes = False
			lease = input("enter lease name: ")
			cursor.execute(f"""SELECT * FROM wellview WHERE LOWER(lease) LIKE LOWER('%{lease}%');""")
		case "Q":
			print("exiting program...")
			exit()
		case other:
			print("invalid query type...")
			main()

	wells = parse_query(cursor.fetchall())
	print_output(wells, include_boxes)

main()

# 35073400000000
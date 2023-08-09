import psycopg2 as ppg2
from psycopg2.extras import RealDictCursor

db_name = "opic_core_fk"
con = ppg2.connect(f"""dbname = {db_name} user=postgres password=p@ssw0rd""")
cursor = con.cursor(cursor_factory=RealDictCursor)

# well-level VIEW
cursor.execute("DROP VIEW IF EXISTS wellview;")
'''cursor.execute("""CREATE VIEW wellview AS SELECT
					file, api, boxcount, operator, lease, well_num,
					sec, town, town_d, range, range_d, QQ, latitude, longitude,
					county, state, field
				FROM wells;""")'''

# takes in a non-empty cursor.fetch_all() object and returns a list of dictionaries of well data
def parse_q(q):

	#if empty, return error code -1
	if len(q) == 0:
		print("not in database")
		return -1

	wells = []

	# scrape through length of returned fetchall() object
	for w in range(len(q)):
		well = {'file' : q[w]['file_num'],
				'API' : q[w]['api'],
				'operator' : q[w]['operator'],
				'lease' : q[w]['lease'],
				'well_num' : q[w]['well_num'],
				'str' : [q[w]['sec'], q[w]['twn'], q[w]['twn_d'], q[w]['rng'], q[w]['rng_d']],
				'qq' : q[w]['qq'],
				'll' : [q[w]['latitude'], q[w]['longitude']],
				'county' : q[w]['county'],
				'state' : q[w]['state'],
				'field' : q[w]['field'],
				'box_count' : q[w]['box_count'],
				'boxes' : []}

		file_num = well['file']

		q_string = f"""SELECT * FROM boxes WHERE file_num = '{file_num}';"""
		cursor.execute(q_string)
		q_boxquery = cursor.fetchall()

		for b in range(len(q_boxquery)):
			bx = { 'file' : q_boxquery[b]['file_num'],
				'#' : q_boxquery[b]['box_num'],
				'top' : q_boxquery[b]['top'],
				'bottom' : q_boxquery[b]['bottom'],
				'fm' : q_boxquery[b]['fm'],
				'diameter' : q_boxquery[b]['diameter'],
				'box type' : q_boxquery[b]['box_type'],
				'sample type' : q_boxquery[b]['sample_type'],
				'condition' : q_boxquery[b]['condition'],
				'restrictions' : q_boxquery[b]['restrictions'],
				'comments' : q_boxquery[b]['comments']}

			well['boxes'].append(bx)
		wells.append(well)

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
			print("Boxes: \t\t\t" + str(well['box_count']))

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
			cursor.execute(f"""SELECT * FROM wells WHERE file_num = '{file_num}';""")
		case "API":
			include = input("include box-level data? (Y to confirm): ").upper()
			include_boxes = True if include == "Y" else False
			api = input("enter API: ")
			cursor.execute(f"""SELECT * FROM wells WHERE api = '{api}';""")
		case "OPERATOR":
			include_boxes = False
			operator = input("enter operator: ")
			cursor.execute(f"""SELECT * FROM wells WHERE LOWER(operator) LIKE LOWER('%{operator}%');""")
		case "LEASE":
			include_boxes = False
			lease = input("enter lease name: ")
			cursor.execute(f"""SELECT * FROM wells WHERE LOWER(lease) LIKE LOWER('%{lease}%');""")
		case "Q":
			print("exiting program...")
			exit()
		case other:
			print("invalid query type...\n")
			main()

	wells = parse_q(cursor.fetchall())
	print_output(wells, include_boxes)

main()
DROP DATABASE IF EXISTS testdb;
DROP TABLE IF EXISTS wells;
DROP TYPE IF EXISTS public.box;

CREATE DATABASE testdb;
\c testdb;

CREATE TYPE box AS (
	num		int,
	fm		varchar(30),
	top		int,
	bottom	int
);

CREATE TABLE wells (
	file		varchar(10),
	operator	varchar(20),
	lease		varchar(20),
	well_num	varchar(10),
	api			int,
	boxes		public.box[],

	PRIMARY KEY(file)
);

-- inserts:

--adding boxes with well-level info

INSERT INTO wells VALUES ('1', 'Op1', 'Lease1', '1-1', '100000', 
							ARRAY[(1, 'Fm1', 100, 110)::public.box, 
									(2, 'Fm1', 110, 120)::public.box]);

-- adding a box to existing well

UPDATE wells SET boxes = (SELECT ARRAY_APPEND(boxes, 
		(3, 'Fm2', 120, 130)::public.box) FROM wells) WHERE file = '1';


-- output:

-- well-level
SELECT * FROM wells;

-- box-level for a given well
WITH boxlevel AS (SELECT UNNEST(boxes) AS row FROM wells WHERE file = '1')
	SELECT (row).* FROM boxlevel;


-- finish up
DROP TABLE IF EXISTS wells;
DROP TYPE IF EXISTS public.box;
\c postgres; -- disconnect from testdb by reconnecting to default database
DROP DATABASE IF EXISTS testdb;
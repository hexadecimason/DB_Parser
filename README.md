# ***Rebuilding Core Database***

## ***Database Parsing Pipeline***

In order to create a database of OPIC well data that is useful in the scope of other projects and easier to work with internally, the data must be cleaned, then restructured and added to a PosgreSQL database. This is done in the following sequence:

(1) **DB_clean.py** 

This script is used locally to input the original data as a CSV, correct potentially errant values (the consequence of Excel preserving auto-formatted vlaues), and parse it into two files with all box-level data present. One file contains all entries where we can determine total boxes represented by the entry (*cleaned.csv*). The other contains entires where 'box number' and 'box total' are *both* null, and so the total number of boxes represented in these rows cannot be determined (*nullboxes.csv*). However, these entries have APIs and there is relevant depth and formation data and so these entries will be included in the final database, but require applying a slightly different procedure to add to the Postgres instance.

Note that in analyzing the entries with no box counts, we determined these to be almost entirely boxes of chips (~ 99.6%), some of which accompany wells with numbered boxes; others do not and are in wells for which we only have boxes of chips.

Of the original wells in our Excel database, there are a total of 274 for which there is no valid API entry: DB_clean.py will exclude these.

(2) **DB_parse.py** 

This uses the *psycopg2* library as an adapter for PostgreSQL to create, structure, and fill a new Postgres database. It features the option to either include or exclude *nullboxes.csv*. Boxes from *nullboxes.csv* will appear along side the other numbered boxes, but will have "None" in place of a number.

## ***PostgreSQL Database Structure***

The Postgres instance contains two tables, 'wells,' composed of the well-level data from the original database (File #, API, Operator, Lease, etc.), and a table for all box-level data. These two tables are correlated using a foreign key on file number. This structure provides an easy mechanism for making queries of both levels of data, and provides a natural structure for integrating into other environments such as Django.

Previously, box-level data were nested in an array in the 'wells' table; however, querying for box level data became unintuitive and verbose and is not a natural fit for how Django builds models of databases.

## ***Interface to Databse***

Currently, the database is stored locally and accessed with a Python program (***DB_cli.py***) that acts as a command line interface (CLI). A Python virtual environment is required for the CLI program to work. It showcases the basic functionality of the Postgres instance in providing a source for pulling data. It provides methods for querying based on File # and API. It also provides methods for fuzzy searching on Operator and Lease.


### ***File Query***

Sample output for DB_cli.py is as follows for a query for file '1A':

```
$ python DB_cli.py
enter query type: FILE / API / OPERATOR / LEASE (Q to quit)
file
include box-level data? (Y to confirm): y
enter file number: 1A
----------------------------------------------
File #:                 1A
API:                    35073355020000
Operator:               King & Stevenson
Lease:                  Kudlac
Well #:                 1
STR:                    [15, 19, 'N', 6, 'W']
QQ:                     C NE SE
[Lat, Long]:            [36.1217518, -97.8198849]
County:                 Kingfisher
State:                  OK
Field:                  Dover-Hennessey
Boxes:                  7


Box #: 1        6688 - 6697     Fm: Maramecian  Slab Pack Box/3"/Slab           Condition: Good         Restrictions: NaN       Comments: 10\' for 5 Slot Box
Box #: 2        6697 - 6709     Fm: Maramecian  Slab Pack Box/3"/Slab           Condition: Good         Restrictions: NaN       Comments: 12\' for 6 Slot Box
Box #: 3        6709 - 6721     Fm: Maramecian  Slab Pack Box/3"/Slab           Condition: Good         Restrictions: NaN       Comments: 12\' for 6 Slot Box
Box #: 4        6721 - 6733     Fm: Maramecian  Slab Pack Box/3"/Slab           Condition: Good         Restrictions: NaN       Comments: 12\' for 6 Slot Box
Box #: 5        6733 - 6745     Fm: Maramecian  Slab Pack Box/3"/Slab           Condition: Good         Restrictions: NaN       Comments: 12\' for 6 Slot Box
Box #: 6        6745 - 6757     Fm: Maramecian  Slab Pack Box/3"/Slab           Condition: Good         Restrictions: NaN       Comments: 12\' for 6 Slot Box
Box #: 7        6757 - 6765     Fm: Maramecian  Slab Pack Box/3"/Slab           Condition: Good         Restrictions: NaN       Comments: 12\' for 6 Slot Box
----------------------------------------------

```

## ***Future Progress***

Going forward, we can expand searching capabilities. For instance, we could implement features for restricting a search to particular STR or Lat/Long values. We could also provide features for searching by Formation or other box-level data such as depths or sample type/condition.

As the databse becomes more widely used, an API written in python will be useful both for ensuring the integrity of the databse and also for providing easier access to common query operations. An API will provide access to box-level data for other projects such as the WellViewer; it will also enable reconstruction of internal databse tooling and allow for the avoidance of large-scale Excel files that rely heavily on macros and frequently re-format data.

Finally, in order to be deployed the database will need to persist on a server environment that can be readily connected to.
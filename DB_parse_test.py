import pandas as pd
import numpy as np
import os
from OPIC_Well import OPIC_Well

badVal = "N/A"

# load CSV into a DF
master_csv = pd.read_csv('data/DB_master.csv', low_memory = False)
df_master = pd.DataFrame(master_csv)

# Select known columns: some are filled with blanks and not needed
df_clean = df_master[['File #', 'Box', 'Total', 'Location', 'API', 'Operator', 
                    'Lease', 'Well #', 'Sec', 'Tw', 'TwD', 'Rg', 'RgD', 'Quarter', 
                    'Latitude', 'Longitude', 'County', 'State', 'Formation', 'Field', 
                    'Top', 'Bottom', 'Type', 'Box Type', 'Condition', 'Diameter', 'Restrictions', 'Comments']]

# Replace NaN with 'badVal' for easier comparison later
df_clean.fillna(badVal, inplace= True)

# liteRepresentation - more human-readable
# rawRepresentation - no formatting for readability: data only
# verBose representation - the original DB as a txt file
liteRepresentation = open("data/Output Files/DB_lite.txt", "w") # file to write to
rawRepresentation = open("data/Output Files/DB_raw.txt", "w")
verboseRepresentation = open("data/Output Files/DB_verbose.txt", "w")
all_wells = []


# Generate LITE representation #########################
print("Parsing & generating LITE representation...")
# per file number...
for num in df_clean["File #"].unique():
    sub_df = df_clean[df_clean['File #'] == num]

    #print(sub_df.head)

    if(sub_df['Total'].iloc[0] != badVal):

        # ... create a well
        well = OPIC_Well(num, 
                    int(sub_df['Total'].iloc[0]), 
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

        # per box in well...
        i = len(sub_df['File #'])
        if(sub_df['Box'].iloc[0] != badVal):
            for line in range(i):
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

        #print('\n')
        #print(well)
        liteRepresentation.write(str(well))
        all_wells.append(well)

liteRepresentation.close()


# Generate RAW representation ##############################

print("Generating RAW representation...")

oklahomaCounties = ['Adair', 'Alfalfa', 'Atoka', 'Beaver', 'Beckham', 'Blaine', 'Bryan', 'Caddo', 'Canadian',
                    'Carter', 'Cherokee', 'Choctaw', 'Cimarron', 'Cleveland', 'Coal', 'Comanche', 'Cotton',
                    'Craig', 'Creek', 'Custer', 'Delaware', 'Dewey', 'Ellis', 'Garfield', 'Garvin', 'Grady', 'Grant',
                    'Greer', 'Harmon', 'Harper', 'Haskell', 'Hughes', 'Jackson', 'Jefferson', 'Johnston', 'Kay',
                    'Kingfisher', 'Kiowa', 'Latimer', 'Le Flore', 'Lincoln', 'Logan', 'Love', 'Major', 'Marshall',
                    'Mayes', 'McClain', 'McCurtain', 'McIntosh', 'Murray', 'Muskogee', 'Noble', 'Nowata', 'Okfuskee',
                    'Oklahoma', 'Okmulgee', 'Osage', 'Ottawa', 'Pawnee', 'Payne', 'Pittsburg', 'Pontotoc', 'Pottawatomie',
                    'Pushmataha', 'Roger Mills', 'Rogers', 'Seminole', 'Sequoyah', 'Stephens', 'Texas', 'Tillman', 'Tulsa',
                    'Wagoner', 'Washington', 'Washita', 'Woods', 'Woodward']

for c in range(len(oklahomaCounties)):
    oklahomaCounties[c] = oklahomaCounties[c].lower()

def getCountyIndex(s) -> int:
    if not (s in oklahomaCounties):
        i = -1 # value for bad county entry
    else:
        i = oklahomaCounties.index(s)
    return i

for w in all_wells:
    s = str([w.fileNumber, w.api, w.operator, w.leaseName, w.wellNum, w.STR, w.QQ, w.latLong, getCountyIndex(w.county.lower()), w.state, w.field])
    rawRepresentation.write(s + '\n')
    for b in w.boxes:
        rawRepresentation.write(b.boxNumber + ':' + b.top + ':' + b.bottom + ':' + b.formation + ':' + b.diameter + ':' + b.sampleType + ':' + b.boxType + ':' + b.condition + ':' + b.restrictions + ':' + b.comments + '\n')
    rawRepresentation.write('#') # separates wells

rawRepresentation.close()


# Generate VERBOSE represenation ##############################
print("Generating VERBOSE representation...")

for row in range(len(df_clean.index)):
    for col in range(len(df_clean.iloc[row,:])):
        verboseRepresentation.write(str(df_clean.iloc[row, col]) + ', ')
    verboseRepresentation.write('\n')

verboseRepresentation.close()


# Size analysis

print("Analyzing file sizes")
lite_size = os.path.getsize("data/Output Files/DB_lite.txt")
raw_size = os.path.getsize("data/Output Files/DB_raw.txt")
verbose_size = os.path.getsize("data/Output Files/DB_verbose.txt")

verbose_raw_ratio = float(verbose_size) / float(raw_size)

print("Verbose size: " + str(verbose_size))
print("Raw size: " + str(raw_size))
print("Verbose/Raw ratio: " + str(verbose_raw_ratio))

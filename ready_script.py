import pandas as pd
import os
import glob

# change the yyyy_mm and filepath for your own system in line 10

path = os.getcwd()
csvPath = glob.glob( os.path.join( path, "*.csv" ))
# 
readyPath = r'C:\Users\aliso\Desktop\CS432\projs\NoSQL\Dataset\2020_03\ready\\'

for file in csvPath:
	dataframe = pd.read_csv( file, header=None )

	dataframe = dataframe[ 0 ]

	filename = file.split("\\")[ -1 ]

	dataframe.to_csv( readyPath+""+filename, index=False, header=None )

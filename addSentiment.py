from twarc import Twarc
import os
import glob
import csv, jsonlines
import io
import pandas as pd
from pandas import json_normalize
import json

path = os.getcwd()
csvPath = glob.glob( os.path.join( path, "*.csv" ))
hydratePath = r'C:\Users\aliso\Desktop\CS432\projs\NoSQL\Dataset\2020_03\hydrated\\'
jsonPath = glob.glob( os.path.join( hydratePath, "*.json" ))

for file in csvPath :
	filename = file.split("\\")[ -1 ]
	output_filename = hydratePath + "" + filename[ :filename.index( "." )] + ".json"

	data = []
	
	with open( file, 'r', newline='', encoding='utf-8') as sentFile :
		sentiments = dict( csv.reader( sentFile, delimiter=',' ))
		# print( sentiments )

	with io.open( output_filename, encoding = "utf-8" ) as f :
		for line in f:
			lineData = json.loads( line )
			ID = lineData[ 'id' ]
			# print( ID )
			sentValue = sentiments[ str( ID ) ]
			lineData[ "sentiment" ] = float( sentValue )
			data.append( lineData )

	with jsonlines.open( output_filename, "w" ) as writer :
			for hydrated_tweet in data :
				writer.write( hydrated_tweet )
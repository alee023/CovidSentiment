import csv
import io
import pandas as pd

with open( 'task4.csv', 'r', newline='', encoding='utf-8') as f :
	countrySents = dict( csv.reader( f, delimiter=',' ))

data = ""

with io.open( 'CountryData.csv', encoding = "utf-8" ) as f :
	for line in f :
		country = line[ :line.index( "," )]
		sentiment = countrySents.get( country, 0 )
		text = ( line.strip() + "," + str( sentiment )).strip() + "\n"
		data += text 
f.close()

# print( data )

writeF = io.open( 'countryDataSents.csv', 'w', encoding='utf-8' )
writeF.write( data )
writeF.close()
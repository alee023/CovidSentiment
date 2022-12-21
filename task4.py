from pymongo import MongoClient
import certifi
import pprint
import json
import warnings
import io 

warnings.filterwarnings( 'ignore' )
ca = certifi.where()

# SENTIMENT DATA BY COUNTRY

client = MongoClient("", tlsCAFile=ca)
db = client.proj3

pipeline = [
    {
        '$group': {
            '_id': '$place.country', 
            'avgSent': {
                '$avg': '$sentiment'
            }
        }
    }
]

results = db.twitterData.aggregate( pipeline ) 
writeFile = io.open( 'task4.csv', 'w', encoding='utf-8' )

for i in results:
	line = ""
	country = i[ '_id' ]
	sentiment = i[ 'avgSent' ]
	line += str( country ) + "," + str( sentiment ) + "\n"
	writeFile.write( line )

writeFile.close()
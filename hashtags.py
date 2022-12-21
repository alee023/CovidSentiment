from pymongo import MongoClient
import certifi
import pprint
import json
import warnings
import io 

warnings.filterwarnings( 'ignore' )
ca = certifi.where()

client = MongoClient("", tlsCAFile=ca)
db = client.proj3

pipeline = [
    {
        '$project': {
            'hashtags': '$entities.hashtags.text', 
            'sentiment': '$sentiment'
        }
    }, {
        '$unwind': {
            'path': '$hashtags'
        }
    }, {
        '$group': {
            '_id': {
                '$toLower': '$hashtags'
            }, 
            'numHashtags': {
                '$sum': 1
            }, 
            'avgSent': {
                '$avg': '$sentiment'
            }
        }
    }, {
        '$sort': {
            'numHashtags': -1
        }
    }, {
        '$limit': 100
    }
]

results = db.twitterData.aggregate( pipeline ) 
writeFile = io.open( 'hashtags.csv', 'w', encoding='utf-8' )

for i in results:
    #print( i )
    line = ""
    hashtag = i[ '_id' ]
    numHTG = i[ 'numHashtags' ]
    avgSent = i[ 'avgSent' ]
    line += str( hashtag ) + "," + str( avgSent ) + "," + str( numHTG ) + "\n"
    writeFile.write( line )

writeFile.close()
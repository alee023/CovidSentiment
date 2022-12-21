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

# SENTIMENT DATA BY WORD
# steps: 1) get the most popular words (top ~100)
#        2) average the sentiments for each popular word with a group by
#        3) categorize the sentiments and place the words into appropriate categories

pipeline = [
    {
        '$project': {
            'words': {
                '$split': [
                    '$full_text', ' '
                ]
            }, 
            'sentiment': '$sentiment', 
            'year': '$date.year'
        }
    }, {
        '$match': {
            'year': '2021'
        }
    }, {
        '$unwind': {
            'path': '$words'
        }
    }, {
        '$group': {
            '_id': {
                'words': '$words'
            }, 
            'numWords': {
                '$sum': 1
            }, 
            'avgSentiment': {
                '$avg': '$sentiment'
            }
        }
    }, {
        '$match': {
            '_id.words': {
                '$nin': [
                    'a', 'the', 'and', '@', 'of', 'to', 'my', '', 'this', 'is', 'for', 'because', 'at', 'I', 'on', 'with', 'it\'s', 'its', 'that', 'if', 'in', 'This', 'day', 'every', 'days', 'you', 'may', 'see', 'are', 'Are', 'from', 'be', 'your', 'The', 'all', 'have', 'it', '&amp;', 'me', 'by', 'Me', 'will', 'not', 'but', 'as', 'can', 'has', 'A', 'so', 'was', 'us', 'We', 'like', 'do', 'what', 'when', 'In', 'or', 'just', 'been', 'I\'m', 'we', 'get', 'up', 'new', 'our', '-', '=', '...', 'My', 'still', 'how', 'had', 'who', 'one', 'after', 'an', '-', 'our', 'up', 'get', 'new', 'about', 'more', 'into', 'these', 'go', '.', '|', 'got', 'only', 'were', 'than', 'would', 'take', 'It', 'much', 'many', 'here', 'they', 'back', 'their', 'THE', 'make', 'going', 'If', 'You', 'her', 'before', ',', '@...', 'am', 'being', 'there', 'his', 'even', 'https://t.co/e98nivciYk', 'now', 'out', 'some', 'last', 'over', 'know', 'Day', 'way', 'around', 'What', 'it\'s', 'So', 'To', 'very', 'while', 'And', 'the...', 'keep', 'those', 'When', 'doing', 'made', '19'
                ]
            }
        }
    }, {
        '$sort': {
            'numWords': -1
        }
    }, {
        '$limit': 100
    }, {
        '$project': {
            'word': '$_id.words', 
            'sentiment': '$avgSentiment', 
            'numWords': '$numWords'
        }
    }
]

results = db.parsedData.aggregate( pipeline ) 
writeFile = io.open( 'sentimentCSV1.csv', 'w', encoding='utf-8' )

for i in results:
    line = ""
    word = i[ 'word' ]
    sentiment = i[ 'sentiment' ]
    numWords = i[ 'numWords']
    line += word + "," + str( sentiment ) + str( numWords ) + "\n"
    writeFile.write( line )

writeFile.close()
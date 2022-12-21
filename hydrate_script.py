from twarc import Twarc
import os
import glob
import csv, jsonlines
from twarc_csv import CSVConverter
import io
import pandas as pd
from pandas import json_normalize
import json

consumer_key = ""
consumer_secret = ""
access_token=""
access_token_secret=""

t = Twarc(consumer_key, consumer_secret, access_token, access_token_secret)

path = os.getcwd()
csvPath = glob.glob( os.path.join( path, "*.csv" ))
hydratePath = r'C:\Users\aliso\Desktop\CS432\projs\NoSQL\Dataset\2020_03\hydrated\\'
jsonPath = glob.glob( os.path.join( hydratePath, "*.json" ))

keyset = [ "created_at", "id", "id_str", "full_text", "source", "truncated", "in_reply_to_status_id",
          "in_reply_to_status_id_str", "in_reply_to_user_id", "in_reply_to_user_id_str", 
          "in_reply_to_screen_name", "user", "coordinates", "place", "quoted_status_id",
          "quoted_status_id_str", "is_quote_status", "quoted_status", "retweeted_status", 
          "quote_count", "reply_count", "retweet_count", "favorite_count", "entities", 
          "extended_entities", "favorited", "retweeted", "possibly_sensitive", "filter_level", 
          "lang", "matching_rules", "current_user_retweet", "scopes", "withheld_copyright", 
          "withheld_in_countries", "withheld_scope", "geo", "contributors", "display_text_range",
          "quoted_status_permalink" ]

# want from this dataset: date (created_at), location (place), text (text), hashtags (hashtags), sentiment (in original files)
# want from separate dataset: # of covid cases and vax rates for location

for file in csvPath :
	filename = file.split("\\")[ -1 ]
	output_filename = hydratePath + "" + filename[ :filename.index( "." )] + ".json"

	hydrated_tweets = []

	for tweet in t.hydrate( open( file, encoding = "utf-8" )) :
		hydrated_tweets.append( tweet )
	
	with jsonlines.open( output_filename, "w" ) as writer :
		for hydrated_tweet in hydrated_tweets :
			writer.write( hydrated_tweet )

for file in jsonPath :
	filename = file.split( "\\" )[ -1 ]
	output_filename = filename[ :filename.index( "." )] + ".csv"
	hydrated_tweets = []

	#with jsonlines.open( file, "r" ) as reader :
	#	with open( hydratePath+""+output_filename, "w" ) as output_file : 
	#		converter = CSVConverter( infile = reader, outfile = output_file )
	#		converter.process

	#with jsonlines.open( file, "r" ) as reader :
	#	for i in reader.iter( type = dict, skip_invalid = True ) :
	#		hydrated_tweets.append( i ) ;

	with io.open( file, encoding = "utf-8" ) as f :
		d = json.loads( "[" + f.read().replace( "}\n{", "},\n{" ) + "]" )
	df = pd.json_normalize( d ) 
	df.to_csv( hydratePath + "" + output_filename )

	#with io.open( hydratePath+""+output_filename, "w", encoding = "utf-8", newline = '' ) as output_file :
	#	d = csv.DictWriter( output_file, keyset )
	#	d.writeheader()
	#	d.writerows( hydrated_tweets )

#for file in notFinalPath :
#	filename = file.split( "\\" )[ -1 ]
#	output_filename = filename[ :filename.index( "." )] + ".csv"

#	inputCSV = pd.read_csv( file, skiprows = 1 ) 
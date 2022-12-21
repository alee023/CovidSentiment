import io 

sentiments = io.open( 'wordSentiments20.txt', 'r', encoding='utf-8' )
lines = sentiments.readlines() 

count = 1

writeFile = io.open( 'sentimentCSV20.csv', 'w', encoding='utf-8' )
lineText = ""
for line in lines :
	if count % 3 == 1 :
		lineText += line[ line.index( ":" ) + 1:] + ","

	if count % 3 == 2 :
		lineText += line[ line.index( ":" ) + 1:] + ","

	if count % 3 == 0 :
		lineText += line[ line.index( ":" ) + 1:] + ",\n"
		writeFile.write( lineText )
		lineText = ""

	count += 1

writeFile.close()
sentiments.close()

cleanR = io.open( 'sentimentCSV20.csv', encoding='utf-8' )
contents = cleanR.read()
cleanR.close()
new_contents = contents.replace( '\n,', ',' )
cleanW = io.open( 'sentimentCSV20.csv', 'w', encoding='utf-8' )
cleanW.write( new_contents )
cleanW.close()
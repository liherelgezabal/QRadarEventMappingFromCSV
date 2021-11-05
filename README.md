# QRadarEventMappingFromCSV
QRadar scripts to import event mappings from a CSV file

This is an unsupported demo script. Use it at your own risk and test it in a non-production environment

Usage:

1. Use DSMEventMappingTemplate.xlsx to create a new CSV file with the mapping data
2. If this is the first time you run the script, get server certificates using this command
	./getCert.sh services-emea.skytap.com:14575 > cert.crt
3. run the script like this, and follow instructions
	./MapEventsFromCSV.py -i DSMEventMappingTemplate.csv -o DSMEventMappingTemplate.out.csv -l log/output.log
4. check logs and output csv file

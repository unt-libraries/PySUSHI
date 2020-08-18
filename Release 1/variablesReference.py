


##Where would you like to save your usage data CSV files?
path  = r'J:\\Collection Assessment Department\\Data\\LUC\\'

## What is the name of the table where you store your credentials for the platforms from which you would like to collect R4 reports?
r4table = "ResourcesR4"

## What is the name of the table where you store your credentials for the platforms from which you would like to collect R4 reports?
r5table = "ResourcesR5"

##What is the name of your usage database?
db = "LUC"

##What is the name of the server that your usage database is hosted on?
server = "LibMSSQL01"

## These are the schemas for the R5 master reports.
TR_J3_COLUMNS  = ['Title',
				  'Access_Type',
				  'Platform',
				  'ItemDataType',
				  'Print_ISSN',
				  'Online_ISSN',
				  'DOI',
				  'Proprietary',
				  'Date',
				  'Category',
				  'Metric_Type',
				  'Count']
DR_D1_COLUMNS = [ "Database",
				  "Platform",
				  "Publisher",
				  "Publisher_ID",
				  "Access_Method",
				  "Date",
				  "Metric_Type",
				  "Count"]
TR_COLUMNS = ["Title",
			 "Data_Type",
			 "Access_Type",
			 "Platform",
			 "Publisher",
			 "ItemDataType",
			 "Print_ISSN",
			 "Online_ISSN",
			 "DOI",
			 "ISBN",
		     "Proprietary",
		     "Date",
		     "Category",
		     "Metric_Type",
		     "Count"]
##If functionality for any new reports is being added, that report type needs to be added to the report_set tuple to prevent an "invalid report type" error.
report_set = ["TR_J3", "TR", "DR_D1", "TR_B3", "TR_B2", "TR_J2"]

## These are the address constants for COUNTER4 server requests.
NS = {
		"SOAP-ENV": "http://schemas.xmlsoap.org/soap/envelope/",
		"sushi": "http://www.niso.org/schemas/sushi",
		"sushicounter": "http://www.niso.org/schemas/sushi/counter",
		"counter": "http://www.niso.org/schemas/counter",
	}

## Where would you like to save your usage data CSV files?
path  = r"<If you're using this without the database, what's the full folder address where your CSVs will be stored?>"
##Example: path = r"J:\LibColAssess\Data"

## What is the name of the table where you store your credentials for the platforms from which you would like to collect R4 reports?
r4table = "ResourcesR4"

## What is the name of the table where you store your credentials for the platforms from which you would like to collect R4 reports?
r5table = "ResourcesR5"

##What is the name of your usage database?
db = "<Name of your usage database>"

##What is the name of the server that your usage database is hosted on?
server = "<Name of the SQL server on which your usage database is stored>"

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

##Note for TR/DR tables: The 'TRecNum' column isn't present here because it's added as 
## ... a full-fill column in the actual R5 report object instantiation (in the Counter5pybr file) 
## ... based on the trecnum argument to the constructor.
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

##These are the available months
month_selection = [	"January"
					,"February"
					,"March"
					,"April"
					,"May"
					,"June"
					,"July"
					,"August"
					,"September"
					,"October"
					,"November"
					,"December"]
months_dict = {		"January" : 1
					,"February" : 2
					,"March" : 3
					,"April" : 4
					,"May" : 5
					,"June" : 6
					,"July" : 7
					,"August" : 8
					,"September" : 9
					,"October" : 10
					,"November" : 11
					,"December" : 12}
##These are the available years
year_selection = [	2012
					,2013
					,2014
					,2015
					,2016
					,2017
					,2018
					,2019
					,2020]
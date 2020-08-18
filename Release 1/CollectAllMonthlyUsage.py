'''
Author: 				Chris Hergert
Date Created: 			10/1/2019
Date Last Modified:		3/4/2020
Created under Open Commons III License for The University Of North Texas Libraries

NOTE: This script requires that the user have access to their usage data server for pyodbc to connect. The server name and database name should be used for the 'server' and 'db' variables.
NOTE: This script is configured to pull all usage credentials for reports from tables in the usage database (Called LUC here), where these tables are called 'ResourcesR5' and 'ResourcesR4' for resources reporting usage in COUNTER R5 and COUNTER R4, respectively.
'''
from Counter5pybr import *
from Counter4pybr import *

#what month and year would you like to get usage for?
year = 2019
month = 8


os.system('cls' if os.name == 'nt' else 'clear')
##===========================================================================================================
##Collect R5 reports
##===========================================================================================================
## Pull down records file
conn = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + db + ';Trusted_Connection=yes')
sql = "SELECT * FROM " + r5table
recs = pd.read_sql(sql,conn)

## Initialize arrays for each report
jr1_arr = []
db1_arr = []
br1_arr = []
db2_arr = []
br2_arr = []
tr_arr = []
dr_arr = []

## Collect all R5 Usage
for row in recs.itertuples():
	lgn = getCredentialsR5(row[1])
	if (row[2] != '-') and (row[2] != None):
		if row[7] == 'x':
			try:
				usage = Counter5Report(year, month, 'TR_J3', lgn[0], lgn[1], lgn[2], f'{row[1]} - {month}, 2019', lgn[3], lgn[4])
				usage.display()
				tr_arr.append(usage.reportDF)
				
				turnaways = Counter5Report(year, month, 'TR_J2', lgn[0], lgn[1], lgn[2], f'{row[1]} - {month}, 2019', lgn[3], lgn[4])
				turnaways.display()
				tr_arr.append(turnaways.reportDF)
			except:
				logging.warning(f"usage-gathering error for {row[1]} TR_J.")
		if row[8] == 'x':
			try:
				usage = Counter5Report(year, month, 'TR_B3', lgn[0], lgn[1], lgn[2], f'{row[1]} - {month}, 2019', lgn[3], lgn[4]) 
				usage.display()
				tr_arr.append(usage.reportDF)
				
				turnaways = Counter5Report(year, month, 'TR_B3', lgn[0], lgn[1], lgn[2], f'{row[1]} - {month}, 2019', lgn[3], lgn[4]) 
				turnaways.display()
				tr_arr.append(turnaways.reportDF)
			except:
				logging.warning(f"usage-gathering error for {row[1]} TR_B.")
		if row[10] == 'x':
			try:
				usage = Counter5Report(year, month, 'DR_D1', lgn[0], lgn[1], lgn[2], f'{row[1]} - {month}, 2019', lgn[3], lgn[4]) 
				usage.display()
				dr_arr.append(usage.reportDF)
			except:
				logging.warning(f"usage-gathering error for {row[1]} DR_D1.")

## Compile the TR and DR arrays into complete dataframes, and print them to Excel files.
CompletedTR = pd.concat(tr_arr)
CompletedDR = pd.concat(dr_arr)
CompletedTR.to_excel(os.path.join(path,r'TR Usage.xlsx'), index = False, encoding = None)
CompletedDR.to_excel(os.path.join(path,r'DR Usage.xlsx'), index = False, encoding = None)


##===========================================================================================================
##Collect R4 reports
##===========================================================================================================
## Pull down records file
conn = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + db + ';Trusted_Connection=yes')
sql = "SELECT * FROM " + r4table
recs = pd.read_sql(sql,conn)

## Collect all R4 Usage
for row in recs.itertuples():
	lgn = getCredentialsR4(row[1])
	if (row[2] != '-') and (row[2] != None):
		print(row[1])
		if row[5] == 'x':
			jr1_temp = jr1_df(lgn,month,year)
			pprint(jr1_temp)
			jr1_arr.append(jr1_temp)
		
		if row[6] == 'x':
			br1_temp = br1_df(lgn,month,year)
			pprint(br1_temp)
			br1_arr.append(br1_temp)
		
		if row[7] == 'x':
			db1_temp = db1_df(lgn,month,year)
			pprint(db1_temp)
			db1_arr.append(db1_temp)
		
		if row[8] == 'x':
			br2_temp = br2_df(lgn,month,year)
			pprint(br2_temp)
			br2_arr.append(br2_temp)
		
		if row[9] == 'x':
			db2_temp = db2_df(lgn,month,year)
			pprint(db2_temp)
			db2_arr.append(db2_temp)
#'''

## Compile the distinct report-type arrays into complete dataframes, and print them to Excel files.
CompletedJR1 = pd.concat(jr1_arr)
CompletedDB1 = pd.concat(br1_arr)
CompletedBR1 = pd.concat(db1_arr)
CompletedBR2 = pd.concat(br2_arr)
CompletedDB2 = pd.concat(db2_arr)
CompletedJR1.to_excel(os.path.join(path,r'JR1 Usage.xlsx'), index = False, encoding = None)
CompletedDB1.to_excel(os.path.join(path,r'DB1 Usage.xlsx'), index = False, encoding = None)
CompletedBR1.to_excel(os.path.join(path,r'BR1 Usage.xlsx'), index = False, encoding = None)
CompletedBR2.to_excel(os.path.join(path,r'BR2 Usage.xlsx'), index = False, encoding = None)
CompletedDB2.to_excel(os.path.join(path,r'DB2 Usage.xlsx'), index = False, encoding = None)
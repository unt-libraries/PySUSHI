from Counter5pybr import *
from Counter4pybr import *

##Set global variables
month = 0
year = 0

#Setters for global vars
def setMonth(mnth):
	global month
	month = mnth
	print(month, year)
def setYear(yr):
	global year
	year = yr
	print(month, year)
def sweep():
	conn = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + db + ';Trusted_Connection=yes')
	sql = "SELECT rtg.Platform, rtg.TRecNum, rtg.ReportID, pss.SUSHIURL, pss.SUSHIRequestorID, pss.SUSHICustomerID, pss.SUSHIAPIKey, pss.SUSHIRequestLowercase FROM ReportsToGather AS rtg LEFT JOIN PlatformStatsSource AS pss ON rtg.TRecNum = pss.TRecNum"
	reps = pd.read_sql(sql,conn)


	jr1_arr = []
	db1_arr = []
	br1_arr = []
	db2_arr = []
	br2_arr = []
	tr_arr = []
	dr_arr = []


	for reportRow in reps.itertuples():
		'''
		reportRow[1] - Platform name
		reportRow[2] - TRecnum
		reportRow[3] - ReportID
		reportRow[4] - SUSHIURL
		reportRow[5] - SUSHIReqID
		reportRow[6] - SUSHICustID
		reportRow[7] - SUSHIAPIKey
		reportRow[8] - SUSHIReqLowercase
		#sleep(0.5)
		'''
		if(reportRow[3] == 1):
			#attempt to get the TR_J* reports for usage and turnaways.
			try:
				usage = Counter5Report(year, month, 'TR_J3', reportRow[4], reportRow[5], reportRow[6], f'{reportRow[1]} - {month}, {year}', reportRow[7], reportRow[8], trecnum = reportRow[2])
				print(f"{reportRow[1]} journal usage is empty") if (usage.reportDF.empty) else print(f"{reportRow[1]} journal usage data collected.")
				tr_arr.append(usage.reportDF)
				
				turnaways = Counter5Report(year, month, 'TR_J2', reportRow[4], reportRow[5], reportRow[6], f'{reportRow[1]} - {month}, {year}', reportRow[7], reportRow[8], trecnum = reportRow[2])
				print(f"{reportRow[1]} journal turnaways is empty") if (turnaways.reportDF.empty) else print(f"{reportRow[1]} journal turnaways data collected.")
				tr_arr.append(turnaways.reportDF)
				#break
			except:
				logging.warning(f"usage-gathering error for {reportRow[1]} TR_J.")
			#Attempt to get the usage and turnaways data for the TR_B* reports
			try:
				usage = Counter5Report(year, month, 'TR_B3', reportRow[4], reportRow[5], reportRow[6], f'{reportRow[1]} - {month}, {year}', reportRow[7], reportRow[8], trecnum = reportRow[2])
				print(f"{reportRow[1]} book usage is empty") if (usage.reportDF.empty) else print(f"{reportRow[1]} book usage data collected.")
				tr_arr.append(usage.reportDF)
				
				turnaways = Counter5Report(year, month, 'TR_B2', reportRow[4], reportRow[5], reportRow[6], f'{reportRow[1]} - {month}, {year}', reportRow[7], reportRow[8], trecnum = reportRow[2])
				print(f"{reportRow[1]} book turnaways is empty") if (turnaways.reportDF.empty) else print(f"{reportRow[1]} book turnaways data collected.")
				tr_arr.append(turnaways.reportDF)
				#break
			except:
				logging.warning(f"usage-gathering error for {reportRow[1]} TR_B.")
		##	
		#########################	if this report is a DR, attempt to get the DR_D1 report.
		elif(reportRow[3] == 2):
			try:
				usage = Counter5Report(year, month, 'DR_D1', reportRow[4], reportRow[5], reportRow[6], f'{reportRow[1]} - {month}, {year}', reportRow[7], reportRow[8])
				print(f"{reportRow[1]} database usage is empty") if (usage.reportDF.empty) else print(f"{reportRow[1]} database usage data collected.")
				dr_arr.append(usage.reportDF)
				#break
			except:
				logging.warning(f"usage-gathering error for {reportRow[1]} DR_D1.")
		##
		#########################If this report is marked as a JR1, collect as a JR1.
		elif(reportRow[3] == 3):
			try:
				lgn = (reportRow[4], reportRow[5], reportRow[6])
				frame_platform_name(reportRow[1])
				jr1_temp = jr1_df(lgn,month,year)
				print(f"{reportRow[1]} JR1 is empty") if (jr1_temp.empty) else print(f"{reportRow[1]} JR1 data collected.")
				jr1_arr.append(jr1_temp)
			except:
				logging.warning(f"usage-gathering error for {reportRow[1]} JR1.")
		##
		#If the reportcode is 6, indicating a DB1 report,
		elif(reportRow[3] == 6):
			try:
				lgn = (reportRow[4], reportRow[5], reportRow[6])
				frame_platform_name(reportRow[1])
				db1_temp = db1_df(lgn,month,year)
				print(f"{reportRow[1]} DB1 is empty") if (db1_temp.empty) else print(f"{reportRow[1]} DB1 data collected.")
				db1_arr.append(db1_temp)
			except:
				logging.warning(f"usage-gathering error for {reportRow[1]} DB1.")
		##
		##If the reportCode is a 5, get a BR2 report.
		elif(reportRow[3] == 5):
			try:
				lgn = (reportRow[4], reportRow[5], reportRow[6])
				frame_platform_name(reportRow[1])
				br2_temp = br2_df(lgn,month,year)
				print(f"{reportRow[1]} BR2 is empty") if (br2_temp.empty) else print(f"{reportRow[1]} BR2 data collected.")
				br2_arr.append(br2_temp)
			except:
				logging.warning(f"usage-gathering error for {reportRow[1]} BR2.")
		##
		#########################If the reportcode is 4, indicating a BR1 report,
		elif(reportRow[3] == 4):
			try:
				lgn = (reportRow[4], reportRow[5], reportRow[6])
				frame_platform_name(reportRow[1])
				br1_temp = br1_df(lgn,month,year)
				print(f"{reportRow[1]} BR1 is empty") if (br1_temp.empty) else print(f"{reportRow[1]} BR1 data collected.")
				br1_arr.append(br1_temp)
				
			except:
				logging.warning(f"usage-gathering error for {reportRow[1]} BR1.")
	##






	##	###########################################################################################
	# Instantiate the cursor that will be used to write to the database.
	cursor = conn.cursor()

	################################################################################################
	if tr_arr:
		#Concatenate the collected TR reports.
		CompletedTR = pd.concat(tr_arr)
		CompletedTR.drop_duplicates(keep=False,inplace=True)
		pprint(CompletedTR)

		#iterate through the entire CompletedTR table and INSERT each row into the TR table in the LUC database
		for index,row in CompletedTR.iterrows():
			cursor.execute('INSERT INTO LUC.dbo.TR([Platform],[TRecNum], [Title],[Data_Type],[Access_Type], [Publisher], [ItemDataType], [Print_ISSN], [Online_ISSN], [DOI], [ISBN], [Proprietary], [Date], [Category], [Metric_Type], [Count]) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', 
							row['Platform'],
							row['TRecNum'], #This is the addition.
							row['Title'],
							row['Data_Type'],
							row['Access_Type'],
							row['Publisher'],
							row['ItemDataType'],
							row['Print_ISSN'],
							row['Online_ISSN'],
							row['DOI'],
							row['ISBN'],
							row['Proprietary'],
							row['Date'],
							row['Category'],
							row['Metric_Type'],
							row['Count'])
			conn.commit()
	##
	if dr_arr:
		#Concatenate the collected DR reports
		CompletedDR = pd.concat(dr_arr)
		CompletedDR.drop_duplicates(keep=False,inplace=True)
		pprint(CompletedDR)

		#as was done iteratively for the TR reports, do so for the DR reports.
		for index,row in CompletedDR.iterrows():
			cursor.execute('INSERT INTO LUC.dbo.DR([Platform],[Database],[Publisher],[Publisher_ID],[Access_Method],[Date],[Metric_Type],[Count]) values (?,?,?,?,?,?,?,?)', 
							row['Platform'],
							row['Database'],
							row['Publisher'],
							row['Publisher_ID'],
							row['Access_Method'],
							row['Date'],
							row['Metric_Type'],
							row['Count'])
			conn.commit()	
	##	
	if jr1_arr:
		#Concatenate the collected TR reports.
		CompletedJR1 = pd.concat(jr1_arr)
		CompletedJR1.drop_duplicates(keep=False,inplace=True)
		pprint(CompletedJR1)

		#iterate through the entire CompletedTR table and INSERT each row into the TR table in the LUC database
		for index,row in CompletedJR1.iterrows():
			cursor.execute('INSERT INTO LUC.dbo.JR1([ItemPlatform],[ItemName],[ItemDataType],[Print_ISBN],[Print_ISSN],[Online_ISSN],[DOI],[Proprietary],[Date],[Category],[MetricType],[Count]) values (?,?,?,?,?,?,?,?,?,?,?,?)', 
							row['ItemPlatform'],
							row['ItemName'],
							row['ItemDataType'], 
							row['Print_ISBN'], 
							row['Print_ISSN'], 
							row['Online_ISSN'], 
							row['DOI'], 
							row['Proprietary'],
							row['Date'],
							row['Category'],
							row['MetricType'],
							row['Count'])
			conn.commit()	

	if br1_arr:
		#Concatenate the collected TR reports.
		CompletedBR1 = pd.concat(br1_arr)
		CompletedBR1.drop_duplicates(keep=False,inplace=True)
		pprint(CompletedBR1)

		#iterate through the entire CompletedTR table and INSERT each row into the TR table in the LUC database
		for index,row in CompletedBR1.iterrows():
			cursor.execute('INSERT INTO LUC.dbo.BR1([ItemPlatform],[ItemName],[ItemPublisher],[Print_ISBN],[Online_ISBN],[Proprietary],[ItemDataType],[Category],[MetricType],[Date],[Count]) values (?,?,?,?,?,?,?,?,?,?,?)', 
							row['ItemPlatform'],
							row['ItemName'],
							row['ItemPublisher'], 
							row['Print_ISBN'],
							row['Online_ISBN'], 						
							row['Proprietary'],
							row['ItemDataType'],
							row['Category'],
							row['MetricType'],
							row['Date'],
							row['Count'])
			conn.commit()

	if db1_arr:
		#Concatenate the collected TR reports.
		CompletedDB1 = pd.concat(db1_arr)
		CompletedDB1.drop_duplicates(keep=False,inplace=True)
		pprint(CompletedDB1)

		#iterate through the entire CompletedTR table and INSERT each row into the TR table in the LUC database
		for index,row in CompletedDB1.iterrows():
			cursor.execute('INSERT INTO LUC.dbo.DB1([ItemPlatform],[ItemName],[ItemPublisher],[ItemDataType],[Category],[MetricType],[Date],[Count]) values (?,?,?,?,?,?,?,?)', 
							row['ItemPlatform'],
							row['ItemName'],
							row['ItemPublisher'], 
							row['ItemDataType'],
							row['Category'],
							row['MetricType'],
							row['Date'],
							row['Count'])
			conn.commit()
		
	if br2_arr:
		#Concatenate the collected TR reports.
		CompletedBR2 = pd.concat(br2_arr)
		CompletedBR2.drop_duplicates(keep=False,inplace=True)
		pprint(CompletedBR2)

		#iterate through the entire CompletedTR table and INSERT each row into the TR table in the LUC database
		for index,row in CompletedBR2.iterrows():
			cursor.execute('INSERT INTO LUC.dbo.BR2([ItemPlatform],[ItemName],[ItemPublisher],[ItemDataType],[Print_ISBN],[Online_ISBN],[DOI],[Category],[MetricType],[Date],[Count]) values (?,?,?,?,?,?,?,?)', 
							row['ItemPlatform'],
							row['ItemName'],
							row['ItemPublisher'], 
							row['ItemDataType'],
							row['Print_ISBN'],
							row['Online_ISBN'],
							row['DOI'],
							row['Category'],
							row['MetricType'],
							row['Date'],
							row['Count'])
			conn.commit()
	#

	#close the pyodbc cursor and the database connection.	
	cursor.close()
	conn.close()
##

if __name__ == "__main__": 
	root = tk.Tk()
	root.title("PySushi")

	tk.Button(root, text = "January", command = lambda : setMonth(1)).grid(row=0, column=0)
	tk.Button(root, text = "February", command = lambda : setMonth(2)).grid(row=1, column=0)
	tk.Button(root, text = "March", command = lambda : setMonth(3)).grid(row=2, column=0)
	tk.Button(root, text = "April", command = lambda : setMonth(4)).grid(row=3, column=0)
	tk.Button(root, text = "May", command = lambda : setMonth(5)).grid(row=4, column=0)
	tk.Button(root, text = "June", command = lambda : setMonth(6)).grid(row=5, column=0)
	tk.Button(root, text = "July", command = lambda : setMonth(7)).grid(row=6, column=0)
	tk.Button(root, text = "August", command = lambda : setMonth(8)).grid(row=7, column=0)
	tk.Button(root, text = "September", command = lambda : setMonth(9)).grid(row=8, column=0)
	tk.Button(root, text = "October", command = lambda : setMonth(10)).grid(row=9, column=0)
	tk.Button(root, text = "November", command = lambda : setMonth(11)).grid(row=10, column=0)
	tk.Button(root, text = "December", command = lambda : setMonth(12)).grid(row=11, column=0)

	tk.Label(text = "      ").grid(row=0, column=1)
	tk.Button(root, text = "2014", command = lambda : setYear(2014)).grid(row=0, column=3)
	tk.Button(root, text = "2015", command = lambda : setYear(2015)).grid(row=1, column=3)
	tk.Button(root, text = "2016", command = lambda : setYear(2016)).grid(row=2, column=3)
	tk.Button(root, text = "2017", command = lambda : setYear(2017)).grid(row=3, column=3)
	tk.Button(root, text = "2018", command = lambda : setYear(2018)).grid(row=4, column=3)
	tk.Button(root, text = "2019", command = lambda : setYear(2019)).grid(row=5, column=3)
	tk.Button(root, text = "2020", command = lambda : setYear(2020)).grid(row=6, column=3)

	tk.Label(text = "      ").grid(row=0, column=4)
	tk.Button(root, text = "Gather", command = lambda : sweep()).grid(row=2, column=5)
	root.mainloop()

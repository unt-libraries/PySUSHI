'''
Author: 				Chris Hergert
Date Created: 			10/1/2019
Date Last Modified:		11/11/2020
Created under Open Commons III License for The University Of North Texas Libraries
'''
from BaseLibPybr import *


class Counter5Report:
	
	def __init__(self, year, month, report, url, requestor_id, customer_reference, platform_name = None, api_key = None, request_lowercase = '-', data_type = None, access_type = None, print_url = False, trecnum = None, platform_param = '-'):
		###=======================================================================================================
		###Initilize flags to None.
		self.data_type_filter = None
		self.access_type_filter = None
		self.valid_report = False ### IT initializes on acceptance of report type to True, but how do we want to use this flag?
		self.platformNeededFlag = False ##For the platforms that need a "platform" param, it's built into the api_key and this will indicate that the request needs to be made with a different encoding than the requests library default.
		
		###=======================================================================================================
		### Initialize the columnset based on the 'report' string, and if that report string is unrecognized, inform the user.
		report = report.upper()
		if report in report_set:
			self.valid_report = True
			if (report == "TR_J3"): self.colset = TR_COLUMNS
			if (report == "TR"): self.colset = TR_COLUMNS
			if (report == "DR_D1"): self.colset = DR_D1_COLUMNS
			if (report == "DR"): self.colset = DR_D1_COLUMNS
			if (report == "TR_B3"): self.colset = TR_COLUMNS
			if (report == "TR_J2"): self.colset = TR_COLUMNS
			if (report == "TR_B2") : self.colset = TR_COLUMNS
			if (report == "TR_J"): self.colset = TR_COLUMNS
			if (report == "TR_B") : self.colset = TR_COLUMNS
			if (report == "IR_M1") : self.colset = IR_COLUMNS
			
			## Now that the colset is init'ed, generate the empty dataframe
			self.reportDF = pd.DataFrame(columns = self.colset)
			
			## If the flag is up for making the report request in lowercase, make the conversion.
			if ((request_lowercase == 1) or (request_lowercase == '1')):
				report = report.lower()
			
			## If there are any filters activated in the parameters, populate the filter switches.
			if report == "TR":
				if ((data_type != None) and (data_type.upper() == "BOOK")) : self.data_type_filter = "Book"
				if ((data_type != None) and (data_type.upper() == "JOURNAL")) : self.data_type_filter = "Journal"
				if ((access_type != None) and (access_type.upper() == "REGULAR")) : self.access_type_filter = "Regular"
				if ((access_type != None) and (access_type.upper() == "CONTROLLED")) : self.access_type_filter = "Controlled"
				if ((access_type != None) and (access_type.upper() == "OA_GOLD")) : self.access_type_filter = "OA_Gold"
		else:
			print(f'{report} not recognized')
		#
		###=======================================================================================================
		###  Initialize the server request params
		print(year, month)
		start_date = datetime.date(year,month,1)
		end_date = datetime.date(year, month, calendar.monthrange(year,month)[1])
		frame_platform_name(platform_name)

		### Generate the request URL parameters, then the params to customize the request
		url_params = {"url": url, "report": report}
		req_params = {
				"begin_date": start_date,
				"end_date": end_date,
			}
		if requestor_id != '-' and requestor_id:
			req_params["requestor_id"] = requestor_id
			
		if customer_reference != '-' and customer_reference:
			req_params["customer_id"] = customer_reference
			
		if api_key != '-' and api_key:
			if "&platform" in api_key:
				if api_key.split("&platform=")[0]:
					req_params["api_key"] = api_key.split("&platform=")[0]
				if api_key.split("&platform=")[1]:
					req_params["platform"] = api_key.split("&platform=")[1]
				self.platformNeededFlag = True
			else:
				req_params["api_key"] = api_key
		if platform_param != '-' and platform_param != None:
			req_params["platform"] = platform_param
		if self.data_type_filter != None:
			req_params["Data_Type"] = self.data_type_filter
		if self.access_type_filter != None:
			req_params["Access_Type"] = self.access_type_filter
		
		###=======================================================================================================
		### Make the initial server request, then save it as 'response'
		if self.platformNeededFlag == True:
			RequestHeaders={"User-Agent": "UNT_LUC", 'Content-Type': 'application/json;charset=UTF-8'}
		else:
			RequestHeaders={"User-Agent": "UNT_LUC"}
			
		response = requests.get(
			"{url}/reports/{report}".format(**url_params),
			params=req_params,
			headers=RequestHeaders,
			verify=True,
		)
		
		### Wait while the server queues and dequeues the report request, then request again.
		time.sleep(3)
		self.response = requests.get(
			"{url}/reports/{report}".format(**url_params),
			params=req_params,
			headers={"User-Agent": "UNT_LUC"},
			verify=True,
		)

		###=======================================================================================================
		### Convert the request response from a JSON load into a python dictionary.
		try:
			self.repo = (self.response).json()
			#pprint(self.repo) #print the entire report, in the raw JSON-tree format.

			## If there's a fatal exception at the top level, e.g. "the requestor is not authorized to access usage for this institution"
			if 'Exception' in self.repo:
				try:
					pprint(self.repo['Exception']['Message'])
				except:
					logging.warning('Fatal exception in request response, but exception message not included in response.')
			
			## If there's an exception lower in the report header, e.g. "No usage for this time range", flag and log that error message
			elif ('Exceptions' in self.repo['Report_Header']) and (self.repo['Report_Header']['Exceptions'] != ''):
				try:
					logging.warning(self.repo['Report_Header']['Exceptions'][0]['Message'])
					#print(response.content)
				except:
					logging.warning('Exception message not present in report header')
			
			## If this report just outright has no items present, but there wasn't a "No Usage" exception.
			elif self.repo['Report_Items'] == []:
				logging.warning('There are no items in this report, but no error messages.')
			
			## If no exceptions are present, parse the report and complete.
			else:
				try:
					if report.upper() == "TR_J3":
						self.data_type_filter = "Journal"
						self.trparse(report)
						self.reportDF.insert(4, 'TRecNum', trecnum)
					elif report.upper() == "DR_D1":
						self.drd1parse(report)
						self.reportDF.insert(3, 'TRecNum', trecnum)
					elif report.upper() == "DR":
						#pass
						self.drd1parse(report)
						self.reportDF.insert(3, 'TRecNum', trecnum)
					elif report.upper() == "TR":
						self.fulltrparse(report)
						self.reportDF.insert(4, 'TRecNum', trecnum)
					elif report.upper() == "TR_B3":
						self.data_type_filter = "Book"
						self.trparse(report)
						self.reportDF.insert(4, 'TRecNum', trecnum)
					elif report.upper() == "TR_B2":
						self.data_type_filter = "Book"
						self.trparse(report)
						self.reportDF.insert(4, 'TRecNum', trecnum)
					elif report.upper() == "TR_J2":
						self.data_type_filter = "Journal"
						self.trparse(report)
						self.reportDF.insert(4, 'TRecNum', trecnum)
					elif report.upper() == "IR_M1":
						self.irparse(report)
						self.reportDF.insert(4, 'TRecNum', trecnum)
				except:
					logging.warning("Parsing error")
		except:
			######## (10/22/2019) TO-DO: change this to actually logging the errors to an output file as well as console-logging
			logging.warning("report json conversion failed.")
		print_url = True
		if (print_url == True) : print(response.request.url)	#print the request string URL.
		#pprint(self.reportDF)			#print the output dataframe.
	###################################################################################################################################################################################################
	###################################################################################################################################################################################################
	###################################################################################################################################################################################################
	###################################################################################################################################################################################################
	
	###################################################################################################################################################################################################
	###
	###################################################################################################################################################################################################
	def trparse(self, report_type):
		## Loop through the book/journal nodes
		for report_item in self.repo['Report_Items']:
			
			## Generate a fresh tuple of row length and populate it with the top-level identifiers
			temp = [''] * len(self.colset)
			temp[self.colset.index('Title')] = report_item['Title']
			temp[self.colset.index('Platform')] = report_item['Platform']
			
			## If a filter is applied, use this to populate the Date_Type column.
			if self.access_type_filter != None:
				temp[self.colset.index("Access_Type")] = self.access_type_filter
			if self.data_type_filter != None:
				temp[self.colset.index("Data_Type")] = self.data_type_filter
			## Look for Publisher and Access_Type nodes, and populate the column if present.
			for label in ["Publisher", "Access_Type"]:
				try:
					temp[self.colset.index(label)] = report_item[label]
				except:
					pass
			
			## Check for an Item_ID subtree, and populate from it if one is included.
			try:
				for id in report_item['Item_ID']:
					try:
						temp[self.colset.index(id['Type'])] = id['Value']
					except:
						pass
			except:
				pass
				
			## Populate the Metric_Type fields and counts, and then load each of these nodes to the dataframe.
			for perfnode in report_item["Performance"]:
				temp[self.colset.index("Date")] = perfnode["Period"]["Begin_Date"]
				for instance in perfnode["Instance"]:
					temp[self.colset.index("Metric_Type")] = instance["Metric_Type"]
					temp[self.colset.index("Count")] = instance["Count"]
					self.reportDF.loc[len(self.reportDF)] = temp
	#
	def fulltrparse(self, report_type):
		## Loop through the book/journal nodes
		for report_item in self.repo['Report_Items']:
			
			## Generate a fresh tuple of row length and populate it with the top-level identifiers
			temp = [''] * len(self.colset)
			temp[self.colset.index('Title')] = report_item['Title']
			temp[self.colset.index('Platform')] = report_item['Platform']
			
			## If a filter is applied, use this to populate the Date_Type column.
			if self.access_type_filter != None:
				temp[self.colset.index("Access_Type")] = self.access_type_filter
			if self.data_type_filter != None:
				temp[self.colset.index("Data_Type")] = self.data_type_filter
			## Look for Publisher and Access_Type nodes, and populate the column if present.
			for label in ["Publisher", "Access_Type"]:
				try:
					temp[self.colset.index(label)] = report_item[label]
				except:
					pass
			
			## Check for an Item_ID subtree, and populate from it if one is included.
			try:
				for id in report_item['Item_ID']:
					try:
						temp[self.colset.index(id['Type'])] = id['Value']
					except:
						pass
			except:
				pass
				
			## Populate the Metric_Type fields and counts, and then load each of these nodes to the dataframe.
			for perfnode in report_item["Performance"]:
				try:
					temp[self.colset.index("Date")] = perfnode["Period"]["Begin_Date"]
					for instance in perfnode["Instance"]:
						try:
							temp[self.colset.index("Metric_Type")] = instance["Metric_Type"]
							temp[self.colset.index("Count")] = instance["Count"]
							self.reportDF.loc[len(self.reportDF)] = temp
						except:
							pass
				except:
					pass

	###################################################################################################################################################################################################
	###
	###################################################################################################################################################################################################
	def irparse(self, report_type):
		## Loop through the book/journal nodes
		for report_item in self.repo['Report_Items']:
			temp = [''] * len(self.colset)
			temp[self.colset.index('Item')] = report_item['Item']
			try:
				temp[self.colset.index('Platform')] = report_item['Platform']
				temp[self.colset.index('Publisher')] = report_item['Publisher']
				
				## Populate the Metric_Type fields and counts, and then load each of these nodes to the dataframe.
				for perfnode in report_item["Performance"]:
				
					## Attempt to set the date via the begin_date included in the JSON load.
					try:
						temp[self.colset.index("Date")] = perfnode["Period"]["Begin_Date"]
					except:
						pass
					
					##Collect the Metric_Type coding information and the count of that metric.
					for instance in perfnode["Instance"]:
						temp[self.colset.index("Metric_Type")] = instance["Metric_Type"]
						temp[self.colset.index("Count")] = instance["Count"]
						self.reportDF.loc[len(self.reportDF)] = temp
			except:
				pass
	
	###################################################################################################################################################################################################
	###
	###################################################################################################################################################################################################
	def drd1parse(self, report_type):
		for item in self.repo["Report_Items"]:
			#grab all of the fields that are constant for this report.
			temp = [''] * len(self.colset)
			
			##Grab all of the top-level data pieces.
			for topLevelNode in ("Database","Platform", "Publisher"):
				try:
					temp[self.colset.index(topLevelNode)] = item[topLevelNode]
				except:
					pass
			## Get the top-level nodes that may or may not be included: Publisher_ID and Access_Method
			try:
				temp[self.colset.index("Publisher_ID")] = item["Publisher_ID"][0]["Value"]
			except:
				pass
			 ## Try to get Access_Method, if it's included in this report_node.
			try:
				temp[self.colset.index("Access_Method")] = item["Access_Method"]
			except:
				pass
				
			## Populate the Metric_Type fields and counts, and then load each of these nodes to the dataframe.
			for perfnode in item["Performance"]:
				try:
					temp[self.colset.index("Date")] = perfnode["Period"]["Begin_Date"]
					for instance in perfnode["Instance"]:
						temp[self.colset.index("Metric_Type")] = instance["Metric_Type"]
						temp[self.colset.index("Count")] = instance["Count"]
						self.reportDF.loc[len(self.reportDF)] = temp
				except:
					logging.warning(f"Error in {item['Database']}")
	###################################################################################################################################################################################################
	### Parse out the TRJ3 reports
	###################################################################################################################################################################################################
	def trj3parse(self, report_type):
		if report_type == 'TR_J3':
			### Format the report into a dataframe
			for test in self.repo['Report_Items']:
				temp = [''] * len(self.colset)
				temp[self.colset.index('Title')] = test['Title']
				temp[self.colset.index('Access_Type')] = test['Access_Type']
				temp[self.colset.index('Platform')] = test['Platform']
				
				### Test the ID fields. The ID's that we may want are included in the colset, others are not kept.
				for id in test['Item_ID']:
					try:
						temp[self.colset.index(id['Type'])] = id['Value']
					except:
						print(id['Value'])
				for perfnode in test['Performance']:
					temp[self.colset.index('Date')] = perfnode['Period']['Begin_Date']
					for node in perfnode['Instance']:
						temp[self.colset.index('Metric_Type')] = node['Metric_Type']
						temp[self.colset.index('Count')] = node['Count']
						self.reportDF.loc[len(self.reportDF)] = temp
	###################################################################################################################################################################################################
	###
	###################################################################################################################################################################################################
	def display(self):
		print(self.reportDF)
	def SaveTo(self, filename, address = None, encode = None):
		if address == None:
			if encode == None:
				self.reportDF.to_excel(filename, encoding = encode)
	###################################################################################################################################################################################################
	###
	###################################################################################################################################################################################################
	
#

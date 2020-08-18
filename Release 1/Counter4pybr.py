from BaseLibPybr import *

#############################################################################################################################
#############################################################################################################################
#
#############################################################################################################################
def raw_xml_rep( lg, mnth = 3, yr = 2019, report = "JR1", release = 4):
	'''
	#Retrive the XML-formatted report from the given SUSHI server
	lg -> a 3-member tuple of strings that holds the SUSHI server's location and login credentials
		lg[0] -> (str) The SUSHI server URL
		lg[1] -> (str) The SUSHI requestor ID
		lg[2] -> (str) the SUSHI customer ID
	mnth -> (int) the month of the requested report
	yr -> (int) the year of the requested report
	report -> (str) the type of report requested
	release -> (int) the COUNTER release of the requested report. Currently defaults to 4.
	'''
	wsdl_url = lg[0]
	start_date = datetime.date(yr,mnth,1)
	end_date = datetime.date(yr, mnth, calendar.monthrange(yr,mnth)[1])
	requestor_id = lg[1]
	requestor_email=None
	requestor_name=None
	customer_reference=lg[2]
	customer_name=None
	sushi_dump=False
	verify=True
	#=======================================================================
	rooty = etree.Element("{%(SOAP-ENV)s}Envelope" % NS, nsmap=NS)	#This is the root of the tree that we're going to pass as the header to the SUSHI server.
	body = etree.SubElement(rooty, "{%(SOAP-ENV)s}Body" % NS)		#build the body node for the SUSHI request tree
	timestamp = pendulum.now("UTC").isoformat()						#Timestamp the report request
	rr = etree.SubElement(
		body,
		"{%(sushicounter)s}ReportRequest" % NS,
		{"Created": timestamp, "ID": str(uuid.uuid4())},
	)
	#=======================================================
	
	#Create the XML outline that's going to be submitted with the POST request to the SUSHI server for population.

	req = etree.SubElement(rr, "{%(sushi)s}Requestor" % NS)	# Link to the sushi schema and add this link into the etree with the 'Requestor' tag
	rid = add_sub(req, "{%(sushi)s}ID" % NS, requestor_id)	# Create a new subelement of 'req' tagged 'ID' containing the requestor ID
	#-----------
	req_name_element = add_sub(req, "{%(sushi)s}Name" % NS, requestor_name)		#Create a child node of 'req' tagged 'Name', holding the requestor name (Institution name)
	req_email_element = add_sub(req, "{%(sushi)s}Email" % NS, requestor_email)	#Create a child node of 'req' tagged 'Email', holding the requestor's provided email address
	#-----------
	cust_ref_elem = etree.SubElement(rr, "{%(sushi)s}CustomerReference" % NS)	#Create a child node of 'req' tagged 'CustomerReference'
	cID = add_sub(cust_ref_elem, "{%(sushi)s}ID" % NS, customer_reference)		#Create a child node of 'CustomerReference', tagged 'ID' and holding the cutomer ref ID

	cust_name_elem = add_sub(cust_ref_elem, "{%(sushi)s}Name" % NS, customer_name)

	report_def_elem = etree.SubElement(	  rr,   "{%(sushi)s}ReportDefinition" % NS,   Name=report,   Release=str(release)  )
	filters = etree.SubElement(report_def_elem, "{%(sushi)s}Filters" % NS)
	udr = etree.SubElement(filters, "{%(sushi)s}UsageDateRange" % NS)
	beg = etree.SubElement(udr, "{%(sushi)s}Begin" % NS)

	beg.text = start_date.strftime("%Y-%m-%d")
	end = etree.SubElement(udr, "{%(sushi)s}End" % NS)
	end.text = end_date.strftime("%Y-%m-%d")
	payload = etree.tostring(   rooty,   pretty_print=True,   xml_declaration=True,   encoding="utf-8" )
	#=============================================================
	headers = {
		"SOAPAction": '"SushiService:GetReportIn"',
		"Content-Type": "text/xml; charset=UTF-8",
		"Content-Length": str(len(payload)),
	}
	
	response = requests.post(url=wsdl_url, headers=headers, data=payload, verify=verify) # Post the SUSHI tree to the server and save teh response as 'response'
	print(response.request.url)
	if sushi_dump:
		logger.debug(
			"SUSHI DUMP: request: %s \n\n response: %s", payload, response.content
		)
	#rt = etree.fromstring(response.content) # tombstoned for testing
	return response.content
#############################################################################################################################
#############################################################################################################################
#
#############################################################################################################################
def jr1_df(lgn, month, year):
	'''
	Pull down a single month's JR1 report, and return it as a DataFrame
	-----------------------------------------------------------------------------
	lgn -> a 3-member tuple of strings that holds the SUSHI server's location and login credentials
		lgn[0] -> (str) The SUSHI server URL
		lgn[1] -> (str) The SUSHI requestor ID
		lgn[2] -> (str) the SUSHI customer ID
	month -> (int) the month of the requested report
	year -> (int) the year of the requested report
	'''
	colset = [	'ItemName', 'ItemPlatform', 'ItemDataType', 'Print_ISBN', 
				'Print_ISSN', 'Online_ISSN', 'DOI', 'Proprietary', 
				'Date', 'Category', 'MetricType', 'Count']
	retdf = pd.DataFrame(columns = colset)
	date = datetime.datetime(int(year), int(month), 1)
	
	
	try:
		repo = etree.fromstring(raw_xml_rep( lg = lgn, mnth = month, yr = year, report = 'JR1'))
		rootp = repo[0][0][3][0][1]
		
		for rrnode in rootp.findall('{http://www.niso.org/schemas/counter}ReportItems'):
			#establish the new item's baseline vector
			temp = [''] * len(colset)
			temp[colset.index('Date')] = date
			
			#run through the nodes that have either no subnodes or have data with type/value subnode pairs( e.g. Print_ISBN, DOI, etc.)
			for top_level_node in rrnode:
				tag  = removeURLstring(top_level_node.tag)
				if tag in colset:
					temp[colset.index(tag)] = top_level_node.text
				if tag == 'ItemIdentifier':
					type = top_level_node.find('{http://www.niso.org/schemas/counter}Type').text
					temp[colset.index(type)] = top_level_node.find('{http://www.niso.org/schemas/counter}Value').text
					
			#get only the ItemPerformance subnodes
			for perfnode in rrnode.findall('{http://www.niso.org/schemas/counter}ItemPerformance'):
				
				#populate the Category column with the top-level node
				temp[colset.index('Category')] = perfnode.find('{http://www.niso.org/schemas/counter}Category').text
				
				#loop through the instance subnodes in each ItemPerformance node
				for instance_node in perfnode.findall('{http://www.niso.org/schemas/counter}Instance'):
						temp[colset.index('MetricType')] = instance_node.find('{http://www.niso.org/schemas/counter}MetricType').text
						temp[colset.index('Count')] = instance_node.find('{http://www.niso.org/schemas/counter}Count').text
						
						# Load this completed temp vector into the array.
						retdf.loc[len(retdf)] = temp
					
	# Catch potential errors and return an error flag and an empty dataframe. This matches what's sometimes
	# returned from error-ed out SUSHI requests, so we'll just check for this format of dataframe to determine when an error has occurred
	except:
		print("Error in handling",lgn)
		try:
			print(etree.tostring( etree.fromstring(raw_xml_rep( lg = lgn, mnth = month, yr = year, report = 'JR1')), pretty_print = True, encoding = 'unicode')) 		#look at the whole tree
		except:
			print('invalid xml returned');
	return retdf
#############################################################################################################################
#############################################################################################################################
#
#############################################################################################################################
def mr1_df(lgn,month,year):
	'''
	Pull down a single month's MR1 report, and return it as a DataFrame
	-----------------------------------------------------------------------------
	lgn -> a 3-member tuple of strings that holds the SUSHI server's location and login credentials
		lgn[0] -> (str) The SUSHI server URL
		lgn[1] -> (str) The SUSHI requestor ID
		lgn[2] -> (str) the SUSHI customer ID
	month -> (int) the month of the requested report
	year -> (int) the year of the requested report
	'''
	# Initialize the report in var 'repo'
	repo = etree.fromstring(raw_xml_rep( lg = lgn, mnth = month, yr = year, report = 'MR1'))
	month_date = pd.Period(freq = 'M', year = year , month = month)


	# Initialize the colset, df, and report data parent node
	colset = ['ItemName', 'ItemPlatform', 'ItemPublisher', 'Category', 'MetricType', 'Date', 'Count']
	df = pd.DataFrame(columns = colset)
	try:
		rootp = repo[0][0][3][0][1] #	set up the root node for the actual report content
		
		for report_item in rootp.getchildren(): #	repo[0][0][3][0][1] is the parent node to all of the ReportItem nodes
			temp = [''] * len(colset)				#	Create the holder list that will hold this row and go into the DF 
			if report_item.tag == '{http://www.niso.org/schemas/counter}ReportItems':
				for data_node in report_item.getchildren():
					
					# Loop through all of the columns and grab the first-level texts whose tag matches a column name
					for colname in colset:
						if "{http://www.niso.org/schemas/counter}"+colname == data_node.tag:
							temp[colset.index(colname)] = data_node.text
					
					# Dig into the data_node's ItemPerformance subnode and grab the category, metric type, and count data points
					if data_node.tag == '{http://www.niso.org/schemas/counter}ItemPerformance':
						
						# Grab the top-level category data point
						categ = instance = data_node.find('{http://www.niso.org/schemas/counter}Category')
						temp[colset.index('Category')] = categ.text
						
						# Drill into the Instance node and find the 'MetricType' and 'Count' nodes, then add them to 'temp'
						instance = data_node.find('{http://www.niso.org/schemas/counter}Instance')
						metric_type = instance.find('{http://www.niso.org/schemas/counter}MetricType').text
						ct = instance.find('{http://www.niso.org/schemas/counter}Count').text
						temp[colset.index('MetricType')] = metric_type
						temp[colset.index('Date')] = month_date.strftime('%b-%Y')
						temp[colset.index('Count')] = ct
						
			# Add the completed row to the dataframe
			if temp != [''] * len(colset):
				df.loc[len(df)] = temp
		return df
	except:
		return df

#############################################################################################################################
#############################################################################################################################
#
#############################################################################################################################
def br1_df(lgn,month,year):
	# download report, define dataframe to populate and return, and format date for date column
	colset = ['ItemName', 'ItemPlatform', 'ItemPublisher', 'Print_ISBN', 'Print_ISSN', 'Online_ISSN', 'Online_ISBN',  'Proprietary', 'ItemDataType', 'Category', 'MetricType', 'Date', 'Count']
	df = pd.DataFrame(columns = colset)
	date = datetime.date(int(year), int(month), 1)
	
	try:
		repo = etree.fromstring(raw_xml_rep( lg = lgn, mnth = month, yr = year, report = 'BR1'))
		rootp = repo[0][0][3][0][1]
		for report_row_node in rootp:
			temp = [''] * len(colset)
			if report_row_node.tag == '{http://www.niso.org/schemas/counter}ReportItems':
				# Create a line-holder vector, then go into the actual data nodes for this row
				temp[colset.index('Date')] = date
				for data_node in report_row_node:
				
					 # STEP I: Loop through all of the columns and grab the first-level texts whose tag matches a column name
					 #NOTE: Might be better to replace with a parser to remove URL from XML tag and use "if-in" to check colset for remaining tag-text. 
					for colname in colset:
						if "{http://www.niso.org/schemas/counter}"+colname == data_node.tag:
							temp[colset.index(colname)] = data_node.text
							
					# STEP II: in each of the 3 ItemIdentifier nodes, grab the Type and Values subnodes' texts and 
					 #use the Type to identify the correct column for the Value text.
					if data_node.tag == '{http://www.niso.org/schemas/counter}ItemIdentifier':
						type = data_node.find('{http://www.niso.org/schemas/counter}Type').text
						value = data_node.find('{http://www.niso.org/schemas/counter}Value').text
						temp[colset.index(type)] = value
						
					#STEP III: grab the Category subnode's text and populate that column, then
					 #search the Instance subnode and take the values of its MetricType and Count subnodes.
					 #Use these values to populate the appropriate spaces in the array.
					if data_node.tag == '{http://www.niso.org/schemas/counter}ItemPerformance':
						temp[colset.index('Category')] = data_node.find('{http://www.niso.org/schemas/counter}Category').text
						instance = data_node.find('{http://www.niso.org/schemas/counter}Instance')
						temp[colset.index('MetricType')] = instance.find('{http://www.niso.org/schemas/counter}MetricType').text
						temp[colset.index('Count')] = instance.find('{http://www.niso.org/schemas/counter}Count').text

			# Add the completed row to the dataframe
			if temp != [''] * len(colset):
				df.loc[len(df)] = temp
		return df
	except:
		return df
#############################################################################################################################
#############################################################################################################################
#
#############################################################################################################################
def db1_df(lgn, month, year):
	date = datetime.datetime(int(year), int(month), 1)

	# Define the dataframe that we'll start with
	colset = ['ItemName',
			  'ItemPlatform',
			  'ItemPublisher',
			  'Print_ISBN',
			  'Online_ISBN',
			  'Print_ISSN',
			  'Online_ISSN',			  
			  'Proprietary',
			  'ItemDataType',
			  'Category',
			  'MetricType', 
			  'Date',
			  'Count']
	df = pd.DataFrame(columns = colset)
	try:
		# Define the report tree that we'll start with.
		repo = etree.fromstring(raw_xml_rep( lg = lgn, mnth = month, yr = year, report = 'DB1'))
		rootp = repo[0][0][3][0][1]
		
		# loop through every report_row_node in the report tree
		for report_row_node in rootp:
			temp = [''] * len(colset)
			
			#if the report_row_node is tagged as a data row, drill down into it and set up the initial temp array.
			if report_row_node.tag == '{http://www.niso.org/schemas/counter}ReportItems':
				temp[colset.index('Date')] = date
				
				
				#loop through the subnodes of the rrnode to get the top-level data elements that are immediate children of this report_row_node's root node (name, platform, publisher, etc.)
				for data_node in report_row_node:
					for colname in colset:
						if "{http://www.niso.org/schemas/counter}"+colname == data_node.tag:
							temp[colset.index(colname)] = data_node.text
				
				
				#loop through the nodes again to isolate the actual item performance nodes, then drill down into these and loop through the instances in each performance node.
				for data_node in report_row_node:
					if data_node.tag == '{http://www.niso.org/schemas/counter}ItemPerformance':
						
						#grab the top-level node that holds the category data.
						temp[colset.index('Category')] = data_node.find('{http://www.niso.org/schemas/counter}Category').text
						
						#loop through the instance and populate the resident data into the temp array.
						for instance_node in data_node:
							if instance_node.tag == '{http://www.niso.org/schemas/counter}Instance':
								temp[colset.index('MetricType')] = instance_node.find('{http://www.niso.org/schemas/counter}MetricType').text
								temp[colset.index('Count')] = instance_node.find('{http://www.niso.org/schemas/counter}Count').text
								
								#load the completed array into the main dataframe.
								df.loc[len(df)] = temp
		#Return the completed dataframe
		return df
	except:
		#If there was an error, return the empty dataframe
		return df
#############################################################################################################################
#############################################################################################################################
#
#############################################################################################################################
def br2_df(lgn, month, year):
	# Define our master DataFrame
	colset = ['ItemName',
			  'ItemPlatform',
			  'ItemPublisher',
			  'Print_ISBN',
			  'Online_ISBN',
			  'Print_ISSN',
			  'Online_ISSN',			  
			  'Proprietary',
			  'ItemDataType',
			  'Category',
			  'MetricType', 
			  'Date',
			  'Count']
	df = pd.DataFrame(columns = colset)
	date = datetime.datetime(int(year), int(month), 1)
	try:
		#Pull down the XML report and parse off the trivial content
		repo = etree.fromstring(raw_xml_rep( lg = lgn, mnth = month, yr = year, report = 'BR2'))
		rootp = repo[0][0][3][0][1]
		#print(etree.tostring(repo[0][0][3][0][1],pretty_print = True, encoding = 'unicode')) 		#look at the whole tree


		#Loop through all the first-level nodes in the rootp tree
		for report_row_node in rootp:
			
			# Create the temp dataframe that we'll use
			temp = [''] * len(colset)
			
			if report_row_node.tag == '{http://www.niso.org/schemas/counter}ReportItems':
				temp[colset.index('Date')] = date
				
				#loop through the subnodes of the rrnode to get the first-level data elements (name, platform, publisher, etc.)
				for data_node in report_row_node:
					# Extract the tag from the URL-prefixed tagstring
					tag  = removeURLstring(data_node.tag)
					
					#If the tag is a column in the colset, just automatically add that node's text to the temp vector
					if tag in colset:
						temp[colset.index(tag)] = data_node.text
					
					#if the node's an item ID code, drill down and grab the type and value, then populate the temp vector appropriately.
					if tag == 'ItemIdentifier':
						type = data_node.find('{http://www.niso.org/schemas/counter}Type').text
						temp[colset.index(type)] = data_node.find('{http://www.niso.org/schemas/counter}Value').text
					
				#get only the ItemPerformance subnodes
				for perfnode in report_row_node.findall('{http://www.niso.org/schemas/counter}ItemPerformance'):
					
					#populate the Category column with the top-level node
					temp[colset.index('Category')] = data_node.find('{http://www.niso.org/schemas/counter}Category').text
					
					#loop through the instance subnodes in each ItemPerformance node
					for instance_node in perfnode.findall('{http://www.niso.org/schemas/counter}Instance'):
							temp[colset.index('MetricType')] = instance_node.find('{http://www.niso.org/schemas/counter}MetricType').text
							temp[colset.index('Count')] = instance_node.find('{http://www.niso.org/schemas/counter}Count').text
							
							# Load this completed temp vector into the array.
							df.loc[len(df)] = temp 
		#
		return df
	except:
		return df
#############################################################################################################################
#############################################################################################################################
#
#############################################################################################################################
def db2_df(lgn, month, year):
	

	# Define our master DataFrame
	colset = [	'ItemName', 'ItemPlatform', 'ItemDataType', 'Print_ISBN', 'Print_ISSN','Proprietary', 'DOI', 'Date', 'Category', 'MetricType', 'Count']
	df = pd.DataFrame(columns = colset)
	date = datetime.datetime(int(year), int(month), 1)
	
	try:
		#Pull down the XML report and parse off the trivial content
		repo = etree.fromstring(raw_xml_rep( lg = lgn, mnth = month, yr = year, report = 'BR2'))
		rootp = repo[0][0][3][0][1]

		#Loop through all the first-level nodes in the rootp tree
		for report_row_node in rootp:
			
			# Create the temp dataframe that we'll use
			temp = [''] * len(colset)
			
			if report_row_node.tag == '{http://www.niso.org/schemas/counter}ReportItems':
				temp[colset.index('Date')] = date
				
				#loop through the subnodes of the rrnode to get the first-level data elements (name, platform, publisher, etc.)
				for data_node in report_row_node:
					# Extract the tag from the URL-prefixed tagstring
					tag  = removeURLstring(data_node.tag)
					
					#If the tag is a column in the colset, just automatically add that node's text to the temp vector
					if tag in colset:
						temp[colset.index(tag)] = data_node.text
					
					#if the node's an item ID code, drill down and grab the type and value, then populate the temp vector appropriately.
					if tag == 'ItemIdentifier':
						type = data_node.find('{http://www.niso.org/schemas/counter}Type').text
						temp[colset.index(type)] = data_node.find('{http://www.niso.org/schemas/counter}Value').text
					
				#get only the ItemPerformance subnodes
				for perfnode in report_row_node.findall('{http://www.niso.org/schemas/counter}ItemPerformance'):
					
					#populate the Category column with the top-level node
					temp[colset.index('Category')] = data_node.find('{http://www.niso.org/schemas/counter}Category').text
					
					#loop through the instance subnodes in each ItemPerformance node
					for instance_node in perfnode.findall('{http://www.niso.org/schemas/counter}Instance'):
							temp[colset.index('MetricType')] = instance_node.find('{http://www.niso.org/schemas/counter}MetricType').text
							temp[colset.index('Count')] = instance_node.find('{http://www.niso.org/schemas/counter}Count').text
							
							# Load this completed temp vector into the array.
							df.loc[len(df)] = temp 
		return df
	except:
		return df
#############################################################################################################################
#############################################################################################################################
#
#############################################################################################################################
def mr1_over_time(start_year, end_year, start_month, end_month, credentials, out_file = ''):
	'''
	This is the in-progress version of the MR1 over-time report harvester.
	'''
	col_set = ['ItemName', 'ItemPlatform', 'ItemPublisher', 'Category', 'MetricType']
	temp = pd.DataFrame(columns = col_set)
	for y in range (start_year, end_year +1):		# Loop through the full year range
		for m in range(start_month, end_month +1):	# loop through the full month range
			if temp.empty:	#if this is the first 
				temp = mr1_df(credentials,m,y)
				if temp.empty:
					print('This B empty.', m, y)
				else:
					print(m,y, "confirmed")
			else:
				try:
					start = time.time()
					to_add = mr1_df(credentials,m,y)
					temp = pd.concat([temp,mr1_df(credentials,m,y)], ignore_index = True)
					end = time.time()
					print(end-start, m, y)
				except:
					print(m, y, 'not available')
	temp.fillna(0, inplace = True)
	if out_file != '':
		temp.to_csv(out_file)
	return temp
#############################################################################################################################
#############################################################################################################################
#
#############################################################################################################################
def CollectAllR4Reports(recordsFile, month, year, outputName, report):
	if report == 'JR1':
	
		#Create empty base dataframe
		full_DF = jr1_df(('','',''), 7, 2019)
		
		#Loop through the records file and act on every row marked 'x' in the JR1 column (col 5)
		for row in recordsFile.itertuples():
			if row[5] =='x':
				frame_platform_name(row[1])
				
				#assemble the login mpr from the logins dataframe, then collect the report
				mpr = (str(row[2]),str(row[3]),str(row[4])) 
				try:
					temp = jr1_df(mpr, month, year)
					pprint(temp)
					
				except:
					pprint('ERROR')
				#Display the report DF, then concatenate it onto the full monthly report.
				full_DF = pd.concat([full_DF,temp], ignore_index = True)
		#write the full monthly report to a CSV.
		full_DF.to_csv(outputName)
	#--------------------------------------------------------------
	elif report == 'BR1':
		full_DF = br1_df(('','',''), 7, 2019)
		######################
		for row in recordsFile.itertuples():
			if row[6] =='x':
				frame_platform_name(row[1])
				
				#assemble the login mpr from the logins dataframe, then collect the report
				mpr = (str(row[2]),str(row[3]),str(row[4])) 
				try:
					temp = br1_df(mpr, month, year)
					pprint(temp)
					
					#Display the report DF, then concatenate it onto the full monthly report.
					full_DF = pd.concat([full_DF,temp], ignore_index = True)
				except:
					pprint('ERROR')
				
		#write the full monthly report to a CSV.
		full_DF.to_csv(outputName)
#
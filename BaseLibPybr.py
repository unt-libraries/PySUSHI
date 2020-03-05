'''
Author: 				Chris Hergert
Date Created: 			10/1/2019
Date Last Modified:		3/5/2019
'''

import calendar
import collections
import datetime
import logging
from lxml import etree
from lxml import objectify
import pandas as pd
import pendulum
from pprint import pprint
import requests
import os
import sys
import time
import uuid
import pyodbc
from variablesReference import *



def removeURLstring(text):
	pref = '{http://www.niso.org/schemas/counter}'
	if text.startswith(pref):
		return text[len(pref):]
	else:
		return text

def is_in_cols(text, cols):
	txt = removeURLstring(text)
	if txt in cols:
		return True
	else:
		return False

def add_sub(parent, tag, content):
	temp = etree.SubElement(parent, tag)
	temp.text = content
	return temp

def spaceout(n = 2):
	for i in range(n):
		print()
def isNum(month, year):
	try:
		if int(month) > 0 and int(month) < 13 and int(year) > 2009 and int(year) < 2020:
			return True
		else:
			return False
	except:
		return False
		
def view(parent_node):
	print("************************************************************")
	print(etree.tostring(parent_node,pretty_print = True, encoding = 'unicode')) 		#look at the whole tree
	print("************************************************************")
def frame_platform_name(platform_name):
	print('=========================================================================')
	print(platform_name)
	print('=========================================================================')

def openRecordsFile(pathstring, sheet):
	lgns = pd.read_excel(pathstring, sheet_name = sheet).fillna('')
	return lgns

def getCredentialsR4(platformName):
	## Connect to the server using the server and database (db) variables initialized in the variables reference file
	conn = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + db + ';Trusted_Connection=yes')
	sql = "SELECT * FROM " + r4table
	recordsDF = pd.read_sql(sql,conn)
	
	try:
		lgn = (str(recordsDF.loc[recordsDF['Platform Name'] == platformName ].values[0][1]),
				str(recordsDF.loc[recordsDF['Platform Name'] == platformName ].values[0][2]),
				str(recordsDF.loc[recordsDF['Platform Name'] == platformName ].values[0][3]))
	except:
		lgn = ('','','')
	return lgn

def getCredentialsR5(platformName):
	## Connect to the server
	conn = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + db + ';Trusted_Connection=yes')
	sql = "SELECT * FROM " + r5table
	recordsDF = pd.read_sql(sql,conn)
	
	try:
		lgn = (str(recordsDF.loc[recordsDF['Platform Name'] == platformName ].values[0][1]),
				str(recordsDF.loc[recordsDF['Platform Name'] == platformName ].values[0][2]),
				str(recordsDF.loc[recordsDF['Platform Name'] == platformName ].values[0][3]),
				str(recordsDF.loc[recordsDF['Platform Name'] == platformName ].values[0][4]),
				str(recordsDF.loc[recordsDF['Platform Name'] == platformName ].values[0][5]))
	except:
		lgn = ('','','', '')
	return lgn
########################################################################################################################
## Deprecated versions of functions that are tombstoned for testing purposes.
########################################################################################################################
def getCredentialsBase_DEPRECATED(recordsDF, platformName):
	try:
		lgn = (str(recordsDF.loc[recordsDF['Platform Name'] == platformName ].values[0][1]),
				str(recordsDF.loc[recordsDF['Platform Name'] == platformName ].values[0][2]),
				str(recordsDF.loc[recordsDF['Platform Name'] == platformName ].values[0][3]))
	except:
		lgn = ('','','')
	return lgn
def getCredentialsR5_DEPRECATED(recordsDF, platformName):
	try:
		lgn = (str(recordsDF.loc[recordsDF['Platform Name'] == platformName ].values[0][1]),
				str(recordsDF.loc[recordsDF['Platform Name'] == platformName ].values[0][2]),
				str(recordsDF.loc[recordsDF['Platform Name'] == platformName ].values[0][3]),
				str(recordsDF.loc[recordsDF['Platform Name'] == platformName ].values[0][4]),
				str(recordsDF.loc[recordsDF['Platform Name'] == platformName ].values[0][5]))
	except:
		lgn = ('','','', '')
	return lgn
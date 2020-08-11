# PySUSHI
PySUSHI is a locally-deployed system for automated collection and storage of COUNTER reports via SUSHI protocols.
Presented at ER&L 2020 by Karen Harker and Chris Hergert, and currently maintained by Chris Hergert.
PySushi release 2 is now in alpha testing, with stable functionality and more features forthcoming.


#Release 2
PySushi v2.0 is available in the "release 2" folder, although the binaries are not yet available as a Github release.
The counter4pybr.py and counter5pybr.py files are library files that contain the parsing functions necessary for the various R4 and R5 (respeectively) reports as collected via SUSHI. The VariablesReference.py and BaseLibPybr.py files are library files that support the CounterXpybr.py files' functionality.
The UsageSweep.py file is actually the source code for the monthly usage collection applicatio currently used at the University of North Texas libraries for monthly SUSHI data collection. It supports collection of JR1, JR2, BR1, BR2, DB1, DB2, MR1, TR(J1,J2,J3,BR1,BR2,BR3), and DR_D1 reports via SUSHI from a usage credentials database. This can be  adjusted to read in credentials from an Excel or CSV file by adjusting the query in lines 17, 18. If a database is being used, then the address will need to be configured in the first three variables in the VariablesReference file. The reports are also currently uploaded directly to a usage database with a table for each report type (all TR sub-reports reside together on a TR table), but these can be adjusted to print to a .csv/.xlsx file by making adding a readout to the sweep() file just prior to database uploading. The UsageSweep file is intended to be either useful to libraries matching the setup at the Universit of North Texas, or instructional in potential uses of the PySUSHI system for institutions hoping to implement part of the PySushi system or take ideas for their own projects.
the requirements.txt file is a dependency file for users utilizing a virtualenv for execution of the PySUSHI system.


#Release 1
For reference, a script called CollectAllMonthlyUsage.py is included, where a library can change the year and month in lines 14 and 15, and the collection system will collect all and save all usage for all resources, for that single month.

To set up the PySUSHI usage system for your local use, open the variablesReference file and change the text strings for the names of your usage database, the server hosting that database, and the table names for your table holding the names and credentials for platforms that distribute usage in R4 reports and the corresponding table for platforms that distribute usage in R5 reports.

# PySUSHI
PySUSHI is a locally-deployed system for automated collection and storage of COUNTER reports via SUSHI protocols.
Presented at ER&L 2020 by Karen Harker and Chris Hergert, and currently maintained by Chris Hergert.

For reference, a script called CollectAllMonthlyUsage.py is included, where a library can change the year and month in lines 14 and 15, and the collection system will collect all and save all usage for all resources, for that single month.

To set up the PySUSHI usage system for your local use, open the variablesReference file and change the text strings for the names of your usage database, the server hosting that database, and the table names for your table holding the names and credentials for platforms that distribute usage in R4 reports and the corresponding table for platforms that distribute usage in R5 reports.

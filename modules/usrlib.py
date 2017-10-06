#Standard user defined function for quickly load and run
# Version 1.2
# Compatible with MySQL DB 


# Table of Contents                                                      
#------------------------------------------------------------------------
# 1. Imported standard module
# 2. Define funciton
#  |_ 2.1. MySQL Query 
#  |___ 2.1.1. Query for MySql
#  |___ 2.1.2. SQL file to query string
#  |___ 2.1.3. Transformm into Insert Query
#  |___ 2.1.4. Create Insert Query Base ON Query Result 
#  |_ 2.2. Logging output
#  |_ 2.3. Get list of file locate in directory and it's sub-folders 
#  |_ 2.4. Filter text with regex
# 3.Internal codes (for internal module process only)

# 1. Import standard module

import mysql.connector
import os, fnmatch
import re
from datetime import datetime

# 2. Define funciton
# 2.1.1. MySQL Query

# 2.1. Query for MySql
# Query function from data base, take into account following information:
#  1. config = db info, recommended load from *.ini file, if not,
#     pass it a dictionary {"user" = "", "password" = "", "host" = "", "database" = ""}
#  2. query = sql input
#  3. para = parameter to query/insert (mandatory), return mysql error if not,
#     when no para required, let it like empy list/tuple ('',)
#  4. is_header = True if result return required header
#  5. is_commit = True if result to insert into system
def query_data(config, query, para, is_commit=False, is_return=True, is_header=False, is_buffer=False):
	cnn = mysql.connector.connect(user=config['user'], password=config['password'], host=config['host'], database=config['database'])
	cursor = cnn.cursor(buffered = is_buffer)
	multi_check = True if ';\n' in query else False
	for x in para:
		cursor.execute(query,x, multi = multi_check)
		
	if is_commit == False:
		if is_return == True:
			result = cursor.fetchall()
			if is_header == True:
				tmp = list(i[0] for i in cursor.description)
				result.insert(0,tmp)
			return result
	else:
		cnn.commit()
	cursor.close()
	cnn.close()
	
# 2.1.2. SQL file to query string
# Input the direction and return query string, take note this only return raw data from sql still appear \n \t according to original
def read_query(url):
	with open(url,'r') as rd:
		result = ' '.join(rd.readlines())
	return result
	
# 2.1.3. Transformm into Insert Query
# Transform the normal query into Insert Query (with duplicate update) 
def insert_query(query,dest_tb, is_truncate=False, is_duplicate_update=False):
	select_str = query[6: query.index('FROM')].replace("\t","") # get only elements in select sections
	select_lst = select_str.split(",\n")
	tmp_lst  = [x[x.index(' AS ') + 4:].replace('`','').replace('\n','').replace(' ','') if ' AS ' in x else x[x.index('.') + 1 :].replace('\n','').replace(' ','') for x in select_lst]
	tmp1 = ''
	tmp2 = ''
	tmp3 = ''
	for x in range(len(tmp_lst)):	
		tmp1 += '%s, '
		if x > 1:
			tmp2 += tmp_lst[x] + '=%s, '	
		tmp3 += tmp_lst[x] + ','
	result = 'TRUNCATE TABLE ' + dest_tb + ';\n' if is_truncate == True else ''	
	result += 'INSERT INTO ' + dest_tb + ' (' + tmp3[:len(tmp3) - 1] + ') VALUES (' + tmp1[:len(tmp1) - 2] + ')' 
	if is_duplicate_update == True: 
		result += ' ON DUPLICATE KEY UPDATE ' + tmp2[:len(tmp2) - 2]
	return result	
	
# 2.1.4. Create Insert Query Base ON Query Result
# Transform 1st row header into list of column in insert query string, the primary id must be in 1st position
# STRONGLY RECOMMENED USING is_header = TRUE if using query_data
def insert_query_2(query_rs, dest_tb, is_truncate=False, is_duplicate_update=False):
	header = query_rs[0]
	to_be_insert = ['%' for x in header]
	result = 'TRUNCATE TABLE ' + dest_tb + ';\n' if is_truncate == True else ''	
	result += 'INSERT INTO ' + dest_tb + ' (' + ', '.join(header) + ') VALUES (' + ', '.join(to_be_insert) + ')' 
	if is_duplicate_update == True: 
		result += ' ON DUPLICATE KEY UPDATE ' + ', '.join(to_be_insert[1:])
	return result	
	
# 2.2. Logging output
# Output into url provide, append mode, format as "datetime - blabla", url must be a valid 
def writelog(url, txt):
	with open(url,'a') as wr:
		wr.write(datetime.now().__format__("%Y-%m-%d %H:%M:%S") + txt +  '\n')
	print(datetime.now().__format__("%Y-%m-%d %H:%M:%S") + txt)
	
# 2.3. Get list of file locate in directory and it's sub-folders 
# Input address and return list of files, if with_root is True
def get_lst_all(url, with_root=False):
	lst = []
	for root, dirnames, filenames in os.walk(url):
		for filename in fnmatch.filter(filenames, '*.csv'):
			tmp = [filename]
			if with_root == True:
				tmp.append(os.path.join(root,filename)) 
			lst.append(tmp)
	return lst

# 2.4. Filter text with regex
def fetch_string(input, pattern_str):
	pattern = re.compile(pattern_str)
	return pattern.search(input).group() if pattern.search(input) != None else None

# 3.Internal codes
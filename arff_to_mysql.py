#!/usr/bin/python
#
# Author: David Kelly
#
# This script converts an ARFF dataset to a MySQL database table.
#
# The ARFF interpretation is based on the information here:
#         http://weka.wikispaces.com/ARFF+%28stable+version%29
#
#
# Some good datasets can be found at:
#         http://www.cs.waikato.ac.nz/ml/weka/index_datasets.html
#

import datetime, re, shlex, sys, types

NUMERIC_TYPE = 'decimal(20,5)'
STRING_TYPE = 'varchar(72)'
DATE_TYPE = 'timestamp'
INTEGER_TYPE = 'int'
REAL_TYPE = NUMERIC_TYPE


class Arff():

	def __init__(self, arff_file, sql_formatter=None):
		self.header = []
		self.instances = []
		self.attributes = []
		self.has_data = False
		self.sql_formatter = sql_formatter
		
		if type(arff_file) is types.StringType:
			_arff_file = open(arff_file)
		elif type(arff_file) is types.FileType:
			_arff_file = arff_file
		else:
			raise TypeError
		
		self.__parse_file(_arff_file)
	
	def __parse_file(self, f):
		if self.sql_formatter:
			for line in f.readlines():
				if line[0] == '%':
					# The line is a comment.
					self.sql_formatter.format_comment(line[1:])
				elif line[0] == '@':
					# The line describes the dataset.
					self.__parse_declaration(line)
				elif len(line) > 0 and self.has_data:
					self.sql_formatter.format_instance(self.relation,
						self.__parse_instance(line))
				else:
					raise TypeError('Unexpected line encountered: %s' % line)
		
		else:
			for line in f.readlines():
				if line[0] == '%':
					# The line is a comment. Put it in the header section sans %.
					self.header.append(line[1:])
				elif line[0] == '@':
					# The line describes the dataset.
					self.__parse_declaration(line)
				elif len(line) > 0 and self.has_data:
					self.instances.append(self.__parse_instance(line))
				else:
					print 'Unexpected line encountered:\n%s' % line
	
	def __parse_declaration(self, line):
		components = line.split(' ', 1)
		declaration = components[0].strip()
		if len(components) == 2:
			value = components[1].strip()
		if imatches(declaration, '@RELATION'):
			# Found the RELATION declaration.
			self.relation = value
		elif imatches(declaration, '@ATTRIBUTE'):
			# Found an ATTRIBUTE declaration.
			self.attributes.append(Attribute(value))
		elif imatches(declaration, '@DATA'):
			self.has_data = True
			self.sql_formatter.format_create(self.relation, self.attributes)
	
	def __parse_instance(self, line):
		return Instance(line, self.attributes)
				
class Attribute():

	class DataType:
		NUMERIC = 0
		NOMINAL = 1
		STRING = 2
		DATE = 3
		RELATIONAL = 4
		INTEGER = 5
		REAL = 6
		
	def __init__(self, defn):
		self.value = None
		tokens = shlex.split(defn)
		self.name = tokens[0].replace(' ', '_')
		
		if len(tokens) > 1:
			if imatches(tokens[1], 'REAL'):
				self.datatype = self.DataType.REAL
			elif imatches(tokens[1], 'INTEGER'):
				self.datatype = self.DataType.INTEGER
			elif imatches(tokens[1], 'NUMERIC'):
				self.datatype = self.DataType.NUMERIC
			elif imatches(tokens[1], 'STRING'):
				self.datatype = self.DataType.STRING
			elif imatches(tokens[1], 'INTEGER'):
				self.datatype = self.DataType.DATE
				if len(tokens) > 2:
					self.dateformat = tokens[2][1:][:-1]
				else:
					self.dateformat = '%Y-%m-%dT%H:%M:%S'
			elif imatches('{.*}', tokens[1]):
				self.datatype = self.DataType.NOMINAL
				self.accepts = tokens[1][1:][:-1].split(',')
		else:
			print 'bad attribute specification: %s' % defn

class Instance():
	def __init__(self, defn, attributes):
		self.fields = []
		values = defn.split(',')
		for i in range(0, len(attributes)):
			values[i] = values[i].strip()
			if values[i] == '?' or values[i] == '\'?\'':
				values[i] = None
			elif attributes[i].datatype in (Attribute.DataType.NUMERIC,
												Attribute.DataType.REAL,):
				try:
					attributes[i].value = float(values[i])
				except ValueError:
					print 'Could not parse field %s' % values[i]
			elif attributes[i].datatype == Attribute.DataType.INTEGER:
				try:
					attributes[i].value = int(values[i])
				except ValueError:
					print 'Could not parse field %s' % values[i]
			elif attributes[i].datatype == Attribute.DataType.NOMINAL:
				if values[i][1:][:-1] in attributes[i].accepts:
					attributes[i].value = values[i][1:][:-1]
				else:
					print attributes[i].accepts
					print 'Bad value %s for nominal attribute %s' % (values[i],
															attributes[i].name)
			elif attributes[i].datatype == Attribute.DataType.DATE:
				try:
					attributes[i].value = datetime.strptime(values[i][1:][:-1],
													attributes[i].dateformat)
				except ValueError:
					print 'Bad date format for attribute %s: %s | %s' % (
						attributes[i].name, attributes[i].dateformat, values[i])
			elif attributes[i].datatype == Attribute.DataType.STRING:
				if values[i] != '?':
					attributes[i].value = values[i]
			
			self.fields.append(attributes[i])

def imatches(word, match):
	return re.compile(r'%s' % word, re.IGNORECASE).match(match)

class MySQLFormatter():

	def __init__(self, outfile=None):
		if type(outfile) is types.StringType:
			self.out = open(outfile)
		elif type(outfile) is types.FileType:
			self.out = outfile
		else:
			self.out = sys.stdout.write

	def format_comment(self, comment):
		self.out('--' + comment)
	
	def format_instance(self, table_name, instance):
		self.out('INSERT INTO ' + table_name + ' VALUES(' +
			', '.join(self.__quote_value(field) for field in instance.fields) +
			');\n'
		)
	
	def format_create(self, table_name, attributes):
		self.out('CREATE TABLE ' + self.__replace_bad_chars(table_name) +
			' (\n' +
			',\n\t'.join(self.__replace_bad_chars(attr.name) +
				' ' + self.__sql_type(attr) for attr in attributes) +
			'\n);\n\n'
		)
	
	def __quote_value(self, field):
		if field.value == None:
			return 'NULL'
		if field.datatype in (Attribute.DataType.DATE,
				Attribute.DataType.STRING, Attribute.DataType.NOMINAL):
			return '\'%s\'' % field.value
		else:
			return '%s' % field.value
		
	def __replace_bad_chars(self, word):
		return '`' + re.compile('[^a-zA-Z0-9_\$]').sub('_', word) + '`'
	
	def __sql_type(self, attr):
		if attr.datatype == Attribute.DataType.NUMERIC:
			return NUMERIC_TYPE
		elif attr.datatype == Attribute.DataType.REAL:
			return REAL_TYPE
		elif attr.datatype == Attribute.DataType.INTEGER:
			return INTEGER_TYPE
		elif attr.datatype == Attribute.DataType.DATE:
			return DATE_TYPE
		elif attr.datatype == Attribute.DataType.STRING:
			return STRING_TYPE
		elif attr.datatype == Attribute.DataType.NOMINAL:
			return 'varchar(%d)' % len(max(attr.accepts, key=len))
		else:
			return Attribute.DataType.STRING

def arff_to_mysql(arff_file):
	Arff(arff_file, MySQLFormatter())

if __name__ == '__main__':
	if len(sys.argv) > 1:
		try:
			arff_to_mysql(open(sys.argv[1]))
		except IOError:
			print '\nCannot read file %s\n' % sys.argv[1]
	else:
		print 'Usage:\n\tpython arff_to_mysql.py <dataset.arff>\n'

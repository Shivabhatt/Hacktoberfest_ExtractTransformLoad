import psycopg2
from psycopg2 import Error
import config
import os
import re
import pandas as pd 
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem.porter import PorterStemmer
from nltk.stem import WordNetLemmatizer
import nltk
#from wordcloud import WordCloud, STOPWORDS
import numpy as np
import matplotlib.pyplot as plt
from textblob import TextBlob


class TweetObject():
	
	def __init__(self, host, database, user, password):
		self.password = password
		self.host = host
		self.database = database
		self.user = user
		


	def MySQLConnect(self,query):
		"""
		Connects to database and extracts
		raw tweets and any other columns we
		need
		Parameters:
		----------------
		arg1: string: SQL query
		Returns: pandas dataframr
		----------------
		"""

		try:
			print('Connecting to the PostgreSQL database...')
			con = psycopg2.connect(host = self.host, database = self.database, user = self.user, password = self.password)

			
			print("Successfully connected to database")

			cursor = con.cursor()
			query = query
			cursor.execute(query)

			data = cursor.fetchall()
			# store in dataframe
			df = pd.DataFrame(data,columns = ['productalid', 'name'])
			print(df.head())



		except Error as e:
			print(e)
		
		cursor.close()
		con.close()

		
		return df
		
	def insertdata(self):
		try:
			print('Connecting to the PostgreSQL database...')
			con = psycopg2.connect(host = self.host, database = self.database, user = self.user, password = self.password)

			
			print("Successfully connected to database")
			cursor = con.cursor()
			cols = ",".join([str(i) for i in data.columns.tolist()])

			# Insert DataFrame recrds one by one.
			for i,row in data.iterrows():
				sql = "INSERT INTO public.dimprod (" +cols + ") VALUES (" + "%s,"*(len(row)-1) + "%s)"
				cursor.execute(sql, tuple(row))

				# the connection is not autocommitted by default, so we must commit to save our changes
				con.commit()
				# Execute query
			sql = "SELECT * FROM public.dimprod;"
			cursor.execute(sql)

			# Fetch all the records
			result = cursor.fetchall()
			for i in result:
				print(i)
		except Error as e:
			print(e)
		
		cursor.close()
		con.close()
			


t = TweetObject( host = 'localhost', database = 'check', user = 'postgres' , password= '1234')
data  = t.MySQLConnect("SELECT prodid,pname FROM public.travello_checker;")

t = TweetObject( host = 'localhost', database = 'testing', user = 'postgres' , password= '1234')
t.insertdata()
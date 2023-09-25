# -*- coding: utf-8 -*-
# scrapy crawl facturas -o ../input/facturas/facturas.json
# 
import scrapy
import time
import re
import sqlalchemy
import sqlite3
import pandas as pd
import json


class QuotesSpider(scrapy.Spider):

	def query(self):
		cnx = sqlite3.connect('../database.db')
		consulta  = " \
			SELECT \
				DISTINCT(acepta.uri), \
				acepta.emisor, \
				acepta.folio, \
				acepta.unico \
			FROM \
				acepta \
		"
		query = pd.read_sql_query(consulta, cnx)

		lista = []
		for index, row in query.iterrows():
			x = row['uri'].split("/")
			URL = 'http://'+x[2]+'/ca4webv3/XmlView?url=http%3A%2F%2F'+x[2]+'%2Fv01%2F'+x[4]
			lista.append(URL)
		return {'lista' : lista, 'row' : row}


	def parseNumero(self, numero, rango):
		word = ['&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;']
		newtext = []
		for x in numero:
			if x not in word:
				newtext.append(x)

		DIFERENCIA = rango - len(newtext)

		if DIFERENCIA > 0:
			for i in range(0, DIFERENCIA):
				newtext.append('0.00')

		DIFERENCIA = rango - len(newtext)

		newtext2 = []
		for x in newtext:
			newtext2.append(int(x.split(".")[0]))

		return newtext2

	def parseUnidad(self, numero, rango):
		word = ['&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;']
		newtext = []
		for x in numero:
			if x not in word:
				newtext.append(x)

		DIFERENCIA = rango - len(newtext)

		if DIFERENCIA > 0:
			for i in range(0, DIFERENCIA):
				newtext.append('N/A')

		DIFERENCIA = rango - len(newtext)

		return newtext

	def parseCodigo(self, numero, rango):
		#print( ">>>>>>>>>>>>>>> ", rango)
		if not numero:
			for i in range(0, rango):
				numero.append('N/A')

		word = ['&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;']
		newtext = []
		for x in numero:
			if x not in word:
				newtext.append(x)

		return newtext

	def parseNmbItem(self, numero):
		if not numero:
			numero.append('N/A')

		word = ['&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;']
		newtext = []
		for x in numero:
			if x not in word:
				r = re.sub("&nbsp;|N&deg;|@@|  |   |    |                            ", " ", x)
				newtext.append(r)

		return newtext

	def parseDscItem(self, numero, rango):
		if not numero:
			for i in range(0, rango):
				numero.append('N/A')

		DIFERENCIA = rango - len(numero)

		if DIFERENCIA > 0:
			for i in range(0, DIFERENCIA):
				numero.append('N/A')

		DIFERENCIA = rango - len(numero)

		string = []
		for i in numero:
			stringNew = i.replace("&nbsp;", " ")
			string.append(re.sub("&nbsp;|N&deg;|@@|  |   |    |                            ", " ", i))
		return string
		# =============================================================

	name = "facturas"

	def start_requests(self):
		listado_urls = self.query()
		start_urls = listado_urls['lista']

		for url in start_urls:
			yield scrapy.Request(url=url, callback=self.parse)

	def parse(self, response):
		response.text.lstrip('&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;')
		rango = len(re.findall(r'(?<=MontoItem<\/span>&gt;<span class="xmlverb-text">).*?(?=<\/span>)', response.text))
		#print(">>> rango : > ",  rango)

		RUTEmisor	= re.findall(r'(?<=RUTEmisor<\/span>&gt;<span class="xmlverb-text">).*?(?=<\/span>)', response.text)
		Folio 		= re.findall(r'(?<=Folio<\/span>&gt;<span class="xmlverb-text">).*?(?=<\/span>)', response.text)
		Cantidad 	= self.parseNumero(re.findall(r'(?<=QtyItem<\/span>&gt;<span class="xmlverb-text">).*?(?=<\/span>)', response.text), rango)
		Unidad 		= self.parseUnidad(re.findall(r'(?<=UnmdItem<\/span>&gt;<span class="xmlverb-text">).*?(?=<\/span>)', response.text), rango)
		Codigo 		= self.parseCodigo(re.findall(r'(?<=VlrCodigo<\/span>&gt;<span class="xmlverb-text">).*?(?=<\/span>)', response.text), rango)
		NmbItem 	= self.parseNmbItem(re.findall(r'(?<=NmbItem<\/span>&gt;<span class="xmlverb-text">).*?(?=<\/span>)', response.text))
		DscItem 	= self.parseDscItem(re.findall(r'(?<=DscItem<\/span>&gt;<span class="xmlverb-text">).*?(?=<\/span>)', response.text), rango)
		PrcItem 	= self.parseNumero(re.findall(r'(?<=PrcItem<\/span>&gt;<span class="xmlverb-text">).*?(?=<\/span>)', response.text), rango)
		MontoItem	= self.parseNumero(re.findall(r'(?<=MontoItem<\/span>&gt;<span class="xmlverb-text">).*?(?=<\/span>)', response.text), rango)

		store =	 {
				'RUTEmisor' 	:RUTEmisor,
				'Folio' 		:Folio,
				'Cantidad' 		:Cantidad,
				'Unidad' 		:Unidad,
				'Codigo' 		:Codigo,
				'NmbItem' 		:NmbItem,
				'DscItem' 		:DscItem,
				'PrcItem' 		:PrcItem,
				'MontoItem'		:MontoItem
			}

		for key, value in store.items():
			print("============== :", key, ' 	->', value)

		for x in range(len(store['PrcItem'])):
			yield  {

					'unico'	: store['RUTEmisor'][0]+store['Folio'][0], 
					'RUTEmisor'	: store['RUTEmisor'][0], 
					'Folio'		: store['Folio'][0],
					'Cantidad'	: store['Cantidad'][x],
					'Unidad'	: store['Unidad'][x],
					'Codigo'	: store['Codigo'][x],
					'NmbItem'	: store['NmbItem'][x],
					'DscItem'	: store['DscItem'][x],
					'PrcItem'	: int(store['MontoItem'][x]/ store['Cantidad'][x]), 
					'MontoItem'	: store['MontoItem'][x], 
					'Url'		: response.url
					}

		print("IIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII " )
		print()


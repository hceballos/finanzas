import pandas as pd
import sqlite3
import numpy as np
from datetime import datetime
from datetime import date


class Informes(object):

	def __init__(self):
		
		self.selectSubCategorias()

	def selectSubCategorias(self):
		cnx = sqlite3.connect('database.db')
		consulta = " \
			SELECT \
				DISTINCT(clas.item) \
			FROM \
				(SELECT cfp.* FROM cfp)c \
				LEFT JOIN(SELECT clasificador.* FROM clasificador)clas  ON c.concepto = clas.denominacion \
			WHERE \
				clas.item IS NOT NULL \
			GROUP BY \
				clas.item \
			ORDER BY \
				c.concepto \
		"
		clasificador = pd.read_sql_query(consulta, cnx)
		for item in clasificador['item']:
			print(item)
			self.databaseArbol(item, cnx)

	def databaseArbol(self, item, cnx):
		consulta = " \
			SELECT \
				clas.subtitulo as categoria, \
				clas.item as subcategoria, \
				clas.denominacion as denominacion, \
				c.*, \
				COALESCE(a.uri, 'N#A') AS acepta_info, \
				COALESCE(a.folio_oc, 'N#A') AS orden_de_compra \
			FROM \
				(SELECT cfp.* FROM cfp)c \
				LEFT JOIN(SELECT clasificador.* FROM clasificador)clas  ON c.concepto = clas.denominacion \
				LEFT JOIN(SELECT acepta.* FROM acepta)a  ON c.unico = a.unico \
			WHERE \
				clas.item = '"+item+"' \
		"
		df = pd.read_sql_query(consulta, cnx)
		df['year'] = pd.DatetimeIndex(df['fechaGeneracion']).year
		df['mes']  = pd.DatetimeIndex(df['fechaGeneracion']).month

		# ----------------------------------------------------------------------------------------------
		df_item = pd.pivot_table(df,
							index = ["subcategoria"],
							values = ["monto"],
							columns = ["year", "mes"],
							aggfunc=[np.sum],
							fill_value=0,
							margins=True
							)

		df_item.rename(columns={1 :'Ene', 2 :'Feb', 3 :'Mar', 4 :'Abr', 5 :'May', 6 :'Jun', 7 :'Jul', 8 :'Ago', 9 :'Sep', 10 :'Oct', 11 :'Nov', 12 :'Dic', 'Concepto':'concepto', 'Principal':'principal', 'Monto Documento':'montoDocumento', 'Fecha Generación':'fechaGeneracion', 'Folio':'folio', 'Título':'titulo', 'Número Documento': 'numero', 'Fecha Documento':'fechaDocumento', 'Tipo Documento':'tipoDocumento','Monto Documento.1':'monto'}, inplace=True)
		# ----------------------------------------------------------------------------------------------
		df_concepto = pd.pivot_table(df,
							index = ["subcategoria", "denominacion"],
							values = ["monto"],
							columns = ["year", "mes"],
							aggfunc = [np.sum],
							fill_value = 0,
							margins = True
							)
		df_concepto.rename(columns={1 :'Ene', 2 :'Feb', 3 :'Mar', 4 :'Abr', 5 :'May', 6 :'Jun', 7 :'Jul', 8 :'Ago', 9 :'Sep', 10 :'Oct', 11 :'Nov', 12 :'Dic', 'Concepto':'concepto', 'Principal':'principal', 'Monto Documento':'montoDocumento', 'Fecha Generación':'fechaGeneracion', 'Folio':'folio', 'Título':'titulo', 'Número Documento': 'numero', 'Fecha Documento':'fechaDocumento', 'Tipo Documento':'tipoDocumento','Monto Documento.1':'monto'}, inplace=True)
		# ----------------------------------------------------------------------------------------------
		df_principal = pd.pivot_table(df,
							index = ["subcategoria", "denominacion", "principal"],
							values = ["monto"],
							columns = ["year", "mes"],
							aggfunc = [np.sum],
							fill_value = 0,
							margins = True
							)
		df_principal.rename(columns={1 :'Ene', 2 :'Feb', 3 :'Mar', 4 :'Abr', 5 :'May', 6 :'Jun', 7 :'Jul', 8 :'Ago', 9 :'Sep', 10 :'Oct', 11 :'Nov', 12 :'Dic', 'Concepto':'concepto', 'Principal':'principal', 'Monto Documento':'montoDocumento', 'Fecha Generación':'fechaGeneracion', 'Folio':'folio', 'Título':'titulo', 'Número Documento': 'numero', 'Fecha Documento':'fechaDocumento', 'Tipo Documento':'tipoDocumento','Monto Documento.1':'monto'}, inplace=True)
		# ----------------------------------------------------------------------------------------------
		df_facturacion = pd.pivot_table(df,
							index = ["subcategoria", "denominacion", "principal", "numero"],
							values = ["monto"],
							columns = ["year", "mes"],
							aggfunc = [np.sum],
							fill_value = 0,
							margins = True
							)
		df_facturacion.rename(columns={1 :'Ene', 2 :'Feb', 3 :'Mar', 4 :'Abr', 5 :'May', 6 :'Jun', 7 :'Jul', 8 :'Ago', 9 :'Sep', 10 :'Oct', 11 :'Nov', 12 :'Dic', 'Concepto':'concepto', 'Principal':'principal', 'Monto Documento':'montoDocumento', 'Fecha Generación':'fechaGeneracion', 'Folio':'folio', 'Título':'titulo', 'Número Documento': 'numero', 'Fecha Documento':'fechaDocumento', 'Tipo Documento':'tipoDocumento','Monto Documento.1':'monto'}, inplace=True)
		# ----------------------------------------------------------------------------------------------
		df_testUrl = pd.pivot_table(df,
							index = ["subcategoria", "denominacion", "principal", "numero", "acepta_info", "orden_de_compra"],
							values = ["monto"],
							columns = ["year", "mes"],
							aggfunc = [np.sum],
							fill_value = 0,
							margins = True
							)
		df_testUrl.rename(columns={1 :'Ene', 2 :'Feb', 3 :'Mar', 4 :'Abr', 5 :'May', 6 :'Jun', 7 :'Jul', 8 :'Ago', 9 :'Sep', 10 :'Oct', 11 :'Nov', 12 :'Dic', 'Concepto':'concepto', 'Principal':'principal', 'Monto Documento':'montoDocumento', 'Fecha Generación':'fechaGeneracion', 'Folio':'folio', 'Título':'titulo', 'Número Documento': 'numero', 'Fecha Documento':'fechaDocumento', 'Tipo Documento':'tipoDocumento','Monto Documento.1':'monto'}, inplace=True)
		# ----------------------------------------------------------------------------------------------

		consulta = " \
			SELECT \
			    strftime('%Y', fechaGeneracion) as YEAR, \
			    sum(case strftime('%m', c.fechaGeneracion) when '01' then monto else 0 end) Ene, \
			    sum(case strftime('%m', c.fechaGeneracion) when '02' then monto else 0 end) Feb, \
			    sum(case strftime('%m', c.fechaGeneracion) when '03' then monto else 0 end) Mar, \
			    sum(case strftime('%m', c.fechaGeneracion) when '04' then monto else 0 end) Abr, \
			    sum(case strftime('%m', c.fechaGeneracion) when '05' then monto else 0 end) May, \
			    sum(case strftime('%m', c.fechaGeneracion) when '06' then monto else 0 end) Jun, \
			    sum(case strftime('%m', c.fechaGeneracion) when '07' then monto else 0 end) Jul, \
			    sum(case strftime('%m', c.fechaGeneracion) when '08' then monto else 0 end) Ago, \
			    sum(case strftime('%m', c.fechaGeneracion) when '09' then monto else 0 end) Sep, \
			    sum(case strftime('%m', c.fechaGeneracion) when '10' then monto else 0 end) Oct, \
			    sum(case strftime('%m', c.fechaGeneracion) when '11' then monto else 0 end) Nov, \
			    sum(case strftime('%m', c.fechaGeneracion) when '12' then monto else 0 end) Dic \
			FROM \
			    (SELECT cfp.* FROM cfp)c \
				LEFT JOIN(SELECT clasificador.* FROM clasificador)clas ON c.concepto = clas.denominacion \
			WHERE \
				clas.item = '"+item+"' \
			group by \
			    c.YEAR \
			order by \
			    c.YEAR desc \
		"
		comparativa_nual = pd.read_sql_query(consulta, cnx)


		writer = pd.ExcelWriter(r'./output/'+item+'.xlsx', engine='xlsxwriter',options={'strings_to_urls': False})
		df_item.to_excel(writer, sheet_name='Mensual Historico')
		comparativa_nual.to_excel(writer, sheet_name='Comparativa Anual', index=False)
		df_concepto.to_excel(writer, sheet_name='Concepto')
		df_principal.to_excel(writer, sheet_name='Proveedor')
		df_facturacion.to_excel(writer, sheet_name='Facturacion')
		df_testUrl.to_excel(writer, sheet_name='Facturas y OC')
		writer.save()
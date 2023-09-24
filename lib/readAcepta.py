import pandas as pd
import glob
import xlrd
import csv
import os
import re
import sys
from lib.database import Database

class ReadAcepta:
    def __init__(self, datos):
        self.datos = datos
        self.process_files()
        
    def normalizeRut(self, string):
        # Normalizar el formato de RUT
        string = [w.replace(',00', '') for w in string]
        string = [w.replace(' ', '') for w in string]
        string = [w.replace('$', '') for w in string]
        string = [w.replace('.', '') for w in string]
        string = [w.replace('(', '-') for w in string]
        string = [w.replace(')', '') for w in string]
        return string

    def normalizeNumeric(self, string):
        # Normalizar valores numéricos
        return pd.to_numeric(string)

    def normalizeFolio(self, string):
        # Normalizar el formato del folio
        string = [w.replace('.0', '') for w in string]
        return string

    def normalizeFolio2(self, string):
        # Normalizar folio como entero
        mi_serie_enteros = string.apply(lambda x: int(float(x)))
        return mi_serie_enteros

    def process_files(self):
        acepta = pd.DataFrame()
        for f in glob.glob(self.datos['acepta'], recursive=True):
            print('Procesando: ', f)
            wb = xlrd.open_workbook(f)
            pestania = wb.sheet_names()
            sh = wb.sheet_by_name(pestania[0])
            aceptacsv = open('acepta.csv', 'w', newline='', encoding='UTF-8')
            wr = csv.writer(aceptacsv, quoting=csv.QUOTE_ALL)
            for rownum in range(sh.nrows):
                wr.writerow(sh.row_values(rownum))
            aceptacsv.close()
            df1 = pd.read_csv('acepta.csv', converters={'folio': str}, encoding='UTF-8')
            acepta = acepta.append(df1, ignore_index=True)

        # Normalizar columnas
        acepta['folio'] = self.normalizeFolio2(acepta['folio'])
        acepta['emisor'] = self.normalizeRut(acepta['emisor'])
        acepta = acepta[acepta['tipo'] != 52]  # Eliminar filas con tipo 52
        acepta['fecha_ingreso'] = acepta['fecha_ingreso'].str.split(' ', n=1, expand=True)[0]

        # Eliminar columnas no deseadas
        columns_to_delete = ['impuestos', 'estado_intercambio', 'informacion_intercambio', 'estado_nar', 'uri_nar',
                             'mensaje_nar', 'uri_arm', 'fecha_arm', 'fmapago', 'estado_acepta', 'estado_sii',
                             'referencias', 'fecha_nar', 'controller', 'fecha_vencimiento', 'estado_cesion',
                             'url_correo_cesion', 'fecha_recepcion_sii', 'estado_reclamo', 'fecha_reclamo',
                             'mensaje_reclamo', 'estado_devengo', 'razon_social_emisor', 'folio_rc',
                             'fecha_ingreso_rc', 'ticket_devengo', 'folio_sigfe', 'tarea_actual', 'area_transaccional',
                             'fecha_aceptacion', 'fecha', 'tipo', 'tipo_documento', 'receptor', 'publicacion',
                             'emision', 'monto_neto', 'monto_exento', 'monto_iva', 'fecha_ingreso_oc',
                             'codigo_devengo']
        acepta.drop(columns=columns_to_delete, inplace=True)

        # Normalizar monto_total
        acepta['monto_total'] = self.normalizeNumeric(acepta['monto_total'])
        acepta['ordenDeCompra'] = "pendiente"
        acepta['status'] = "pendiente"
        acepta['unico'] = acepta['emisor'] + acepta['folio'].apply(str)
        acepta['unico'] = self.normalizeFolio(acepta['unico'])

        # Eliminar duplicados basados en la columna "unico"
        acepta.drop_duplicates(subset="unico", keep=False, inplace=True)

        # Eliminar filas con valores nulos en la columna "folio_oc" (si es necesario)
        # acepta = acepta[acepta['folio_oc'].notna()]

        # Eliminar archivo CSV temporal
        os.remove("acepta.csv")

        # Conectar a la base de datos y almacenar datos
        database = Database()
        database.databaseAcepta(acepta)

        # Incrementar el límite de recursión si es necesario
        # sys.setrecursionlimit(10**6)

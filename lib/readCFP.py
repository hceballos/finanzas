import pandas as pd
import glob
import xlrd
import csv
import os
import re
from lib.database import Database

class ReadCFP:
    def __init__(self, datos):
        self.datos = datos
        self.process_clasification_files()
        self.process_cfp_files()

        
    def normalizeDateConta(self, dateString):
        # Normalizar la fecha en formato 'dd/mm/yyyy' a 'yyyy-mm-dd'
        return dateString.replace(['(\d{2}).(\d{2}).(\d{2})?(\d{2})'], ['\g<4>-\g<2>-\g<1>'], regex=True)

    def normalizeDate(self, dateString):
        # Normalizar la fecha en formato 'dd/mm/yyyy' a 'yyyy-mm-dd'
        return dateString.replace(['(\d{2})\/(\d{2})\/(\d{4})'], ['\g<3>-\g<2>-\g<1>'], regex=True)

    def normalizeNumeric(self, string):
        # Normalizar valores numéricos
        string = [w.replace(',00', '') for w in string]
        string = [w.replace(' ', '') for w in string]
        string = [w.replace('$', '') for w in string]
        string = [w.replace('.', '') for w in string]
        string = [w.replace('(', '-') for w in string]
        string = [w.replace(')', '') for w in string]
        return pd.to_numeric(string)

    def normalizeFloat64ToString(self, numero):
        # Convertir números flotantes a cadena (no parece necesario)
        numeroInString = numero.astype(str)
        return numero

    def process_clasification_files(self):
        # Procesar archivos de clasificación
        for f in glob.glob('./input/clasificacion/*.xlsx', recursive=True):
            print('Procesando: ', f)
            df = pd.read_excel(f, skiprows=7)
            df['Nivel 1'] = ''
            df['Nivel 2'] = ''
            df['Nivel 3'] = ''
            current_nivel1 = ''
            current_nivel2 = ''
            result_rows = []

            def remove_numbers(text):
                return re.sub(r'\d+', '', text)

            for index, row in df.iterrows():
                nivel = row['Nivel']
                concepto = row['Concepto Presupuestario']

                if nivel == 1:
                    current_nivel1 = remove_numbers(concepto)
                elif nivel == 2:
                    current_nivel2 = remove_numbers(concepto)
                elif nivel == 3 or nivel == 4:
                    result_rows.append([current_nivel1, current_nivel2, concepto])

            result_df = pd.DataFrame(result_rows, columns=['subtitulo', 'item', 'denominacion'])

            database = Database()
            database.databaseClasificador(result_df)

    def process_cfp_files(self):
        cfp = pd.DataFrame()
        for f in glob.glob(self.datos['cfp'], recursive=True):
            print('Procesando: ', f)
            wb = xlrd.open_workbook(f)
            sh = wb.sheet_by_name('Sheet1')
            cfpcsv = open('cfp.csv', 'w', newline='', encoding='UTF-8')
            wr = csv.writer(cfpcsv, quoting=csv.QUOTE_ALL)
            for rownum in range(sh.nrows):
                wr.writerow(sh.row_values(rownum))
            cfpcsv.close()
            df1 = pd.read_csv('cfp.csv', converters={'Número Documento': str}, encoding='UTF-8')
            cfp = cfp.append(df1, ignore_index=True)

        cfp = cfp[cfp['Tipo Vista'] != 'Saldo Inicial']
        cfp['Fecha Documento'] = self.normalizeDate(cfp['Fecha Documento'])
        cfp['Fecha Documento'] = pd.to_datetime(cfp['Fecha Documento']).dt.date
        cfp['Fecha Generación'] = self.normalizeDate(cfp['Fecha Generación'])
        cfp['Fecha Generación'] = pd.to_datetime(cfp['Fecha Generación']).dt.date
        cfp['rut'] = cfp['Principal'].str.split(' ', n=1, expand=True)[0]
        cfp['CodConcepto'] = cfp['Concepto'].str.split(' ', n=1, expand=True)[0]
        cfp['unico'] = cfp['rut'] + cfp['Número Documento']
        cfp['ordenDeCompra'] = "pendiente"
        cfp['status'] = "pendiente"
        cfp['Monto Documento.1'] = self.normalizeNumeric(cfp['Monto Documento.1'])
        cfp['Monto Documento'] = self.normalizeNumeric(cfp['Monto Documento'])
        del cfp['Tipo Vista']

        cfp['year'] = pd.DatetimeIndex(cfp['Fecha Generación']).year
        cfp['mes'] = pd.DatetimeIndex(cfp['Fecha Generación']).month

        cfp.rename(columns={'Concepto': 'concepto', 'Principal': 'principal', 'Monto Documento': 'montoDocumento',
                            'Fecha Generación': 'fechaGeneracion', 'Folio': 'folio', 'Título': 'titulo',
                            'Número Documento': 'numero', 'Fecha Documento': 'fechaDocumento',
                            'Tipo Documento': 'tipoDocumento', 'Monto Documento.1': 'monto'}, inplace=True)
        os.remove("cfp.csv")

        database = Database()
        database.databaseCFP(cfp)

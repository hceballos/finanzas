import json
import sqlalchemy
import pandas as pd


class ReadFacturas(object):
	def __init__(self, datos):
		self.datos = datos
		

		# Open JSON data
		print('Procesando  : ', self.datos['facturas'])
		with open(self.datos['facturas']) as f:
			data = json.load(f)

		# Create A DataFrame From the JSON Data
		df = pd.DataFrame(data)


		df['ItemBruto'] = (df['MontoItem']*1.19).astype(int)
		df.rename(columns={'Folio':'Factura'}, inplace=True)



		metadata = sqlalchemy.MetaData()
		engine = sqlalchemy.create_engine('sqlite:///database.db', echo=False)
		metadata = sqlalchemy.MetaData()

		metadata.create_all(engine)
		df.to_sql('facturas', engine, if_exists='replace')
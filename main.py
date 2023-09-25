# PS C:\Users\hector.ceballos\Desktop\HectorCeballos\pandora> scrapy crawl facturas -o facturas.json
# scrapy crawl facturas -o ..\pandora\input\facturas\facturas.json
from lib.informes  import Informes
from lib.readAcepta  import ReadAcepta
from lib.readCFP  import ReadCFP
from lib.readJson  import ReadJson
import subprocess

class main(ReadJson):
	def __init__(self, json_path):
		ReadJson.__init__(self, json_path)
		datos = self.datos
		
		ReadAcepta(datos) 	# Acepta - para tener listado de las OC
		ReadCFP(datos)		# Cartera financiera presupuestaria - devengo & Clasificador
		Informes()			# Informes generados en Output
		
		comando_scrapy = ["scrapy", "crawl", "facturas", "-o", "../input/facturas/facturas.json"]
		subprocess.run(comando_scrapy, cwd="scrapyAcepta")


json_path = r'../finanzas/data/data.json'

if __name__ == '__main__':
	main(json_path)
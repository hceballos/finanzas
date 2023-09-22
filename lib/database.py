import sqlalchemy


class Database(object):

	def databaseCFP(self, cfp):
		metadata = sqlalchemy.MetaData()
		engine = sqlalchemy.create_engine('sqlite:///database.db', echo=False)
		metadata = sqlalchemy.MetaData()

		OrdenDeCompra = sqlalchemy.Table(
			'cfp',
			metadata,
			sqlalchemy.Column('id', sqlalchemy.Integer),
			sqlalchemy.Column('concepto', sqlalchemy.String),
			sqlalchemy.Column('principal', sqlalchemy.String),
			sqlalchemy.Column('montoDocumento', sqlalchemy.BigInteger),
			sqlalchemy.Column('fechaGeneracion', sqlalchemy.String),
			sqlalchemy.Column('folio', sqlalchemy.String),
			sqlalchemy.Column('titulo', sqlalchemy.String),
			sqlalchemy.Column('numero', sqlalchemy.String),
			sqlalchemy.Column('fechaDocumento', sqlalchemy.String),
			sqlalchemy.Column('tipoDocumento', sqlalchemy.String),
			sqlalchemy.Column('monto', sqlalchemy.BigInteger),
			sqlalchemy.Column('rut', sqlalchemy.String),
			sqlalchemy.Column('unico', sqlalchemy.String, primary_key=True),
			sqlalchemy.Column('ordenDeCompra', sqlalchemy.String),
			sqlalchemy.Column('status', sqlalchemy.String),
			sqlalchemy.Column('year', sqlalchemy.String),
			sqlalchemy.Column('mes', sqlalchemy.String)
			)

		metadata.create_all(engine)
		cfp.to_sql('cfp', engine, if_exists='replace')


	def databaseEjecucion(self, ejecucion):
		metadata = sqlalchemy.MetaData()
		engine = sqlalchemy.create_engine('sqlite:///database.db', echo=False)
		metadata = sqlalchemy.MetaData()

		OrdenDeCompra = sqlalchemy.Table(
			'ejecucion',
			metadata,
			sqlalchemy.Column('concepto', sqlalchemy.String, primary_key=True),
			sqlalchemy.Column('monto', sqlalchemy.Integer),
			sqlalchemy.Column('codConcepto', sqlalchemy.String),
			sqlalchemy.Column('fecha', sqlalchemy.String)
			)
		
		metadata.create_all(engine)
		ejecucion.to_sql('ejecucion', engine, if_exists='replace')


	def databaseClasificador(self, clasificador):
		metadata = sqlalchemy.MetaData()
		engine = sqlalchemy.create_engine('sqlite:///database.db', echo=False)
		metadata = sqlalchemy.MetaData()

		OrdenDeCompra = sqlalchemy.Table(
			'clasificador',
			metadata,
			sqlalchemy.Column('codigo', sqlalchemy.String, primary_key=True),
			sqlalchemy.Column('categoria', sqlalchemy.String),
			sqlalchemy.Column('subcategoria', sqlalchemy.String)
			)

		metadata.create_all(engine)
		clasificador.to_sql('clasificador', engine, if_exists='replace')


	def databaseAcepta(self, acepta):
		metadata = sqlalchemy.MetaData()
		engine = sqlalchemy.create_engine('sqlite:///database.db', echo=False)
		metadata = sqlalchemy.MetaData()

		OrdenDeCompra = sqlalchemy.Table(
			'acepta',
			metadata,
			sqlalchemy.Column('folio', sqlalchemy.Integer),
			sqlalchemy.Column('emisor', sqlalchemy.String),
			sqlalchemy.Column('monto_total', sqlalchemy.String),
			sqlalchemy.Column('uri', sqlalchemy.String),
			sqlalchemy.Column('folio_oc', sqlalchemy.Integer),
			sqlalchemy.Column('unico', sqlalchemy.String, primary_key=True )
			)

		metadata.create_all(engine)
		acepta.to_sql('acepta', engine, if_exists='replace')




	def databaseCompromiso(self, compromiso):
		metadata = sqlalchemy.MetaData()
		engine = sqlalchemy.create_engine('sqlite:///database.db', echo=False)
		metadata = sqlalchemy.MetaData()

		OrdenDeCompra = sqlalchemy.Table(
			'compromiso',
			metadata,
			sqlalchemy.Column('id', sqlalchemy.Integer),
			sqlalchemy.Column('Concepto', sqlalchemy.String),
			sqlalchemy.Column('Principal', sqlalchemy.String),
			sqlalchemy.Column('Monto Documento', sqlalchemy.BigInteger),
			sqlalchemy.Column('Tipo Vista', sqlalchemy.String),
			sqlalchemy.Column('Fecha', sqlalchemy.String),
			sqlalchemy.Column('Folio', sqlalchemy.String),
			sqlalchemy.Column('Título', sqlalchemy.String),
			sqlalchemy.Column('Etapa Compromiso', sqlalchemy.String),
			sqlalchemy.Column('numero', sqlalchemy.String),
			sqlalchemy.Column('fechaDocumento', sqlalchemy.String),
			sqlalchemy.Column('Tipo Documento', sqlalchemy.String),
			sqlalchemy.Column('unico', sqlalchemy.String, primary_key=True)
			)










		metadata.create_all(engine)
		compromiso.to_sql('compromiso', engine, if_exists='replace')



		"""
		cnx = sqlite3.connect('database.db')
		consulta  = " \
			SELECT \
				cfp.*, \
				acepta.folio_oc \
			FROM  \
				cfp	LEFT JOIN acepta \
				on cfp.unico = acepta.unico \
				and (cfp.rut = acepta.emisor \
				and cfp.numero = acepta.folio) \
		"
		query = pd.read_sql_query(consulta, cnx)
		query.to_sql('query', engine, if_exists='replace')

		writer = pd.ExcelWriter('query.xlsx', engine='xlsxwriter')
		query.to_excel(writer, sheet_name='Todas las cuentas')
		writer.save()
		"""
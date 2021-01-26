import sqlite3
import time
import urllib.request, urllib.parse, urllib.error
from urllib.parse import urljoin
from urllib.parse import urlparse
import re

conn = sqlite3.connect('volcado.sqlite')
cur = conn.cursor()

baseurl = "ftp://ftp.justiciachaco.gov.ar/listas/"

cur.executescript('''
	CREATE TABLE IF NOT EXISTS Juzgados(
		id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE, 
		nombre TEXT UNIQUE, 
		nombre_ftp TEXT);

	CREATE TABLE IF NOT EXISTS Archivos(
		id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE, 
		nombre TEXT UNIQUE,
		juzgado_id INTEGER);


	CREATE TABLE IF NOT EXISTS Proveidos(
		id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
		fecha_id INTEGER,
		texto TEXT,
		juzgado_id INTEGER,
		archivo_id INTEGER);

	CREATE TABLE IF NOT EXISTS Fechas(
		id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
		fecha TEXT UNIQUE)

		'''
		)

url = baseurl

def abro_url(url, document = None, error = 0):
	while document == None:
		if error > 0:
			print ("La apertura del documento dio un error, esperamos y reintentamos")
			error += 1
			print("Reintento N°: "+ str(error) ,url)
			time.sleep(1)
		try:
			# Open with a timeout of 30 seconds
			document = urllib.request.urlopen(url)#, None, 30)#, context=ctx)

		except KeyboardInterrupt:
			print('')
			print('Program interrupted by user...')
			
		except Exception as e:
			print("Unable to retrieve or parse page",url)
			print("Error",e)
	
	return document


#controlo cual es la dependencia mas vieja a la que llegue
start = None
cur.execute('''SELECT nombre_ftp FROM Juzgados 
				WHERE id =(SELECT max(id) FROM Juzgados)''' )
try:
    dependencia_sigo = cur.fetchone()
    if dependencia_sigo is None :
        start = 0
    else:
        start = dependencia_sigo[0]
except:
    start = 0

if start is None : start = 0
	
document = abro_url(url)

texto = document.read().decode("ISO-8859-1")


dependencias = list()
estructura = {}

for i in range(len(texto.split("\n"))):
	if start in texto.split("\n")[i]:
		pos = i



if start == 0:	
	pos=0


# primero proceso las dependencias
	
for linea in texto.split("\n")[pos:]:
	res = re.findall("<DIR>          ([^(Zip)].+?_Pro)", linea)
	if len(res)>0:
		dependencias.append(res)
		estructura[res[0]] = []

# segundo proceso los archivos en cada dependencia
cur.execute('''SELECT nombre FROM Archivos 
				WHERE id =(SELECT max(id) FROM Archivos)''' )
archivo_sigo = cur.fetchone()



for dependencia in dependencias:
	url = baseurl + dependencia[0]
	document = abro_url(url)
	texto = document.read().decode("ISO-8859-1")
	for i in range(len(texto.split("\n"))):
		
		if archivo_sigo[0] in texto.split("\n")[i]:
			pos1 = i
			break


	if start == 0:	
		pos1=0
	else:
		pos1+=1

	for linea in texto.split("\n")[pos1:]:
		res = re.findall(".+[0-9] (.+.Txt)",linea)
		if len(res)>0:
			estructura[dependencia[0]].append(res)
	

for dependencia in estructura.keys():
	for archivo in estructura[dependencia]:
		url = baseurl + dependencia + "/" + archivo[0]
		
		document = abro_url(url)
		
		texto = document.read().decode("ISO-8859-1")
		comienza_prov = False
		proveido = ""
		proveidos = list()

		hay_juzgado, hay_fecha = True, True

		#analizo si la estructura del archivo de proveidos es la normal
		#buscando juzgado y fecha, si no es normal le busco contenido alternativo

		juzgado = re.findall("               (.+) - Fecha Despacho", texto)
		if len(juzgado)==0:
			hay_juzgado = False
			cur.execute('SELECT nombre FROM Juzgados WHERE nombre_ftp = ? ', (dependencia, ))
			juzgado.append(cur.fetchone()[0])
		
		fecha = re.findall("Fecha Despacho: (.+) -", texto)
		if len(fecha)==0:
			hay_fecha = False
			fecha = re.findall(".+_Pro_(.+).Txt", archivo[0])
			fechan = fecha[0].split("-")
			fecha[0] = "/".join([fechan[2],fechan[1],fechan[0]])
		for linea in texto.split("\n"):	
			if hay_fecha or hay_juzgado:	
				comienzo=re.findall("Expte. N°: .+", linea)
				termino = re.findall("------------------------------------------------------", linea)
			else:
				comienzo=re.findall(".+",linea)
				if texto.split("\n").index(linea)==len(texto.split("\n"))-1:
					termino =re.findall(".*",linea)

			if len(comienzo) == 1: comienza_prov=True
			if comienza_prov: proveido += linea
			
			if len(termino)==1 and comienza_prov:	
				comienza_prov = False
				proveidos.append(proveido)
				proveido = ""
			
		#pongo otro string en juzgado, porque el que extraigo contiene errores
		juzgado = re.findall("(.+)_Pro_.+.Txt", archivo[0])
		juzgado[0]= " ".join(juzgado[0].split("_"))
		print("juzgado",juzgado)
		print("fecha",fecha)
		print("cantidad de proveidos:",len(proveidos))
				

		cur.execute('''INSERT OR IGNORE INTO Juzgados (nombre, nombre_ftp) 
			VALUES ( ?,? )''', ( juzgado[0], dependencia ))
		cur.execute('SELECT id FROM Juzgados WHERE nombre = ? ', (juzgado[0], ))
		juzgado_id = cur.fetchone()[0]

		cur.execute('''INSERT OR IGNORE INTO Fechas (fecha) 
			VALUES ( ? )''', (fecha[0],))
		cur.execute('SELECT id FROM Fechas WHERE fecha = ? ', (fecha[0], ))
		fecha_id = cur.fetchone()[0]

		cur.execute('''INSERT OR IGNORE INTO Archivos (nombre, juzgado_id) 
			VALUES ( ?, )''', (archivo[0], juzgado_id))
		cur.execute('SELECT id FROM Archivos WHERE nombre = ? ', (archivo[0], juzgado_id ))
		archivo_id = cur.fetchone()[0]


		for proveido in proveidos:
			cur.execute('''INSERT OR IGNORE INTO Proveidos (fecha_id, texto, juzgado_id, archivo_id) 
				VALUES ( ?,?,?,? )''', (fecha_id, proveido, juzgado_id, archivo_id))
			
	conn.commit()


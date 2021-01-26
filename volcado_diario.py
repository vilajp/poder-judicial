import sqlite3
import re
import urllib.request, urllib.parse, urllib.error
from urllib.parse import urljoin
from urllib.parse import urlparse

baseurl = "ftp://ftp.justiciachaco.gov.ar/listas/"


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
			break
		except Exception as e:
			print("Unable to retrieve or parse page",url)
			print("Error",e)
	
	return document


conn = sqlite3.connect('volcado.sqlite')
cur = conn.cursor()

cur.execute("SELECT id, nombre_ftp FROM Juzgados")
juzgados = cur.fetchall()

cur.execute("SELECT juzgado_id, nombre FROM Archivos")
archivos = cur.fetchall()

#genero primero la estructura guardada en la base de datos

estructura_guardada = {}
dependencia_anterior = " "

for dependencia_ftp in juzgados:

#controlo que cambie de carpeta antes de cerear
	if dependencia_ftp[1]!=dependencia_anterior:
		estructura_guardada[dependencia_ftp[1]]=[]
	for archivo in archivos:
		if dependencia_ftp[0] == archivo[0]:
			estructura_guardada[dependencia_ftp[1]].append(archivo[1])
	dependencia_anterior = dependencia_ftp[1]
#genero estructura actual para comparar y actualizar
#primero proceso las dependencias

url = baseurl
document = abro_url(url)

texto = document.read().decode("ISO-8859-1")


dependencias = list()
estructura_actual = {}	

for linea in texto.split("\n"):
	res = re.findall("<DIR>          ([^(Zip)].+?_Pro)", linea)
	if len(res)>0:
		dependencias.append(res)
		estructura_actual[res[0]] = []

#segunda proceso archivos de cada dependencia
for dependencia in dependencias:
	url = baseurl + dependencia[0]
	document = abro_url(url)
	texto = document.read().decode("ISO-8859-1")

	for linea in texto.split("\n"):
		res = re.findall(".+[0-9] (.+.Txt)",linea)
		if len(res)>0:
			estructura_actual[dependencia[0]].append(res[0])

#ya tengo las dos estructuras la guardada en la base y la actual
#voy a comparar las dos estructuras

estructura = {}
for dependencia in estructura_actual.keys():
	if dependencia in estructura_guardada.keys():
		if estructura_actual[dependencia]==estructura_guardada[dependencia]:
			continue
		else:
			
			estructura[dependencia] = list(set(estructura_actual[dependencia]) - set(estructura_guardada[dependencia]))

print(estructura.items())

actualizo = 0 

#proceso cada archivo de la estructura
for dependencia in estructura.keys():

#controlo que todos los valores esten vacios en la estructura a actualizar
	if not estructura[dependencia]:actualizo += 1

	for archivo in estructura[dependencia]:
		url = baseurl + dependencia + "/" + archivo
		
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
			fecha = re.findall(".+_Pro_(.+).Txt", archivo)
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
		juzgado = re.findall("(.+)_Pro_.+.Txt", archivo)
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
			VALUES ( ?,? )''', (archivo, juzgado_id))
		cur.execute('SELECT id FROM Archivos WHERE nombre = ? ', (archivo, ))
		archivo_id = cur.fetchone()[0]


		for proveido in proveidos:
			cur.execute('''INSERT OR IGNORE INTO Proveidos (fecha_id, texto, juzgado_id, archivo_id) 
				VALUES ( ?,?,?,? )''', (fecha_id, proveido, juzgado_id, archivo_id))
			
	conn.commit()

if len(estructura) == actualizo:
	print("No hay nada para actualizar")
else:
	print(f"Se actualizaran {str(len(estructura)-actualizo)} archivos")






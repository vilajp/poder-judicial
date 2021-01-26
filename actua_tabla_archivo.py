import sqlite3

# Actualiza juzgado_id en tabla Archivos
# para que tenga identificacion de juzgado

conn = sqlite3.connect('volcado.sqlite')
cur = conn.cursor()

cur.execute("SELECT id, nombre FROM Juzgados")
juzgados = cur.fetchall()

cur.execute("SELECT id, nombre FROM Archivos")
archivos = cur.fetchall()

for juzgado in juzgados:
	for archivo in archivos:
		nombre = "_".join(juzgado[1].split())
		file = str(archivo[1])
		
		if nombre in file:
			cur.execute('''UPDATE Archivos 
						set juzgado_id = ? 
						where id = ? ''', (juzgado[0], archivo[0]))
	conn.commit()

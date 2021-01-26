import sqlite3

conn = sqlite3.connect('volcado.sqlite')
cur = conn.cursor()

cur.execute("SELECT id, texto FROM Proveidos WHERE juzgado_id = 1")
proveidos = cur.fetchall()
proveidos2 = proveidos


borrar = 0
for proveido in proveidos:
	
	for proveido2 in proveidos2[proveidos.index(proveido)+1:]:

		
		if proveido[1] == proveido2[1]:
			
			borrar += 1
			print(proveido[0], proveido2[0], borrar, end = "\r")
			cur.execute("DELETE from Proveidos where id = ?", (proveido2[0],))
			break
	if borrar%300==0:
		conn.commit()
	
	

import sqlite3
import time
import ssl
import urllib.request, urllib.parse, urllib.error
from urllib.parse import urljoin
from urllib.parse import urlparse
import re
from datetime import datetime, timedelta

conn = sqlite3.connect('proveidos.sqlite')
cur = conn.cursor()

baseurl = "ftp.justiciachaco.gov.ar"

cur.execute(
	'''CREATE TABLE IF NOT EXISTS Juzgado(
		id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE, 
	    nombre TEXT UNIQUE, 
	    circunscripcion_id INTEGER);

    CREATE TABLE IF NOT EXISTS Circunscripcion(
	    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
	    nombre TEXT UNIQUE);

    CREATE TABLE IF NOT EXISTS Expedientes(
	    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
	    numero TEXT UNIQUE,
	    caratula TEXT, 
	    profesionalA_id INTEGER, 
	    profesionalD_id INTEGER);

    CREATE TABLE IF NOT EXISTS Profesional(
	    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
	    matricula TEXT UNIQUE, 
	    nombre TEXT,
	    email TEXT UNIQUE);

    CREATE TABLE IF NOT EXISTS Actor(
	    id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
	    nombre TEXT);

	CREATE TABLE IF NOT EXISTS Proveidos(
		fecha TEXT,
		historia TEXT,
		providencia TEXT,
		expediente_id INTEGER,
		juzgado_id INTEGER,
		PRIMARY KEY(expediente_id, juzgado_id)
	)



     ''')

# Pick up where we left off
start = None
cur.execute('SELECT max(id) FROM Juzgado' )
try:
    row = cur.fetchone()
    if row is None :
        start = 0
    else:
        start = row[0]
except:
    start = 0

if start is None : start = 0

many = 0
count = 0
fail = 0
while True:
    if ( many < 1 ) :
        conn.commit()
        sval = input('How many messages:')
        if ( len(sval) < 1 ) : break
        many = int(sval)

    start = start + 1
    cur.execute('SELECT id FROM Juzgado WHERE id=?', (start,) )
    try:
        row = cur.fetchone()
        if row is not None : continue
    except:
        row = None

    many = many - 1
    url = baseurl + str(start) + '/' + str(start + 1)

    text = "None"
    try:
        # Open with a timeout of 30 seconds
        document = urllib.request.urlopen(url, None, 30)#, context=ctx)
        text = document.read().decode()
        if document.getcode() != 200 :
            print("Error code=",document.getcode(), url)
            break
    except KeyboardInterrupt:
        print('')
        print('Program interrupted by user...')
        break
    except Exception as e:
        print("Unable to retrieve or parse page",url)
        print("Error",e)
        fail = fail + 1
        if fail > 5 : break
        continue

    print(url,len(text))
    count = count + 1

    if not text.startswith("From "):
        print(text)
        print("Did not find From ")
        fail = fail + 1
        if fail > 5 : break
        continue

    pos = text.find("\n\n")
    if pos > 0 :
        hdr = text[:pos]
        body = text[pos+2:]
    else:
        print(text)
        print("Could not find break between headers and body")
        fail = fail + 1
        if fail > 5 : break
        continue

    email = None
    x = re.findall('\nFrom: .* <(\S+@\S+)>\n', hdr)
    if len(x) == 1 :
        email = x[0];
        email = email.strip().lower()
        email = email.replace("<","")
    else:
        x = re.findall('\nFrom: (\S+@\S+)\n', hdr)
        if len(x) == 1 :
            email = x[0];
            email = email.strip().lower()
            email = email.replace("<","")

    date = None
    y = re.findall('\Date: .*, (.*)\n', hdr)
    if len(y) == 1 :
        tdate = y[0]
        tdate = tdate[:26]
        try:
            sent_at = parsemaildate(tdate)
        except:
            print(text)
            print("Parse fail",tdate)
            fail = fail + 1
            if fail > 5 : break
            continue

    subject = None
    z = re.findall('\Subject: (.*)\n', hdr)
    if len(z) == 1 : subject = z[0].strip().lower();

    # Reset the fail counter
    fail = 0
    print("   ",email,sent_at,subject)
    cur.execute('''INSERT OR IGNORE INTO Messages (id, email, sent_at, subject, headers, body)
        VALUES ( ?, ?, ?, ?, ?, ? )''', ( start, email, sent_at, subject, hdr, body))
    if count % 50 == 0 : conn.commit()
    if count % 100 == 0 : time.sleep(1)

conn.commit()
cur.close()

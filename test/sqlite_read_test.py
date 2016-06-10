import sqlite3

conn = sqlite3.connect('dictionaries.db')
c = conn.cursor()

print "ENTITIES"
for row in c.execute('SELECT * FROM entities ORDER BY id'):
    print(row)

print "\nPROPERTIES"
for row in c.execute('SELECT * FROM properties ORDER BY id'):
    print(row)
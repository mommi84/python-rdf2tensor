from rdflib.plugins.parsers.ntriples import NTriplesParser, Sink
import sqlite3

# prepare DB
conn = sqlite3.connect('dictionaries.db')
c = conn.cursor()
# create tables
c.execute('''CREATE TABLE IF NOT EXISTS entities
             (id integer primary key autoincrement not null, entity text not null unique)''')
c.execute('''CREATE TABLE IF NOT EXISTS properties
             (id integer primary key autoincrement not null, property text not null unique)''')


class TensorSink(Sink):

	def triple(self,s,p,o):
		try:
			c.execute("INSERT INTO entities (entity) VALUES (?)", [(s)])
		except sqlite3.IntegrityError:
			pass
		try:    
			c.execute("INSERT INTO properties (property) VALUES (?)", [(p)])
		except sqlite3.IntegrityError:
			pass
		try:
			c.execute("INSERT INTO entities (entity) VALUES (?)", [(o)])
		except sqlite3.IntegrityError:
			pass

TSink = TensorSink()
g = NTriplesParser(TSink)
f = open("test.ttl", 'rb')
g.parse(f)
f.close()
conn.commit()
c.execute("SELECT count(*) FROM properties")
data = c.fetchone()[0]
print(data)
c.execute("SELECT count(*) FROM entities")
data = c.fetchone()[0]
print(data)
conn.close()
		

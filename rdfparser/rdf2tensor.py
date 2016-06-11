from rdflib.plugins.parsers.ntriples import NTriplesParser, Sink
import sqlite3

# prepare DB
conn = sqlite3.connect(':memory:')
c = conn.cursor()
# create tables
c.execute('''CREATE TABLE IF NOT EXISTS entities
             (id integer primary key autoincrement not null, entity text not null unique)''')
props = dict()
i=0
class TensorSink(Sink):

	def triple(self,s,p,o):
		global i
		#parse s,p,o to dictionaries/databases
		try:
			c.execute("INSERT INTO entities (entity) VALUES (?)", [(s)])
		except sqlite3.IntegrityError:
			pass
		try:    
			p_id = props[p][0]
		except KeyError:
			props[p] = (i, open("matrices/" + str(i) + ".mtx", "w+"))
			props[p][1].write("%%MatrixMarket matrix coordinate integer general\n%\nnum_ents num_ents num_nonZeros\n")
			p_id = i
			i += 1

		try:
			c.execute("INSERT INTO entities (entity) VALUES (?)", [(o)])
		except sqlite3.IntegrityError:
			pass

		c.execute("SELECT id FROM entities WHERE entity = ?", [(s)])
		s_id = c.fetchone()[0]
		c.execute("SELECT id FROM entities WHERE entity = ?", [(o)])
		o_id = c.fetchone()[0]
		props[p][1].write(("{} {} 1" + "\n").format(s_id, o_id))
		

TSink = TensorSink()
g = NTriplesParser(TSink)
f = open("test.ttl", 'rb')
g.parse(f)
f.close()
conn.commit()
size_props = len(props)
print(size_props)
c.execute("SELECT count(*) FROM entities")
size_ents = c.fetchone()[0]
print(size_ents)
#close all writers
for key in props:
	props[key][1].close()

conn.close()
		

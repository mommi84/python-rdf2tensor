from rdflib.plugins.parsers.ntriples import NTriplesParser, Sink
import sys
import sqlite3

reload(sys)
sys.setdefaultencoding("utf-8")

# prepare DB
conn = sqlite3.connect('dictionaries.db')
c = conn.cursor()
# create tables
c.execute('''CREATE TABLE IF NOT EXISTS entities
             (id integer primary key autoincrement not null, entity text not null unique)''')
c.execute('''CREATE TABLE IF NOT EXISTS properties
             (id integer primary key autoincrement not null, property text not null unique)''')


class StreamSink(Sink):
    
    def triple(self, s, p, o):
        self.length += 1
        # print "Stream of triples s={s}, p={p}, o={o}".format(s=s, p=p, o=o).encode('utf8')
        try:
            c.execute("INSERT INTO entities (entity) VALUES (?)", [(s)])
            s_id = c.lastrowid
        except sqlite3.IntegrityError:
            c.execute("SELECT id FROM entities WHERE entity = ?", [(s)])
            s_id = c.fetchone()[0]
        try:    
            c.execute("INSERT INTO properties (property) VALUES (?)", [(p)])
            p_id = c.lastrowid
        except sqlite3.IntegrityError:
            c.execute("SELECT id FROM properties WHERE property = ?", [(p)])
            p_id = c.fetchone()[0]
        try:
            c.execute("INSERT INTO entities (entity) VALUES (?)", [(o)])
            o_id = c.lastrowid
        except sqlite3.IntegrityError:
            c.execute("SELECT id FROM entities WHERE entity = ?", [(o)])
            o_id = c.fetchone()[0]
        
        # print "{} {} {}".format(s_id, p_id, o_id)

if __name__ == "__main__":
    # Create a new parser and try to parse the NT file.
    sk = StreamSink()
    n = NTriplesParser(sk)
    with open(sys.argv[1], "r") as anons:
        n.parse(anons)
    conn.commit()
    conn.close()
    print "triples = {}".format(sk.length)
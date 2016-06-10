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


if __name__ == "__main__":
    # Create a new parser and try to parse the NT file.
    sk = StreamSink()
    n = NTriplesParser(sk)
    with open(sys.argv[1], "r") as anons:
        n.parse(anons)
    conn.commit()
    conn.close()
    print "triples = {}".format(sk.length)
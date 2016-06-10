from rdflib.plugins.parsers.ntriples import NTriplesParser, Sink
import sys

reload(sys)
sys.setdefaultencoding("utf-8")

class StreamSink(Sink):
    
    def triple(self, s, p, o):
        self.length += 1
        print "Stream of triples s={s}, p={p}, o={o}".format(s=s, p=p, o=o).encode('utf8')
            

if __name__ == "__main__":
    # Create a new parser and try to parse the NT file.
    sk = StreamSink()
    n = NTriplesParser(sk)
    with open(sys.argv[1], "r") as anons:
        n.parse(anons)
    print "triples = {}".format(sk.length)
from rdflib.plugins.parsers.ntriples import NTriplesParser, Sink
import numpy as np
from scipy import sparse as sparse

#num_k = 110
#num_R = 30378
subject_dict = dict()
predicate_dict = dict()
i = 0
j = 0 
#T = np.array([sparse.lil_matrix((num_R,num_R),dtype = np.int8) for k in range(num_k)])

class TensorSink(Sink):

	def triple(self,s,p,o):
		global i 
		global j
		if s in subject_dict:
			b = subject_dict[s]
		else:
			subject_dict[s] = i
			b = i
			i += 1

		if o in subject_dict:
			c = subject_dict[o]
		else:
			subject_dict[o] = i
			c = i
			i += 1

		if p in predicate_dict:
			a = predicate_dict[p]
		else:
			predicate_dict[p] = j
			a = j
			j += 1
		#T[a][b,c] = 1	


print("...parsing Tensor...")
TSink = TensorSink()
g = NTriplesParser(TSink)
f = open("test.ttl", 'rb')
g.parse(f)
f.close()
print(len(subject_dict))
print(len(predicate_dict))
		

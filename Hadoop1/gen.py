import scipy.sparse as sparse
import numpy,sys
import random


def generate_stoc(dim,n,num,f):
	l=random.sample(range(0, n)+range(n+1,dim), num)
	l2=random.sample(range(0,1000) , num)
	l2=[float(i)/sum(l2) for i in l2]
	for v1,v2 in zip(l,l2):
		f.write(str(n)+","+str(v1)+","+str(v2)+"\n")

if __name__=="__main__":
	dim=int(sys.argv[1])
	density=float(sys.argv[2])
	num_in_each=int(density*dim)
	f=open("/home/kai/mm2/a.txt","w")
	for i in range(dim):
		generate_stoc(dim,i,num_in_each,f)
    	f.close()
    	f=open("/home/kai/mm2/b.txt","w")
    	for i in range(dim):
        	f.write(str(i)+","+str(0)+","+str(float(1)/dim)+"\n")
    	f.close()
			
		

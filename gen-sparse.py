#!/usr/bin/env python

import csv
import numpy
import scipy.sparse

def randIntMatrix(n, density):
    ij   = numpy.random.randint(n, size=(2, n*n*density))
    data = numpy.random.rand(n*n*density)
    A    = scipy.sparse.coo.coo_matrix((data, ij), (n, n))
    
    return A.tocsr()

def writeFile(A, filename):
    rows, cols = A.nonzero()
    with open(filename, 'wb') as csvfile:
        w = csv.writer(csvfile)
        for i in range(len(A.data)):
            w.writerow([rows[i], cols[i], A.data[i]])

if __name__ == "__main__":
    size    = 1e4
    density = 1e-3
    
    M = randIntMatrix(size, density)
    N = randIntMatrix(size, density)
    writeFile(M, 'M-gen.txt')
    writeFile(N, 'N-gen.txt')

This project implements block matrix addition using the MapReduce framework. The goal is to perform efficient addition operations on large sparse matrices by converting them into block matrices and then applying the addition operation on corresponding blocks.

The project begins by taking two sparse matrices as input. These matrices are represented as datasets of triples (i, j, v), where i and j are indices, and v is the value of the matrix at indices i and j.

The sparse matrices are converted into block matrices, where each block is a dense matrix of a specified size. The block matrix representation is achieved using the Hadoop Distributed File System (HDFS) by storing the blocks as binary files in SequenceTextInputFormat. Each block is indexed by a pair of block coordinates (ci, cj), constructed with the Java class Pair. The matrix element Mij is stored inside the block with block coordinates (i/rows, j/columns) at the location (i%rows, j%columns) inside the block.

The conversion from sparse matrices to block matrices is implemented using MapReduce. The Map phase maps each matrix element to its corresponding block using block coordinates. The Reduce phase then assembles the blocks by aggregating the matrix elements within each block.

Finally, block matrix addition is performed by finding blocks from the two block matrices with the same block coordinates and adding their corresponding elements using regular matrix addition in Java.

This project demonstrates the use of MapReduce for efficiently processing large sparse matrices and showcases the advantages of block matrix representation and parallel processing for matrix operations.

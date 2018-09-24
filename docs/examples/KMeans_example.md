### K-Means Clustering

The need to process large amounts of continuously arriving information has led to the exploration 
and application of big data analytics techniques. Likewise, the painstaking process of clustering 
numerous datasets containing large numbers of records with high dimensions calls for innovative 
methods. Traditional sequential clustering algorithms are unable to handle it. They are not scalable
in relation to larger sizes of data sets, and they are most often computationally expensive in 
memory space and time complexities. Yet, the parallelization of data clustering algorithms is 
paramount when dealing with big data. K-Means clustering is an iterative algorithm hence, it  
requires a large number of iterative steps to find an optimal solution, and this procedure increases 
the processing time of clustering. Twister2 provides a dataflow task graph based approach to 
distribute the tasks in a parallel manner and aggregate the results which reduces the processing 
time of K-Means Clustering process. 

### Implementation

First, the KMeansSource is invoked to distribute the datapoints which is based on the number of 
workers. For every iteration, the new centroid value is calculated and the calculated value is 
distributed across all the task instances. This process repeats for ‘N’ number of iterations as 
specified in the KMeans Constants file. At the end of every iteration, the centroid value is updated
based on the calculated value and it will continue until the end of iteration. The PROBLEM_DIMENSION
is used to set the features selected for the clustering analytics process. 

### KMeans Constants

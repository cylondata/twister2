#Support Vector Machines

Support Vector Machines (SVM) is a supervised learning algorithm which is mainly used for the
purpose of classifying data. SVM algorithm is a light-weight classifier compared to Deep Neural
Networks. SVM is a highly used algorithm for classifying data. There are many varieties of SVMs 
which has been developed through out the years. The sequential minimal optimization ([SMO](https://pdfs.semanticscholar.org/59ee/e096b49d66f39891eb88a6c84cc89acba12d.pdf)) based approach 
is one of the most famouos modes in the early days of SVM. Then the research progreeses towards 
matrix decomposition methods like [PSVM](https://static.googleusercontent.com/media/research.google.com/en//pubs/archive/34638.pdf).
Both these methods are high computational intensive algorithms. Then the current research has 
diverted towards using gradient descent based approaches to solve the problem. These researchs show 
that this method is highly efficient and accuracy is as high as the traditional SMO-based and
matrix decomposition-based approaches. 

## Background

Currently Twister2 supports SGD-based SVM binary classifier with Linear Kernel. Our objective 
is to provide fully functional multi-class SVM classifier with multiple kernel support in a future
release.

Here S is a sample space where x<sub>i</sub> and y<sub>i</sub>,

S with n samples,x<sub>i</sub> refers to a d-dimensional feature vector and y<sub>i</sub> 
is  the  label  of  the i<sup>th</sup> sample.  

![eq1](images/eq1.gif)

J<sup>t</sup> refers to the quadratic objective function which is minimized in the algorithm 
to obtain the convergence in the algorithm. 

![eq2](images/eq2.gif)

This is the constraint of the objective function 

![eq3](images/eq4.gif)

The weight vector updates takes as follows. Depending on the gradient of the objective function,

![eq4](images/eq3.gif)


You can find the code to our SVM Implementation 
[here](https://github.com/DSC-SPIDAL/twister2/blob/master/twister2/examples/src/java/edu/iu/dsc/tws/examples/ml/svm/job/SvmSgdAdvancedRunner.java).


 




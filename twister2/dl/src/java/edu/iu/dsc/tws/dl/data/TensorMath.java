//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
package edu.iu.dsc.tws.dl.data;

/**
 * It provides multiple math operation functions for manipulating Tensor objects.
 * All functions support both allocating a new Tensor to return the result
 * and treating the caller as a target Tensor, in which case the target Tensor(s)
 * will be resized accordingly and filled with the result. This property is especially
 * useful when one wants to have tight control over when memory is allocated.
 *
 * @tparam T should be double or float
 */
public interface TensorMath<T> {
  // scalastyle:off methodName

  /**
   * Add all elements of this with value not in place.
   * It will allocate new memory.
   *
   * @param s
   * @return
   */

  Tensor<T> addCopy(T s);

  /**
   * Add a Tensor to another one, return the result in new allocated memory.
   * The number of elements in the Tensors must match, but the sizes do not matter.
   * The size of the returned Tensor will be the size of the first Tensor
   *
   * @param t
   * @return
   */
  Tensor<T> addCopy(Tensor<T> t);

  /**
   * subtract all elements of this with the value not in place.
   * It will allocate new memory.
   *
   * @param s
   * @return
   */
  Tensor<T> subCopy(T s);

  /**
   * Subtract a Tensor from another one, return the result in new allocated memory.
   * The number of elements in the Tensors must match, but the sizes do not matter.
   * The size of the returned Tensor will be the size of the first Tensor
   *
   * @param t
   * @return
   */
  Tensor<T> subCopy(Tensor<T> t);

  /**
   * divide all elements of this with value not in place.
   * It will allocate new memory.
   *
   * @param s
   * @return
   */
  Tensor<T> divCopy(T s);

  /**
   * Divide a Tensor by another one, return the result in new allocated memory.
   * The number of elements in the Tensors must match, but the sizes do not matter.
   * The size of the returned Tensor will be the size of the first Tensor
   *
   * @param t
   * @return
   */
  Tensor<T> divCopy(Tensor<T> t);

  /**
   * multiply all elements of this with value not in place.
   * It will allocate new memory.
   *
   * @param s
   * @return
   */
  Tensor<T> mulCopy(T s);

  /**
   * Multiply a Tensor by another one, return the result in new allocated memory.
   * The number of elements in the Tensors must match, but the sizes do not matter.
   * The size of the returned Tensor will be the size of the first Tensor
   *
   * @param t
   * @return
   */
  Tensor<T> mulCopy(Tensor<T> t);
  // scalastyle:on methodName

  /**
   * returns the sum of the elements of this
   *
   * @return
   */
  T sum();

  /**
   * returns the product of the elements of this
   *
   * @return
   */
  T prod();

  Tensor<T> prod(Tensor<T> x, int dim);

  /**
   * performs the sum operation over the dimension dim
   *
   * @param dim
   * @return
   */
  Tensor<T> sum(int dim);

  Tensor<T> sum(Tensor<T> x, int dim);

  /**
   * returns the mean of all elements of this.
   *
   * @return
   */
  T mean();

  /**
   * performs the mean operation over the dimension dim.
   *
   * @param dim
   * @return
   */
  Tensor<T> mean(int dim);

  /**
   * returns the single biggest element of x
   *
   * @return
   */
  T max();

  /**
   * performs the max operation over the dimension n
   *
   * @param dim
   * @return
   */
  TensorPair<T, T> max(int dim);

  /**
   * performs the max operation over the dimension n
   *
   * @param values
   * @param indices
   * @param dim
   * @return
   */
  TensorPair<T, T> max(Tensor<T> values, Tensor<T> indices, int dim);

  /**
   * returns the single minimum element of x
   *
   * @return
   */
  T min();

  /**
   * performs the min operation over the dimension n
   *
   * @param dim
   * @return
   */
  TensorPair<T, T> min(int dim);

  /**
   * performs the min operation over the dimension n
   *
   * @param values
   * @param indices
   * @param dim
   * @return
   */
  TensorPair<T, T> min(Tensor<T> values, Tensor<T> indices, int dim);

  /**
   * Writes all values from tensor src into this tensor at the specified indices
   *
   * @param dim
   * @param index
   * @param src
   * @return this
   */
  Tensor<T> scatter(int dim, Tensor<T> index, Tensor<T> src);

  /**
   * change this tensor with values from the original tensor by gathering a number of values
   * from each "row", where the rows are along the dimension dim.
   *
   * @param dim
   * @param index
   * @param src
   * @return this
   */
  Tensor<T> gather(int dim, Tensor<T> index, Tensor<T> src);

  /**
   * This function computes 2 dimensional convolution of a single image
   * with a single kernel (2D output). the dimensions of input and kernel
   * need to be 2, and Input image needs to be bigger than kernel. The
   * last argument controls if the convolution is a full ('F') or valid
   * ('V') convolution. The default is valid convolution.
   *
   * @param kernel
   * @param vf     full ('F') or valid ('V') convolution.
   * @return
   */
  Tensor<T> conv2(Tensor<T> kernel, char vf);

  /**
   * This function operates with same options and input/output configurations as conv2,
   * but performs cross-correlation of the input with the kernel k.
   *
   * @param kernel
   * @param vf     full ('F') or valid ('V') convolution.
   * @return
   */
  Tensor<T> xcorr2(Tensor<T> kernel, char vf);

  /**
   * replaces all elements in-place with the square root of the elements of this.
   *
   * @return
   */
  Tensor<T> sqrt();

  /**
   * replaces all elements in-place with the tanh root of the elements of this.
   *
   * @return
   */
  Tensor<T> tanh();

  /**
   * replaces all elements in-place with the absolute values of the elements of this.
   *
   * @return
   */
  Tensor<T> abs();

  /**
   * x.add(value,y) multiply-accumulates values of y into x.
   *
   * @param value scalar
   * @param y     other tensor
   * @return current tensor
   */
  Tensor<T> add(T value, Tensor<T> y);

  /**
   * accumulates all elements of y into this
   *
   * @param y other tensor
   * @return current tensor
   */
  Tensor<T> add(Tensor<T> y);
  // Puts the result of x + value * y in current tensor

  /**
   * z.add(x, value, y) puts the result of x + value * y in z.
   *
   * @param x
   * @param value
   * @param y
   * @return
   */
  Tensor<T> add(Tensor<T> x, T value, Tensor<T> y);

  /**
   * x.add(value) : add value to all elements of x in place.
   *
   * @param value
   * @return
   */
  Tensor<T> add(T value);

  Tensor<T> add(Tensor<T> x, Tensor<T> y);

  /**
   * Performs the dot product. The number of elements must match: both Tensors are seen as a 1D
   * vector.
   *
   * @param y
   * @return
   */
  T dot(Tensor<T> y);


  /**
   * For each elements of the tensor, performs the max operation compared with the given value
   * vector.
   *
   * @param value
   * @return
   */
  Tensor<T> cmax(T value);

  /**
   * Performs the p-norm distance calculation between two tensors
   *
   * @param y    the secode Tensor
   * @param norm the norm of distance
   * @return
   */
  T dist(Tensor<T> y, int norm);

  /**
   * Performs the element-wise multiplication of tensor1 by tensor2, multiply the result by the
   * scalar value (1 if not present) and add it to x. The number of elements must match, but sizes
   * do not matter.
   *
   * @param value
   * @param tensor1
   * @param tensor2
   */
  Tensor<T> addcmul(T value, Tensor<T> tensor1, Tensor<T> tensor2);

  Tensor<T> addcmul(Tensor<T> tensor1, Tensor<T> tensor2);

  /**
   * Performs the element-wise division of tensor1 by tensor2, multiply the result by the scalar
   * value and add it to x.
   * The number of elements must match, but sizes do not matter.
   *
   * @param value
   * @param tensor1
   * @param tensor2
   * @return
   */
  Tensor<T> addcdiv(T value, Tensor<T> tensor1, Tensor<T> tensor2);

  Tensor<T> sub(T value, Tensor<T> y);

  // Puts the result of x - value * y in current tensor
  Tensor<T> sub(Tensor<T> x, T value, Tensor<T> y);

  /**
   * subtracts all elements of y from this
   *
   * @param y other tensor
   * @return current tensor
   */
  Tensor<T> sub(Tensor<T> y);

  Tensor<T> sub(Tensor<T> x, Tensor<T> y);

  Tensor<T> sub(T value);

  /**
   * Element-wise multiply
   * x.cmul(y) multiplies all elements of x with corresponding elements of y.
   * x = x * y
   *
   * @param y tensor
   * @return current tensor
   */
  Tensor<T> cmul(Tensor<T> y);

  /**
   * Element-wise multiply
   * z.cmul(x, y) equals z = x * y
   *
   * @param x tensor
   * @param y tensor
   * @return current tensor
   */
  Tensor<T> cmul(Tensor<T> x, Tensor<T> y);

  /**
   * Element-wise divide
   * x.cdiv(y) all elements of x divide all elements of y.
   * x = x / y
   *
   * @param y tensor
   * @return current tensor
   */
  Tensor<T> cdiv(Tensor<T> y);

  /**
   * Element-wise divide
   * z.cdiv(x, y) means z = x / y
   *
   * @param x tensor
   * @param y tensor
   * @return current tensor
   */
  Tensor<T> cdiv(Tensor<T> x, Tensor<T> y);

  /**
   * multiply all elements of this with value in-place.
   *
   * @param value
   * @return
   */
  Tensor<T> mul(T value);

  /**
   * divide all elements of this with value in-place.
   *
   * @param value
   * @return
   */
  Tensor<T> div(T value);

  /**
   * Element-wise divide
   * x.div(y) all elements of x divide all elements of y.
   * x = x / y
   *
   * @param y tensor
   * @return current tensor
   */
  Tensor<T> div(Tensor<T> y);

  /**
   * put the result of x * value in current tensor
   *
   * @param value
   * @return
   */
  Tensor<T> mul(Tensor<T> x, T value);

  /**
   * Performs a matrix-matrix multiplication between mat1 (2D tensor) and mat2 (2D tensor).
   * Optional values v1 and v2 are scalars that multiply m and mat1 * mat2 respectively.
   * Optional value beta is a scalar that scales the result tensor, before accumulating the result
   * into the tensor. Defaults to 1.0.
   * If mat1 is a n x m matrix, mat2 a m x p matrix, m must be a n x p matrix.
   * <p>
   * res = (v1 * m) + (v2 * mat1*mat2)
   *
   * @param v1
   * @param m
   * @param v2
   * @param mat1
   * @param mat2
   */
  Tensor<T> addmm(T v1, Tensor<T> m, T v2, Tensor<T> mat1, Tensor<T> mat2);

  /**
   * res = m + (mat1*mat2)
   */
  Tensor<T> addmm(Tensor<T> m, Tensor<T> mat1, Tensor<T> mat2);

  /**
   * res = res + mat1 * mat2
   */
  Tensor<T> addmm(Tensor<T> mat1, Tensor<T> mat2);

  /**
   * res = res + v2 * mat1 * mat2
   */
  Tensor<T> addmm(T v2, Tensor<T> mat1, Tensor<T> mat2);

  /**
   * res = v1 * res + v2 * mat1*mat2
   */
  Tensor<T> addmm(T v1, T v2, Tensor<T> mat1, Tensor<T> mat2);

  /**
   * res = mat1*mat2
   */
  Tensor<T> mm(Tensor<T> mat1, Tensor<T> mat2);

  /**
   * Performs the outer-product between vec1 (1D tensor) and vec2 (1D tensor).
   * Optional values v1 and v2 are scalars that multiply mat and vec1 [out] vec2 respectively.
   * In other words,
   * res_ij = (v1 * mat_ij) + (v2 * vec1_i * vec2_j)
   *
   * @param t1
   * @param t2
   * @return
   */
  Tensor<T> addr(Tensor<T> t1, Tensor<T> t2);

  Tensor<T> addr(T v1, Tensor<T> t1, Tensor<T> t2);

  Tensor<T> addr(T v1, Tensor<T> t1, T v2, Tensor<T> t2);

  /**
   * Performs the outer-product between vec1 (1D Tensor) and vec2 (1D Tensor).
   * Optional values v1 and v2 are scalars that multiply mat and vec1 [out] vec2 respectively.
   * In other words,res_ij = (v1 * mat_ij) + (v2 * vec1_i * vec2_j)
   *
   * @param v1
   * @param t1
   * @param v2
   * @param t2
   * @param t3
   * @return
   */
  Tensor<T> addr(T v1, Tensor<T> t1, T v2, Tensor<T> t2, Tensor<T> t3);

  /**
   * return pseudo-random numbers, require 0<=args.length<=2
   * if args.length = 0, return [0, 1)
   * if args.length = 1, return [1, args(0)] or [args(0), 1]
   * if args.length = 2, return [args(0), args(1)]
   *
   * @param args
   */
  T uniform(T... args);

  /**
   * Performs a matrix-vector multiplication between mat (2D Tensor) and vec2 (1D Tensor) and add
   * it to vec1. Optional values v1 and v2 are scalars that multiply vec1 and vec2 respectively.
   * <p>
   * In other words,
   * res = (beta * vec1) + alpha * (mat * vec2)
   * <p>
   * Sizes must respect the matrix-multiplication operation: if mat is a n × m matrix,
   * vec2 must be vector of size m and vec1 must be a vector of size n.
   */
  Tensor<T> addmv(T beta, Tensor<T> vec1, T alpha, Tensor<T> mat, Tensor<T> vec2);

  /**
   * res = beta * res + alpha * (mat * vec2)
   */
  Tensor<T> addmv(T beta, T alpha, Tensor<T> mat, Tensor<T> vec2);

  /**
   * res = res + alpha * (mat * vec2)
   */
  Tensor<T> addmv(T alpha, Tensor<T> mat, Tensor<T> vec2);

  /**
   * res = res + (mat * vec2)
   */
  Tensor<T> mv(Tensor<T> mat, Tensor<T> vec2);

  /**
   * Perform a batch matrix matrix multiplication of matrices and stored in batch1 and batch2
   * with batch add. batch1 and batch2 must be 3D Tensors each containing the same number of
   * matrices. If batch1 is a b × n × m Tensor, batch2 a b × m × p Tensor, res will be a
   * b × n × p Tensor.
   * <p>
   * In other words,
   * res_i = (beta * M_i) + (alpha * batch1_i * batch2_i)
   */
  Tensor<T> baddbmm(T beta, Tensor<T> m, T alpha, Tensor<T> batch1, Tensor<T> batch2);

  /**
   * res_i = (beta * res_i) + (alpha * batch1_i * batch2_i)
   */
  Tensor<T> baddbmm(T beta, T alpha, Tensor<T> batch1, Tensor<T> batch2);

  /**
   * res_i = res_i + (alpha * batch1_i * batch2_i)
   */
  Tensor<T> baddbmm(T alpha, Tensor<T> batch1, Tensor<T> batch2);

  /**
   * res_i = res_i + batch1_i * batch2_i
   */
  Tensor<T> bmm(Tensor<T> batch1, Tensor<T> batch2);

  /**
   * Replaces all elements in-place with the elements of x to the power of n
   *
   * @param y
   * @param n
   * @return current tensor reference
   */
  Tensor<T> pow(Tensor<T> y, T n);

  Tensor<T> pow(T n);

  /**
   * Replaces all elements in-place with the elements of x squared
   *
   * @return current tensor reference
   */
  Tensor<T> square();

  /**
   * Populate the given tensor with the floor result of elements
   *
   * @param y
   * @return
   */
  Tensor<T> floor(Tensor<T> y);

  /**
   * Replaces all elements in-place with the floor result of elements
   *
   * @return
   */
  Tensor<T> floor();

  /**
   * Replaces all elements in-place with the ceil result of elements
   *
   * @return
   */
  Tensor<T> ceil();

  /**
   * Computes the reciprocal of this tensor element-wise and update the content inplace
   *
   * @return
   */
  Tensor<T> inv();

  /**
   * Computes the reciprocal of this tensor element-wise and update the content inplace
   *
   * @return
   */
  Tensor<T> erf();

  /**
   * Computes the reciprocal of this tensor element-wise and update the content inplace
   *
   * @return
   */
  Tensor<T> erfc();

  /**
   * Computes the log of the absolute value of `Gamma(x)` element-wise,
   * and update the content inplace
   *
   * @return
   */
  Tensor<T> logGamma();

  /**
   * Computes Psi, the derivative of Lgamma (the log of the absolute value of
   * `Gamma(x)`), element-wise and update the content inplace
   *
   * @return
   */
  Tensor<T> digamma();

  /**
   * Get the top k smallest values and their indices.
   *
   * @param result       result buffer
   * @param indices      indices buffer
   * @param k
   * @param dim          dimension, default is the last dimension (default -1)
   * @param increase     sort order, set it to true if you want to get the smallest
   *                     top k values (default true)
   * @param sortedResult default value true
   * @return
   */
  TensorPair<T, T> topk(int k, int dim, boolean increase, Tensor<T> result,
                        Tensor<T> indices, boolean sortedResult);

  /**
   * Replaces all elements in-place with the elements of lnx
   *
   * @param y
   * @return current tensor reference
   */
  Tensor<T> log(Tensor<T> y);

  Tensor<T> exp(Tensor<T> y);

  Tensor<T> sqrt(Tensor<T> y);

  Tensor<T> tanh(Tensor<T> y);

  Tensor<T> log1p(Tensor<T> y);

  Tensor<T> log();

  Tensor<T> exp();

  Tensor<T> log1p();

  Tensor<T> abs(Tensor<T> x);

  /**
   * returns the p-norms of the Tensor x computed over the dimension dim.
   *
   * @param y     result buffer
   * @param value
   * @param dim
   * @return
   */
  Tensor<T> norm(Tensor<T> y, int value, int dim);

  /**
   * Implements > operator comparing each element in x with y
   *
   * @param x
   * @param y
   * @return current tensor reference
   */
  Tensor<T> gt(Tensor<T> x, Tensor<T> y);

  /**
   * Implements < operator comparing each element in x with y
   *
   * @param x
   * @param y
   * @return current tensor reference
   */
  Tensor<T> lt(Tensor<T> x, Tensor<T> y);

  /**
   * Implements <= operator comparing each element in x with y
   *
   * @param x
   * @param y
   * @return current tensor reference
   */
  Tensor<T> le(Tensor<T> x, Tensor<T> y);

  /**
   * Implements == operator comparing each element in x with y
   *
   * @param y
   * @return current tensor reference
   */
  Tensor<T> eq(Tensor<T> x, T y);

  /**
   * Fills the masked elements of itself with value val
   *
   * @param mask
   * @param e
   * @return current tensor reference
   */
  Tensor<T> maskedFill(Tensor<T> mask, T e);

  /**
   * Copies the elements of tensor into mask locations of itself.
   *
   * @param mask
   * @param y
   * @return current tensor reference
   */
  Tensor<T> maskedCopy(Tensor<T> mask, Tensor<T> y);

  /**
   * Returns a new Tensor which contains all elements aligned to a 1 in the corresponding mask.
   *
   * @param mask
   * @param y
   * @return current tensor reference
   */
  Tensor<T> maskedSelect(Tensor<T> mask, Tensor<T> y);

  /**
   * returns the sum of the n-norms on the Tensor x
   *
   * @param value the n-norms
   * @return
   */
  T norm(int value);

  /**
   * returns a new Tensor with the sign (+/- 1 or 0) of the elements of x.
   *
   * @return
   */
  Tensor<T> sign();

  /**
   * Implements >= operator comparing each element in x with value
   *
   * @param x
   * @param value
   * @return
   */
  Tensor<T> ge(Tensor<T> x, double value);

  /**
   * Accumulate the elements of tensor into the original tensor by adding to the indices
   * in the order given in index. The shape of tensor must exactly match the elements indexed
   * or an error will be thrown.
   *
   * @param dim
   * @param index
   * @param y
   * @return
   */
  Tensor<T> indexAdd(int dim, Tensor<T> index, Tensor<T> y);

  /**
   * Accumulate the elements of tensor into the original tensor by adding to the indices
   * in the order given in index. The shape of tensor must exactly match the elements indexed
   * or an error will be thrown.
   *
   * @param dim
   * @param index
   * @param y
   * @return
   */
  Tensor<T> index(int dim, Tensor<T> index, Tensor<T> y);

  /**
   * stores the element-wise maximum of x and y in x.
   * x.cmax(y) = max(x, y)
   *
   * @param y tensor
   * @return current tensor
   */
  Tensor<T> cmax(Tensor<T> y);

  /**
   * stores the element-wise maximum of x and y in x.
   * x.cmin(y) = min(x, y)
   *
   * @param y tensor
   * @return current tensor
   */
  Tensor<T> cmin(Tensor<T> y);

  /**
   * stores the element-wise maximum of x and y in z.
   * z.cmax(x, y) means z = max(x, y)
   *
   * @param x tensor
   * @param y tensor
   */
  Tensor<T> cmax(Tensor<T> x, Tensor<T> y);

  /**
   * stores the element-wise maximum of x and y in z.
   * z.cmin(x, y) means z = min(x, y)
   *
   * @param x tensor
   * @param y tensor
   */
  Tensor<T> cmin(Tensor<T> x, Tensor<T> y);

  /**
   * resize this tensor size to floor((xmax - xmin) / step) + 1 and set values from
   * xmin to xmax with step (default to 1).
   *
   * @param xmin
   * @param xmax
   * @param step default 1
   * @return this tensor
   */
  Tensor<T> range(double xmin, double xmax, int step);

  /**
   * Computes numerical negative value element-wise. y = -x
   *
   * @param x
   * @return this tensor
   */
  Tensor<T> negative(Tensor<T> x);

  //TODO: implement for reduce
// /**
//  * Reduce along the given dimension with the given reducer, and copy the result to the result
//  * tensor
//  * @param dim
//  * @param result
//  * @param reducer
//  */
// Tensor<T> reduce(int dim,Tensor<T> result, reducer: (T, T) => T);
  T sumSquare();

  Tensor<T> clamp(double min, double max);
}


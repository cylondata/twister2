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

import java.io.Serializable;

public interface TensorNumeric<T> extends Serializable {
  //T one: T = fromType<int>(1)

  //T zero: T = fromType[Int](0)

  T plus(T x, T y);

  T minus(T x, T y);

  T times(T x, T y);

  T divide(T x, T y);
  T exp(T x);
  T log(T x);
  T max(T x, T y);
  T min(T x, T y);
  T sqrt(T x);
  T tanh(T x);
  T abs(T x);
  T or(T x, T y);
  T and(T x, T y);
  T negative(T x);
  T pow(T x);
  T pow(T x, T y);
  T log1p(T x);
  boolean isGreater(T x, T y);

  boolean isGreaterEq(T x, T y);
  T rand();
  T randn();
  T gemm(char transa, char transb, int m, int n, int k, T alpha, T[] a,
           int aOffset, int lda, T[] b, int bOffset, int ldb,
           T beta, T[] c, int cOffset,int ldc);

  T gemv(char trans, int m, int n, T alpha, T[] a, int aoffset, int lda,
           T[] x,int xOffset, int incx, T beta, T[] y, int yOffset, int incy);

  T axpy(int n, T da,T[] dx,int _dx_offset, int incx, T[] dy,
            int _dy_offset, int incy);

  T dot(int n,T[] dx,int _dx_offset, int incx, T[] dy, int _dy_offset,
          int incy);
  T ger(int m, int n, T alpha,T[] x, int _x_offset, int incx, T[] y,
          int _y_offset, int incy, T[] a, int _a_offset, int lda);

  void fill(T[] data,int fromIndex,int toIndex,T value);

  T fromType[K](k: K)(implicit c: ConvertableFrom[K]);
  T toType[K](t: T)(implicit c: ConvertableTo[K]); K

  T vPowx(int n, T[] a, int aOffset, T b, T[] y, int yOffset); Unit

  T vLn(int n, T[] a, int aOffset, T[] y, int yOffset); Unit

  T vExp(int n, T[] a, int aOffset, T[] y, int yOffset); Unit

  T vSqrt(int n, T[] a, int aOffset, T[] y, int yOffset); Unit

  T vTanh(int n, T[] a, int aOffset, T[] y, int yOffset); Unit

  T vAbs(int n, T[] a, int aOffset, T[] y, int yOffset); Unit

  T vLog1p(int n, T[] a, int aOffset, T[] y, int yOffset); Unit

  T scal(int n, sa: T, sx: Array[T], offset: Int, int incx); Unit

  T inv(T v);
  T erf(T v);
  T erfc(T v);
  T logGamma(T v);
  T digamma(T v);
  T add(int n, T[] a, offset: Int, T v, stride: Int); Unit

  T sub(int n, T[] a, offset: Int, T v, stride: Int); Unit

  T vAdd(int n, T[] a, int aOffset, T[] b, int bOffset, T[] y,
           int yOffset); Unit

  T vSub(int n, T[] a, int aOffset, T[] b, int bOffset, T[] y,
           int yOffset); Unit

  T vMul(int n, T[] a, int aOffset, T[] b, int bOffset, T[] y,
           int yOffset); Unit

  T vDiv(int n, T[] a, int aOffset, T[] b, int bOffset, T[] y,
           int yOffset); Unit

  T sum(int n, T[] a, int aOffset, stride: Int);
  T prod(int n, T[] a, int aOffset, stride: Int);
  T arraycopy(src: Array[T], srcPos: Int,
                dest: Array[T], destPos: Int, length: Int); Unit

  T getType(); TensorDataType

  T addcmul(value: T, int n,
              self: Array[T], selfOffset: Int,
              T[] a, int aOffset,
              T[] b, int bOffset); Unit

  T addcdiv(value: T, int n,
              self: Array[T], selfOffset: Int,
              T[] a, int aOffset,
              T[] b, int bOffset); Unit

  boolean nearlyEqual(a: T, T b, epsilon: Double);

  T floor(a: T);
  T ceil(a: T);
  boolean isFinite(a: T);

  boolean isNan(a: T);

  boolean isInf(a: T);

  T round(a: T);
  T truncate(a: T);
  T floorDiv(a: T, T b);
  T clip(a: T, lower: T, upper: T);}


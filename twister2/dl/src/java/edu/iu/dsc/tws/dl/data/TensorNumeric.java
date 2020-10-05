//  Licensed under the Apache License, Version 2.0 (the "License"){};
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

import edu.iu.dsc.tws.dl.utils.RandomGenerator;
import com.intel.analytics.bigdl.mkl.MKL;

import java.io.Serializable;
import java.util.Arrays;

public class TensorNumeric<T> implements Serializable {
  //T one: T = fromType<int>(1)

  //T zero: T = fromType[Int](0)

  //  T fromType[K](k: K)(implicit c: ConvertableFrom[K]){};
  //  T toType[K](t: T)(implicit c: ConvertableTo[K]){}; K
  // TensorDataType getType(){};


  //double opertions
  double plus(double x, double y) {
    return x + y;
  }

  double minus(double x, double y) {
    return x - y;
  }

  double times(double x, double y) {
    return x * y;
  }

  double divide(double x, double y) {
    return x / y;
  }

  double exp(double x) {
    return exp(x);
  }

  double log(double x) {
    return log(x);
  }

  double max(double x, double y) {
    return Math.max(x, y);
  }

  double min(double x, double y) {
    return Math.min(x, y);
  }

  double sqrt(double x) {
    return Math.sqrt(x);
  }

  double tanh(double x) {
    return Math.tanh(x);
  }

  double abs(double x) {
    return Math.abs(x);
  }

  double or(double x, double y) {
    throw new UnsupportedOperationException("Operation not supported");
  }

  double and(double x, double y) {
    throw new UnsupportedOperationException("Operation not supported");
  }

  double negative(double x) {
    return -x;
  }

  double pow(double x) {
    return Math.pow(x, -1);
  }

  double pow(double x, double y) {
    return Math.pow(x, y);
  }

  double log1p(double x) {
    return Math.log1p(x);
  }

  boolean isGreater(double x, double y) {
    return x > y;
  }

  boolean isGreaterEq(double x, double y) {
    return x >= y;
  }

  double rand() {
    return RandomGenerator.RNG().uniform(0, 1);
  }

  double randn() {
    return RandomGenerator.RNG().normal(0, 1);
  }

  double gemm(char transa, char transb, int m, int n, int k, double alpha, double[] a,
              int aOffset, int lda, double[] b, int bOffset, int ldb,
              double beta, double[] c, int cOffset, int ldc) {
    checkMKL();
    MKL.vdgemm(transa, transb, m, n, k, alpha, a, aOffset, lda, b,
        bOffset, ldb, beta, c, cOffset, ldc)
  }

  double gemv(char trans, int m, int n, double alpha, double[] a, int aoffset, int lda,
              double[] x, int xOffset, int incx, double beta, double[] y, int yOffset, int incy) {
    checkMKL();

    MKL.vdgemv(trans, m, n, alpha, a, aoffset, lda, x, xOffset,
        incx, beta, y, yOffset, incy)
  }

  double axpy(int n, double da, double[] dx, int _dx_offset, int incx, double[] dy,
              int _dy_offset, int incy) {
    checkMKL();

    MKL.vdaxpy(n, da, dx, _dx_offset, incx, dy, _dy_offset, incy)
  }

  double dot(int n, double[] dx, int _dx_offset, int incx, double[] dy, int _dy_offset,
             int incy) {
    checkMKL();
    MKL.vddot(n, dx, _dx_offset, incx, dy, _dy_offset, incy)
  }

  double ger(int m, int n, double alpha, double[] x, int _x_offset, int incx, double[] y,
             int _y_offset, int incy, double[] a, int _a_offset, int lda) {
    checkMKL();
    MKL.vdger(m, n, alpha, x, _x_offset, incx, y, _y_offset,
        incy, a, _a_offset, lda)
  }

  void fill(double[] data, int fromIndex, int toIndex, double value) {
    Arrays.fill(data, fromIndex, toIndex, value);
  }

  void vPowx(int n, double[] a, int aOffset, double b, double[] y, int yOffset) {
    checkMKL();
    MKL.vdPowx(n, a, aOffset, b, y, yOffset)
  }

  void vLn(int n, double[] a, int aOffset, double[] y, int yOffset) {

    checkMKL();
    MKL.vdLn(n, a, aOffset, y, yOffset)

  }

  void vExp(int n, double[] a, int aOffset, double[] y, int yOffset) {
    checkMKL();
    MKL.vdExp(n, a, aOffset, y, yOffset)

  }

  void vSqrt(int n, double[] a, int aOffset, double[] y, int yOffset) {
    checkMKL();
    MKL.vdSqrt(n, a, aOffset, y, yOffset)

  }

  void vTanh(int n, double[] a, int aOffset, double[] y, int yOffset) {
    checkMKL();
    MKL.vdTanh(n, a, aOffset, y, yOffset)

  }

  void vAbs(int n, double[] a, int aOffset, double[] y, int yOffset) {
    checkMKL();
    MKL.vdAbs(n, a, aOffset, y, yOffset)

  }

  void vLog1p(int n, double[] a, int aOffset, double[] y, int yOffset) {
    checkMKL();
    MKL.vdLog1p(n, a, aOffset, y, yOffset)

  }

  void scal(int n, double sa, double[] sx, int offset, int incx) {
    checkMKL();
    MKL.vdscal(n, sa, sx, offset, incx)

  }

  double inv(double v) {
    return 1 / v;
  }

  double erf(double v) {
    return org.apache.commons.math3.special.Erf.erf(v);
  }

  double erfc(double v) {
    return org.apache.commons.math3.special.Erf.erfc(v);
  }

  double logGamma(double v) {
    return org.apache.commons.math3.special.Gamma.logGamma(v);
  }

  double digamma(double v) {
    return org.apache.commons.math3.special.Gamma.digamma(v);
  }

  void add(int n, double[] a, int offset, double v, int stride) {
    int i = 0;
    while (i < n) {
      a[offset + i * stride] += v;
      i += 1;
    }
  }

  void sub(int n, double[] a, int offset, double v, int stride) {
    int i = 0;
    while (i < n) {
      a[offset + i * stride] -= v;
      i += 1;
    }
  }

  void vAdd(int n, double[] a, int aOffset, double[] b, int bOffset, double[] y, int yOffset) {
    MKL.vdAdd(n, a, aOffset, b, bOffset, y, yOffset)

  }

  void vSub(int n, double[] a, int aOffset, double[] b, int bOffset, double[] y, int yOffset) {
    MKL.vdSub(n, a, aOffset, b, bOffset, y, yOffset)

  }

  void vMul(int n, double[] a, int aOffset, double[] b, int bOffset, double[] y, int yOffset) {
    if (checkMKL()) {
      MKL.vdMul(n, a, aOffset, b, bOffset, y, yOffset);
    } else {
      int i = 0;
      while (i < n) {
        y[yOffset + i] = a[aOffset + i] * b[bOffset + i];
        i += 1;
      }
    }
  }

  void vDiv(int n, double[] a, int aOffset, double[] b, int bOffset, double[] y, int yOffset) {
    if (checkMKL()) {
      MKL.vdDiv(n, a, aOffset, b, bOffset, y, yOffset);
    } else {
      int i = 0;
      while (i < n) {
        y[yOffset + i] = a[aOffset + i] / b[bOffset + i];
        i += 1;
      }
    }
  }

  double sum(int n, double[] a, int aOffset, int stride) {
    int i = 0;
    double r = 0.0;
    while (i < n) {
      r += a[aOffset + i * stride];
      i += 1;
    }
    return r;
  }

  double prod(int n, double[] a, int aOffset, int stride) {
    int i = 0;
    double r = 1.0;
    while (i < n) {
      r *= a[aOffset + i * stride];
      i += 1;
    }
    return r;
  }

  void arraycopy(double[] src, int srcPos, double[] dest, int destPos, int length) {
    System.arraycopy(src, srcPos, dest, destPos, length);
  }

  void addcmul(double value, int n,
               double[] self, int selfOffset,
               double[] a, int aOffset,
               double[] b, int bOffset) {
    double v = value;
    int i = 0;
    while (i < n) {
      self[i + selfOffset] += a[aOffset + i] * b[bOffset + i] * v;
      i += 1;
    }
  }

  void addcdiv(double value, int n,
               double[] self, int selfOffset,
               double[] a, int aOffset,
               double[] b, int bOffset) {
    double v = value;
    int i = 0;

    while (i < n) {
      self[i + selfOffset] += a[aOffset + i] / b[bOffset + i] * v;
      i += 1;
    }
  }

  boolean nearlyEqual(double a, double b, double epsilon) {
    double absA = Math.abs(a);
    double absB = Math.abs(b);
    double diff = Math.abs(a - b);

    boolean result = false;
    if (a == b) {
      result = true;
    } else if (a == 0 || b == 0 || diff < Double.MIN_NORMAL) {
      result = diff < (epsilon * Double.MIN_NORMAL);
    } else {
      result = diff / (absA + absB) < epsilon;
    }
    return result;
  }

  double floor(double a) {
    return Math.floor(a);
  }

  double ceil(double a) {
    return Math.ceil(a);
  }

  boolean isFinite(double a) {
    return Double.isFinite(a);
  }

  boolean isNan(double a) {
    return Double.isNaN(a);
  }

  boolean isInf(double a) {
    return Double.isInfinite(a);
  }

  double round(double a) {
    return Math.round(a);
  }

  double truncate(double a) {
    if (a >= 0) {
      return Math.floor(a);
    } else if (a == Math.floor(a)) {
      return a;
    } else {
      return Math.floor(a) + 1;
    }
  }

  double floorDiv(double a, double b) {
    return Math.floor(a / b);
  }

  double clip(double a, double lower, double upper) {
    if (lower <= upper) {
      throw new IllegalStateException("lower bound must be less or equal than upper bound");
    }
    return Math.min(Math.max(a, lower), upper);
  }


  // Float operations
  float plus(float x, float y) {
    return x + y;
  }

  float minus(float x, float y) {
    return x - y;
  }

  float times(float x, float y) {
    return x * y;
  }

  float divide(float x, float y) {
    return x / y;
  }

  float exp(float x) {
    return exp(x);
  }

  float log(float x) {
    return log(x);
  }

  float max(float x, float y) {
    return Math.max(x, y);
  }

  float min(float x, float y) {
    return Math.min(x, y);
  }

  float sqrt(float x) {
    return (float)Math.sqrt(x);
  }

  float tanh(float x) {
    return (float)Math.tanh(x);
  }

  float abs(float x) {
    return Math.abs(x);
  }

  float or(float x, float y) {
    throw new UnsupportedOperationException("Operation not supported");
  }

  float and(float x, float y) {
    throw new UnsupportedOperationException("Operation not supported");
  }

  float negative(float x) {
    return -x;
  }

  float pow(float x) {
    return (float)Math.pow(x, -1);
  }

  float pow(float x, float y) {
    return (float)Math.pow(x, y);
  }

  float log1p(float x) {
    return (float)Math.log1p(x);
  }

  boolean isGreater(float x, float y) {
    return x > y;
  }

  boolean isGreaterEq(float x, float y) {
    return x >= y;
  }

  float gemm(char transa, char transb, int m, int n, int k, float alpha, float[] a,
             int aOffset, int lda, float[] b, int bOffset, int ldb,
             float beta, float[] c, int cOffset, int ldc) {
    checkMKL();
    MKL.vsgemm(transa, transb, m, n, k, alpha, a, aOffset, lda, b,
        bOffset, ldb, beta, c, cOffset, ldc);
  }

  float gemv(char trans, int m, int n, float alpha, float[] a, int aoffset, int lda,
             float[] x, int xOffset, int incx, float beta, float[] y, int yOffset, int incy) {
    checkMKL();

    MKL.vsgemv(trans, m, n, alpha, a, aoffset, lda, x, xOffset,
        incx, beta, y, yOffset, incy);
  }

  float axpy(int n, float da, float[] dx, int _dx_offset, int incx, float[] dy,
             int _dy_offset, int incy) {
    checkMKL();

    MKL.vsaxpy(n, da, dx, _dx_offset, incx, dy, _dy_offset, incy);
  }

  float dot(int n, float[] dx, int _dx_offset, int incx, float[] dy, int _dy_offset,
            int incy) {
    checkMKL();
    MKL.vsdot(n, dx, _dx_offset, incx, dy, _dy_offset, incy);
  }

  float ger(int m, int n, float alpha, float[] x, int _x_offset, int incx, float[] y,
            int _y_offset, int incy, float[] a, int _a_offset, int lda) {
    checkMKL();
    MKL.vsger(m, n, alpha, x, _x_offset, incx, y, _y_offset,
        incy, a, _a_offset, lda);
  }

  void fill(float[] data, int fromIndex, int toIndex, float value) {
    Arrays.fill(data, fromIndex, toIndex, value);
  }

  void vPowx(int n, float[] a, int aOffset, float b, float[] y, int yOffset) {
    checkMKL();
    MKL.vsPowx(n, a, aOffset, b, y, yOffset);
  }

  void vLn(int n, float[] a, int aOffset, float[] y, int yOffset) {

    checkMKL();
    MKL.vsLn(n, a, aOffset, y, yOffset);

  }

  void vExp(int n, float[] a, int aOffset, float[] y, int yOffset) {
    checkMKL();
    MKL.vsExp(n, a, aOffset, y, yOffset);

  }

  void vSqrt(int n, float[] a, int aOffset, float[] y, int yOffset) {
    checkMKL();
    MKL.vsSqrt(n, a, aOffset, y, yOffset);

  }

  void vTanh(int n, float[] a, int aOffset, float[] y, int yOffset) {
    checkMKL();
    MKL.vsTanh(n, a, aOffset, y, yOffset);

  }

  void vAbs(int n, float[] a, int aOffset, float[] y, int yOffset) {
    checkMKL();
    MKL.vsAbs(n, a, aOffset, y, yOffset);

  }

  void vLog1p(int n, float[] a, int aOffset, float[] y, int yOffset) {
    checkMKL();
    MKL.vsLog1p(n, a, aOffset, y, yOffset);

  }

  void scal(int n, float sa, float[] sx, int offset, int incx) {
    checkMKL();
    MKL.vsscal(n, sa, sx, offset, incx);

  }

  float inv(float v) {
    return 1 / v;
  }

  float erf(float v) {
    return org.apache.commons.math3.special.Erf.erf(v);
  }

  float erfc(float v) {
    return org.apache.commons.math3.special.Erf.erfc(v);
  }

  float logGamma(float v) {
    return org.apache.commons.math3.special.Gamma.logGamma(v);
  }

  float digamma(float v) {
    return org.apache.commons.math3.special.Gamma.digamma(v);
  }

  void add(int n, float[] a, int offset, float v, int stride) {
    int i = 0;
    while (i < n) {
      a[offset + i * stride] += v;
      i += 1;
    }
  }

  void sub(int n, float[] a, int offset, float v, int stride) {
    int i = 0;
    while (i < n) {
      a[offset + i * stride] -= v;
      i += 1;
    }
  }

  void vAdd(int n, float[] a, int aOffset, float[] b, int bOffset, float[] y, int yOffset) {
    MKL.vsAdd(n, a, aOffset, b, bOffset, y, yOffset);

  }

  void vSub(int n, float[] a, int aOffset, float[] b, int bOffset, float[] y, int yOffset) {
    MKL.vsSub(n, a, aOffset, b, bOffset, y, yOffset)

  }

  void vMul(int n, float[] a, int aOffset, float[] b, int bOffset, float[] y, int yOffset) {
    if (checkMKL()) {
      MKL.vsMul(n, a, aOffset, b, bOffset, y, yOffset);
    } else {
      int i = 0;
      while (i < n) {
        y[yOffset + i] = a[aOffset + i] * b[bOffset + i];
        i += 1;
      }
    }
  }

  void vDiv(int n, float[] a, int aOffset, float[] b, int bOffset, float[] y, int yOffset) {
    if (checkMKL()) {
      MKL.vsDiv(n, a, aOffset, b, bOffset, y, yOffset);
    } else {
      int i = 0;
      while (i < n) {
        y[yOffset + i] = a[aOffset + i] / b[bOffset + i];
        i += 1;
      }
    }
  }

  float sum(int n, float[] a, int aOffset, int stride) {
    int i = 0;
    float r = 0.0f;
    while (i < n) {
      r += a[aOffset + i * stride];
      i += 1;
    }
    return r;
  }

  float prod(int n, float[] a, int aOffset, int stride) {
    int i = 0;
    float r = 1.0f;
    while (i < n) {
      r *= a[aOffset + i * stride];
      i += 1;
    }
    return r;
  }

  void arraycopy(float[] src, int srcPos, float[] dest, int destPos, int length) {
    System.arraycopy(src, srcPos, dest, destPos, length);
  }

  void addcmul(float value, int n,
               float[] self, int selfOffset,
               float[] a, int aOffset,
               float[] b, int bOffset) {
    float v = value;
    int i = 0;
    while (i < n) {
      self[i + selfOffset] += a[aOffset + i] * b[bOffset + i] * v;
      i += 1;
    }
  }

  void addcdiv(float value, int n,
               float[] self, int selfOffset,
               float[] a, int aOffset,
               float[] b, int bOffset) {
    float v = value;
    int i = 0;

    while (i < n) {
      self[i + selfOffset] += a[aOffset + i] / b[bOffset + i] * v;
      i += 1;
    }
  }

  boolean nearlyEqual(float a, float b, float epsilon) {
    float absA = Math.abs(a);
    float absB = Math.abs(b);
    float diff = Math.abs(a - b);

    boolean result = false;
    if (a == b) {
      result = true;
    } else if (a == 0 || b == 0 || diff < Double.MIN_NORMAL) {
      result = diff < (epsilon * Double.MIN_NORMAL);
    } else {
      result = diff / (absA + absB) < epsilon;
    }
    return result;
  }

  float floor(float a) {
    return (float)Math.floor(a);
  }

  float ceil(float a) {
    return (float)Math.ceil(a);
  }

  boolean isFinite(float a) {
    return Double.isFinite(a);
  }

  boolean isNan(float a) {
    return Double.isNaN(a);
  }

  boolean isInf(float a) {
    return Double.isInfinite(a);
  }

  float round(float a) {
    return Math.round(a);
  }

  float truncate(float a) {
    if (a >= 0) {
      return (float)Math.floor(a);
    } else if (a == Math.floor(a)) {
      return a;
    } else {
      return (float)Math.floor(a) + 1;
    }
  }

  float floorDiv(float a, float b) {
    return (float)Math.floor(a / b);
  }

  float clip(float a, float lower, float upper) {
    if (lower <= upper) {
      throw new IllegalStateException("lower bound must be less or equal than upper bound");
    }
    return Math.min(Math.max(a, lower), upper);
  }
  /**
   * If MKL is not set throw exception
   */
  private boolean checkMKL() {
    if (!MKL.isMKLLoaded) {
      throw new IllegalStateException("mkl isn't loaded");
    }

    return true;
  }
}


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
package edu.iu.dsc.tws.dl.data.tensor;

import edu.iu.dsc.tws.dl.data.*;
import edu.iu.dsc.tws.dl.data.storage.ArrayStorage;

public class DenseTensor<T> implements Tensor<T>, TensorMath<T> {

  private ArrayStorage<T> storage;
  private int storageOffset;
  private int[] size;
  private int[] stride;
  private int nDimension;

  public DenseTensor(ArrayStorage<T> storage, int storageOffset, int[] size, int[] stride, int nDimension) {
    this.storage = storage;
    this.storageOffset = storageOffset;
    this.size = size;
    this.stride = stride;
    this.nDimension = nDimension;
  }

  public DenseTensor(ArrayStorage<T> storage, int storageOffset, int[] size, int[] stride) {
    this.storage = storage;
    this.storageOffset = storageOffset;
    this.size = size;
    this.stride = stride;
  }

  @Override
  public <D> Tensor toTensor(TensorNumeric<D> ev) {
    return null;
  }

  @Override
  public Table toTable() {
    return null;
  }

  @Override
  public boolean isEmpty() {
    return this.storage() == null || this.storage().length() == 0;
  }

  @Override
  public boolean isScalar() {
    return !this.isEmpty() && this.nDimension() == 0;
  }

  @Override
  public int nDimension() {
    return this.nDimension;
  }

  @Override
  public int dim() {
    return this.nDimension;
  }

  @Override
  public int[] size() {
    return new int[0];
  }

  @Override
  public int size(int dim) {
    return 0;
  }

  @Override
  public int[] stride() {
    return new int[0];
  }

  @Override
  public int[] stride(int dim) {
    return new int[0];
  }

  @Override
  public Tensor<T> fill(T v) {
    return null;
  }

  @Override
  public Tensor<T> forceFill(T v) {
    return null;
  }

  @Override
  public Tensor<T> zero() {
    return null;
  }

  @Override
  public Tensor<T> randn() {
    return null;
  }

  @Override
  public Tensor<T> randn(Double mean, Double stdv) {
    return null;
  }

  @Override
  public Tensor<T> rand() {
    return null;
  }

  @Override
  public Tensor<T> rand(Double lowerBound, Double upperBound) {
    return null;
  }

  @Override
  public Tensor<T> bernoulli(double p) {
    return null;
  }

  @Override
  public Tensor<T> transpose(int dim1, int dim2) {
    return null;
  }

  @Override
  public Tensor<T> t() {
    return null;
  }

  @Override
  public Tensor<T> apply(int index) {
    return null;
  }

  @Override
  public T apply(int[] indexes) {
    return null;
  }

  @Override
  public T value() {
    return null;
  }

  @Override
  public T valueAt(int d1) {
    return null;
  }

  @Override
  public T valueAt(int d1, int d2) {
    return null;
  }

  @Override
  public T valueAt(int d1, int d2, int d3) {
    return null;
  }

  @Override
  public T valueAt(int d1, int d2, int d3, int d4) {
    return null;
  }

  @Override
  public T valueAt(int d1, int d2, int d3, int d4, int d5) {
    return null;
  }

  @Override
  public Tensor<T> apply(Table t) {
    return null;
  }

  @Override
  public void update(int index, T value) {

  }

  @Override
  public void update(int index, Tensor<T> src) {

  }

  @Override
  public void update(int[] indexes, T value) {

  }

  @Override
  public Tensor<T> setValue(T value) {
    return null;
  }

  @Override
  public Tensor<T> setValue(int d1, T value) {
    return null;
  }

  @Override
  public Tensor<T> setValue(int d1, int d2, T value) {
    return null;
  }

  @Override
  public Tensor<T> setValue(int d1, int d2, int d3, T value) {
    return null;
  }

  @Override
  public Tensor<T> setValue(int d1, int d2, int d3, int d4, T value) {
    return null;
  }

  @Override
  public Tensor<T> setValue(int d1, int d2, int d3, int d4, int d5, T value) {
    return null;
  }

  @Override
  public void update(Table t, T value) {

  }

  @Override
  public void update(Table t, Tensor<T> src) {

  }

  @Override
  public boolean isContiguous() {
    return false;
  }

  @Override
  public Tensor<T> contiguous() {
    return null;
  }

  @Override
  public boolean isSameSizeAs(Tensor<?> other) {
    return false;
  }

  @Override
  public Tensor<T> emptyInstance() {
    return null;
  }

  @Override
  public Tensor<T> resizeAs(Tensor<?> src) {
    return null;
  }

  @Override
  public Tensor<T> resize(int sizes, int[] strides) {
    return null;
  }

  @Override
  public Tensor<T> resize(int size1) {
    return null;
  }

  @Override
  public Tensor<T> resize(int size1, int size2) {
    return null;
  }

  @Override
  public Tensor<T> resize(int size1, int size2, int size3) {
    return null;
  }

  @Override
  public Tensor<T> resize(int size1, int size2, int size3, int size4) {
    return null;
  }

  @Override
  public Tensor<T> resize(int size1, int size2, int size3, int size4, int size5) {
    return null;
  }

  @Override
  public int nElement() {
    if (this.isEmpty()) {
      return 0;
    } else {
      int n = 1;
      int d = 0;
      while (d < this.nDimension) {
        n = n * this.size[d];
        d += 1;
      }
     return n;
    }
  }

  @Override
  public Tensor<T> select(int dim, int index) {
    return null;
  }

  @Override
  public Storage<T> storage() {
    return null;
  }

  @Override
  public int storageOffset() {
    return 0;
  }

  @Override
  public Tensor<T> narrow(int dim, int index, int size) {
    return null;
  }

  @Override
  public Tensor<T> copy(Tensor<T> other) {
    return null;
  }

  @Override
  public Tensor<T> squeeze() {
    int ndim = 0;
    int d = 0;
    while (d < this.nDimension) {
      if (this.size[d] != 1) {
        if (d != ndim) {
          this.size[ndim] = this.size[d];
          this.stride[ndim] = this.stride[d];
        }
        ndim += 1;
      }
      d += 1;
    }

    if (ndim == 0 && this.nDimension > 0) {
      this.size[0] = 1;
      this.stride[0] = 1;
      ndim = 1;
    }

    this.nDimension = ndim;
    return  this;
  }

  @Override
  public Tensor<T> squeeze(int dim) {
    if(dim >= 0 && dim < this.nDimension){
     throw new IllegalArgumentException("dimension out of range");
    }
    if (this.size(dim) == 1 && this.nDimension > 1) {
      int d = dim;
      while (d < this.nDimension - 1) {
        this.size[d] = this.size[d + 1];
        this.stride[d] = this.stride[d + 1];
        d += 1;
      }

      this.nDimension -= 1;
    }
    return this;
  }

  @Override
  public Tensor<T> squeezeNewTensor() {
    DenseTensor<T> result = new DenseTensor(this.storage, this.storageOffset(), this.size, this.stride);
    return result.squeeze();
  }

  @Override
  public Tensor<T> view(int[] sizes) {
    return null;
  }

  @Override
  public Tensor<T> unfold(int dim, int size, int step) {
    return null;
  }

  @Override
  public Tensor<T> repeatTensor(int[] sizes) {
    return null;
  }

  @Override
  public Tensor<T> expandAs(Tensor<T> template) {
    return null;
  }

  @Override
  public Tensor<T> expand(int[] sizes) {
    return null;
  }

  @Override
  public Tensor<T>[] split(int size, int dim) {
    return new Tensor[0];
  }

  @Override
  public Tensor<T>[] split(int dim) {
    return new Tensor[0];
  }

  @Override
  public boolean diff(Tensor<T> other, int count, boolean reverse) {
    return false;
  }

  @Override
  public Tensor<T> addSingletonDimension(Tensor<T> t, int dim) {
    return null;
  }

  @Override
  public Tensor<T> addMultiDimension(Tensor<T> t, int[] dim) {
    return null;
  }

  @Override
  public Tensor<T> reshape(int sizes) {
    return null;
  }

  @Override
  public Tensor<T> save(String path, boolean overWrite) {
    return null;
  }

  @Override
  public TensorNumeric<T> getTensorNumeric() {
    return null;
  }

  @Override
  public T[] toArray() {
    return null;
  }

  @Override
  public Tensor<T> addCopy(T s) {
    return null;
  }

  @Override
  public Tensor<T> addCopy(Tensor<T> t) {
    return null;
  }

  @Override
  public Tensor<T> subCopy(T s) {
    return null;
  }

  @Override
  public Tensor<T> subCopy(Tensor<T> t) {
    return null;
  }

  @Override
  public Tensor<T> divCopy(T s) {
    return null;
  }

  @Override
  public Tensor<T> divCopy(Tensor<T> t) {
    return null;
  }

  @Override
  public Tensor<T> mulCopy(T s) {
    return null;
  }

  @Override
  public Tensor<T> mulCopy(Tensor<T> t) {
    return null;
  }

  @Override
  public T sum() {
    return null;
  }

  @Override
  public T prod() {
    return null;
  }

  @Override
  public Tensor<T> prod(Tensor<T> x, int dim) {
    return null;
  }

  @Override
  public Tensor<T> sum(int dim) {
    return null;
  }

  @Override
  public Tensor<T> sum(Tensor<T> x, int dim) {
    return null;
  }

  @Override
  public T mean() {
    return null;
  }

  @Override
  public Tensor<T> mean(int dim) {
    return null;
  }

  @Override
  public T max() {
    return null;
  }

  @Override
  public TensorPair<T, T> max(int dim) {
    return null;
  }

  @Override
  public TensorPair<T, T> max(Tensor<T> values, Tensor<T> indices, int dim) {
    return null;
  }

  @Override
  public T min() {
    return null;
  }

  @Override
  public TensorPair<T, T> min(int dim) {
    return null;
  }

  @Override
  public TensorPair<T, T> min(Tensor<T> values, Tensor<T> indices, int dim) {
    return null;
  }

  @Override
  public Tensor<T> scatter(int dim, Tensor<T> index, Tensor<T> src) {
    return null;
  }

  @Override
  public Tensor<T> gather(int dim, Tensor<T> index, Tensor<T> src) {
    return null;
  }

  @Override
  public Tensor<T> conv2(Tensor<T> kernel, char vf) {
    return null;
  }

  @Override
  public Tensor<T> xcorr2(Tensor<T> kernel, char vf) {
    return null;
  }

  @Override
  public Tensor<T> sqrt() {
    return null;
  }

  @Override
  public Tensor<T> tanh() {
    return null;
  }

  @Override
  public Tensor<T> abs() {
    return null;
  }

  @Override
  public Tensor<T> add(T value, Tensor<T> y) {
    return null;
  }

  @Override
  public Tensor<T> add(Tensor<T> y) {
    return null;
  }

  @Override
  public Tensor<T> add(Tensor<T> x, T value, Tensor<T> y) {
    return null;
  }

  @Override
  public Tensor<T> add(T value) {
    return null;
  }

  @Override
  public Tensor<T> add(Tensor<T> x, Tensor<T> y) {
    return null;
  }

  @Override
  public T dot(Tensor<T> y) {
    return null;
  }

  @Override
  public Tensor<T> cmax(T value) {
    return null;
  }

  @Override
  public T dist(Tensor<T> y, int norm) {
    return null;
  }

  @Override
  public Tensor<T> addcmul(T value, Tensor<T> tensor1, Tensor<T> tensor2) {
    return null;
  }

  @Override
  public Tensor<T> addcmul(Tensor<T> tensor1, Tensor<T> tensor2) {
    return null;
  }

  @Override
  public Tensor<T> addcdiv(T value, Tensor<T> tensor1, Tensor<T> tensor2) {
    return null;
  }

  @Override
  public Tensor<T> sub(T value, Tensor<T> y) {
    return null;
  }

  @Override
  public Tensor<T> sub(Tensor<T> x, T value, Tensor<T> y) {
    return null;
  }

  @Override
  public Tensor<T> sub(Tensor<T> y) {
    return null;
  }

  @Override
  public Tensor<T> sub(Tensor<T> x, Tensor<T> y) {
    return null;
  }

  @Override
  public Tensor<T> sub(T value) {
    return null;
  }

  @Override
  public Tensor<T> cmul(Tensor<T> y) {
    return null;
  }

  @Override
  public Tensor<T> cmul(Tensor<T> x, Tensor<T> y) {
    return null;
  }

  @Override
  public Tensor<T> cdiv(Tensor<T> y) {
    return null;
  }

  @Override
  public Tensor<T> cdiv(Tensor<T> x, Tensor<T> y) {
    return null;
  }

  @Override
  public Tensor<T> mul(T value) {
    return null;
  }

  @Override
  public Tensor<T> div(T value) {
    return null;
  }

  @Override
  public Tensor<T> div(Tensor<T> y) {
    return null;
  }

  @Override
  public Tensor<T> mul(Tensor<T> x, T value) {
    return null;
  }

  @Override
  public Tensor<T> addmm(T v1, Tensor<T> m, T v2, Tensor<T> mat1, Tensor<T> mat2) {
    return null;
  }

  @Override
  public Tensor<T> addmm(Tensor<T> m, Tensor<T> mat1, Tensor<T> mat2) {
    return null;
  }

  @Override
  public Tensor<T> addmm(Tensor<T> mat1, Tensor<T> mat2) {
    return null;
  }

  @Override
  public Tensor<T> addmm(T v2, Tensor<T> mat1, Tensor<T> mat2) {
    return null;
  }

  @Override
  public Tensor<T> addmm(T v1, T v2, Tensor<T> mat1, Tensor<T> mat2) {
    return null;
  }

  @Override
  public Tensor<T> mm(Tensor<T> mat1, Tensor<T> mat2) {
    return null;
  }

  @Override
  public Tensor<T> addr(Tensor<T> t1, Tensor<T> t2) {
    return null;
  }

  @Override
  public Tensor<T> addr(T v1, Tensor<T> t1, Tensor<T> t2) {
    return null;
  }

  @Override
  public Tensor<T> addr(T v1, Tensor<T> t1, T v2, Tensor<T> t2) {
    return null;
  }

  @Override
  public Tensor<T> addr(T v1, Tensor<T> t1, T v2, Tensor<T> t2, Tensor<T> t3) {
    return null;
  }

  @Override
  public T uniform(T... args) {
    return null;
  }

  @Override
  public Tensor<T> addmv(T beta, Tensor<T> vec1, T alpha, Tensor<T> mat, Tensor<T> vec2) {
    return null;
  }

  @Override
  public Tensor<T> addmv(T beta, T alpha, Tensor<T> mat, Tensor<T> vec2) {
    return null;
  }

  @Override
  public Tensor<T> addmv(T alpha, Tensor<T> mat, Tensor<T> vec2) {
    return null;
  }

  @Override
  public Tensor<T> mv(Tensor<T> mat, Tensor<T> vec2) {
    return null;
  }

  @Override
  public Tensor<T> baddbmm(T beta, Tensor<T> m, T alpha, Tensor<T> batch1, Tensor<T> batch2) {
    return null;
  }

  @Override
  public Tensor<T> baddbmm(T beta, T alpha, Tensor<T> batch1, Tensor<T> batch2) {
    return null;
  }

  @Override
  public Tensor<T> baddbmm(T alpha, Tensor<T> batch1, Tensor<T> batch2) {
    return null;
  }

  @Override
  public Tensor<T> bmm(Tensor<T> batch1, Tensor<T> batch2) {
    return null;
  }

  @Override
  public Tensor<T> pow(Tensor<T> y, T n) {
    return null;
  }

  @Override
  public Tensor<T> pow(T n) {
    return null;
  }

  @Override
  public Tensor<T> square() {
    return null;
  }

  @Override
  public Tensor<T> floor(Tensor<T> y) {
    return null;
  }

  @Override
  public Tensor<T> floor() {
    return null;
  }

  @Override
  public Tensor<T> ceil() {
    return null;
  }

  @Override
  public Tensor<T> inv() {
    return null;
  }

  @Override
  public Tensor<T> erf() {
    return null;
  }

  @Override
  public Tensor<T> erfc() {
    return null;
  }

  @Override
  public Tensor<T> logGamma() {
    return null;
  }

  @Override
  public Tensor<T> digamma() {
    return null;
  }

  @Override
  public TensorPair<T, T> topk(int k, int dim, boolean increase,
                               Tensor<T> result, Tensor<T> indices, boolean sortedResult) {
    return null;
  }

  @Override
  public Tensor<T> log(Tensor<T> y) {
    return null;
  }

  @Override
  public Tensor<T> exp(Tensor<T> y) {
    return null;
  }

  @Override
  public Tensor<T> sqrt(Tensor<T> y) {
    return null;
  }

  @Override
  public Tensor<T> tanh(Tensor<T> y) {
    return null;
  }

  @Override
  public Tensor<T> log1p(Tensor<T> y) {
    return null;
  }

  @Override
  public Tensor<T> log() {
    return null;
  }

  @Override
  public Tensor<T> exp() {
    return null;
  }

  @Override
  public Tensor<T> log1p() {
    return null;
  }

  @Override
  public Tensor<T> abs(Tensor<T> x) {
    return null;
  }

  @Override
  public Tensor<T> norm(Tensor<T> y, int value, int dim) {
    return null;
  }

  @Override
  public Tensor<T> gt(Tensor<T> x, Tensor<T> y) {
    return null;
  }

  @Override
  public Tensor<T> lt(Tensor<T> x, Tensor<T> y) {
    return null;
  }

  @Override
  public Tensor<T> le(Tensor<T> x, Tensor<T> y) {
    return null;
  }

  @Override
  public Tensor<T> eq(Tensor<T> x, T y) {
    return null;
  }

  @Override
  public Tensor<T> maskedFill(Tensor<T> mask, T e) {
    return null;
  }

  @Override
  public Tensor<T> maskedCopy(Tensor<T> mask, Tensor<T> y) {
    return null;
  }

  @Override
  public Tensor<T> maskedSelect(Tensor<T> mask, Tensor<T> y) {
    return null;
  }

  @Override
  public T norm(int value) {
    return null;
  }

  @Override
  public Tensor<T> sign() {
    return null;
  }

  @Override
  public Tensor<T> ge(Tensor<T> x, double value) {
    return null;
  }

  @Override
  public Tensor<T> indexAdd(int dim, Tensor<T> index, Tensor<T> y) {
    return null;
  }

  @Override
  public Tensor<T> index(int dim, Tensor<T> index, Tensor<T> y) {
    return null;
  }

  @Override
  public Tensor<T> cmax(Tensor<T> y) {
    return null;
  }

  @Override
  public Tensor<T> cmin(Tensor<T> y) {
    return null;
  }

  @Override
  public Tensor<T> cmax(Tensor<T> x, Tensor<T> y) {
    return null;
  }

  @Override
  public Tensor<T> cmin(Tensor<T> x, Tensor<T> y) {
    return null;
  }

  @Override
  public Tensor<T> range(double xmin, double xmax, int step) {
    return null;
  }

  @Override
  public Tensor<T> negative(Tensor<T> x) {
    return null;
  }

  @Override
  public T sumSquare() {
    return null;
  }

  @Override
  public Tensor<T> clamp(double min, double max) {
    return null;
  }
}

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
import edu.iu.dsc.tws.dl.data.function.TensorFunc2;
import edu.iu.dsc.tws.dl.data.function.TensorFunc4;
import edu.iu.dsc.tws.dl.data.storage.ArrayDoubleStorage;
import edu.iu.dsc.tws.dl.utils.RandomGenerator;
import edu.iu.dsc.tws.dl.utils.Util;

import java.util.Arrays;

@SuppressWarnings({"ChainingConstructorIgnoresParameter", "NeedBraces", "LocalVariableName"})
public class DenseTensor implements Tensor, TensorMath {

  private ArrayDoubleStorage storageInternal;
  private int storageOffsetInternal;
  private int[] sizeInternal;
  private int[] strideInternal;
  private int nDimensionInternal;

  public DenseTensor() {
    this(null, 0, null, null, 0);
  }

  public DenseTensor(int dim1) {
    this(new ArrayDoubleStorage(dim1), 0, new int[]{dim1}, new int[]{1}, 1);
  }

  public DenseTensor(int dim1, int dim2) {
    this(new ArrayDoubleStorage(dim1 * dim2), 0, new int[]{dim1, dim2},
        new int[]{dim2, 1}, 2);
  }

  public DenseTensor(int dim1, int dim2, int dim3) {
    this(new ArrayDoubleStorage(dim1), 0, new int[]{dim1, dim2, dim3},
        new int[]{dim3 * dim2, dim3, 1}, 3);
  }

  public DenseTensor(ArrayDoubleStorage storage) {
    this.storageInternal = storage;
    initWithStorage(storage, 0, new int[storage.length()], new int[1]);
  }

  private DenseTensor(ArrayDoubleStorage storage, int storageOffset, int[] size,
                      int[] stride, int nDimension) {
    this.storageInternal = storage;
    this.storageOffsetInternal = storageOffset;
    this.sizeInternal = size;
    this.strideInternal = stride;
    this.nDimensionInternal = nDimension;
  }

  private DenseTensor(ArrayDoubleStorage storage, int storageOffset, int[] size, int[] stride) {
    this.storageInternal = storage;
    this.storageOffsetInternal = storageOffset;
    this.sizeInternal = size;
    this.strideInternal = stride;
  }

  public DenseTensor(ArrayDoubleStorage newStorage, int storageOffset, int[] size) {
    this(newStorage, 0, null, null, 0);
    if (newStorage != null) {
      int tempstorageOffset = storageOffset - 1;
      int[] tempSize = (size == null) ? new int[newStorage.length()] : size;
      int[] tempStride = (size == null) ? null : strideInternal;
      initWithStorage(newStorage, tempstorageOffset, tempSize, tempStride);
    }
  }

  public static boolean canFastBroadcast(DenseTensor tensor, DenseTensor other) {
    if (tensor.nDimension() < other.nDimension()) return false;

    int delta = tensor.nDimension() - other.nDimension();
    int d = other.nDimension();
    // Check dimensions
    boolean broadcasting = false;
    while (d > 0) {
      if (broadcasting) {
        if (other.size(d) != 1) return false;
      } else if (tensor.size(delta + d) != other.size(d)) {
        if (other.size(d) != 1) return false;
        broadcasting = true;
      }
      d -= 1;
    }

    return true;
  }

  private void initWithStorage(ArrayDoubleStorage newStorage, int newStorageOffset,
                               int[] newSize, int[] newStride) {
    if (newSize != null && newStride != null) {
      if (newSize.length == newStride.length) {
        throw new IllegalArgumentException("inconsistent size");
      }
    }
    this.storageInternal = newStorage;
    this.storageOffsetInternal = newStorageOffset;

    if (newSize != null) {
      this.nDimensionInternal = newSize.length;
    } else if (newStride != null) {
      this.nDimensionInternal = newStride.length;
    } else {
      this.nDimensionInternal = 0;
    }
    rawResize(this, nDimensionInternal, newSize, newStride);
  }

  @Override
  public Tensor toTensor(TensorNumeric ev) {
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
    return this.nDimensionInternal;
  }

  @Override
  public int dim() {
    return this.nDimensionInternal;
  }

  @Override
  public int[] size() {
    if (sizeInternal == null) {
      return null;
    } else {
      return Arrays.copyOfRange(sizeInternal, 0, nDimensionInternal);
    }
  }

  @Override
  public int size(int dim) {
    return sizeInternal[dim - 1];
  }

  @Override
  public int[] stride() {
    if (strideInternal == null) {
      return null;
    } else {
      return Arrays.copyOfRange(strideInternal, 0, nDimensionInternal);
    }
  }

  @Override
  public int stride(int dim) {
    return strideInternal[dim - 1];
  }

  @Override
  public Tensor fill(double v) {
    if (this.storage() == null) return this;

    if (this.isContiguous()) {
      this.storage().fill(v, this.storageOffset(), this.nElement());
    } else {
      throw new UnsupportedOperationException("non contiguous not supported");
    }
    return this;
  }

  @Override
  public Tensor forceFill(double v) {
    return this.fill(v);
  }

  @Override
  public Tensor zero() {
    return this.fill(0.0);
  }

  @Override
  public Tensor randn() {
    return this.randn(0.0, 1.0);
  }

  @Override
  public Tensor randn(Double mean, Double stdv) {
    if (this.isContiguous()) {
      int i = 0;
      int total = this.nElement();
      double[] data = this.storage().toDoubleArray();
      int offset = this.storageOffset() - 1;
      while (i < total) {
        data[offset + i] = RandomGenerator.RNG().normal(mean, stdv);
        i += 1;
      }
    } else {
      throw new UnsupportedOperationException("non contiguous not supported");
    }
    return this;
  }

  @Override
  public Tensor rand() {
    return this.rand(0.0, 1.0);
  }

  @Override
  public Tensor rand(Double lowerBound, Double upperBound) {
    if (this.isContiguous()) {
      int i = 0;
      int total = this.nElement();
      double[] data = this.storage().toDoubleArray();
      int offset = this.storageOffset() - 1;
      while (i < total) {
        data[offset + i] = RandomGenerator.RNG().uniform(lowerBound, upperBound);
        i += 1;
      }
    } else {
      throw new UnsupportedOperationException("non contiguous not supported");
    }
    return this;
  }

  @Override
  public Tensor bernoulli(double p) {
    throw new UnsupportedOperationException("operation not supported");
  }

  @Override
  public Tensor transpose(int dim1, int dim2) {
    DenseTensor result = newWithTensor(this);
    transpose(result, null, dim1 - 1, dim2 - 1);
    return result;
  }

  private void transpose(DenseTensor self, DenseTensor source, int dimension1, int dimension2) {
    DenseTensor src = source;
    if (src == null) src = self;
    Util.require(dimension1 >= 0 && dimension1 < src.nDimension(), "out of range");
    Util.require(dimension2 >= 0 && dimension2 < src.nDimension(), "out of range");

    set(self, src);
    if (dimension1 == dimension2) {
      return;
    }
    int z = self.strideInternal[dimension1];
    self.strideInternal[dimension1] = self.strideInternal[dimension2];
    self.strideInternal[dimension2] = z;
    z = self.sizeInternal[dimension1];
    self.sizeInternal[dimension1] = self.sizeInternal[dimension2];
    self.sizeInternal[dimension2] = z;
  }

  private DenseTensor set(DenseTensor self, DenseTensor other) {
    if (self != other) {
      return rawSet(self, other.storage(), other.storageOffset(),
          other.nDimension(), other.size(), other.stride());
    } else {
      return self;
    }
  }

  private DenseTensor newWithTensor(DenseTensor old) {
    DenseTensor newTensor = new DenseTensor();
    return newTensor.rawSet(newTensor, old.storage(), old.storageOffset(), old.nDimension(),
        old.size(), old.stride());
  }

  @Override
  public Tensor t() {
    Util.require(this.nDimension() == 2, "t() is only for 2D tensor");
    return transpose(1, 2);
  }

  @Override
  public Tensor apply(int index) {
    Util.require(this.nDimension() > 0, "empty or scalar tensor");
    int _index = index - 1;
    if (_index < 0) _index = this.sizeInternal[0] + _index + 1;
    Util.require(_index >= 0 && _index < this.sizeInternal[0],
        "out of range, ${_index}: 0 to ${this._size(0)}");

    DenseTensor result = newWithTensor(this);
    select(result, null, 0, _index);
    return result;
  }

  @Override
  public double apply(int[] indexes) {
    Util.require(indexes.length == this.nDimension(), "invalid size");
    int offset = this.storageOffsetInternal;
    int d = 0;
    while (d < indexes.length) {
      offset += getOffset(indexes[d] - 1, d + 1);
      d += 1;
    }
    return this.storageInternal.get(offset);
  }

  private int getOffset(int z, int dim) {
    int _z = z;
    if (_z < 0) {
      _z = this.size(dim) + _z + 1;
    }
    Util.require(_z >= 0 && _z < this.size(dim), "index out of bound");
    return _z * this.stride(dim);
  }

  @Override
  public double value() {
    Util.require(1 == this.nElement(), "invalid size: 1 == ${this.nElement()}");
    int offset = this.storageOffsetInternal;
    return this.storageInternal.get(offset);
  }

  @Override
  public double valueAt(int d1) {
    Util.require(1 == this.nDimension(), "invalid size: 1 == ${this.nDimension}");
    int offset = this.storageOffsetInternal;
    offset += getOffset(d1 - 1, 1);
    return this.storageInternal.get(offset);
  }

  @Override
  public double valueAt(int d1, int d2) {
    Util.require(2 == this.nDimension(), "invalid size");
    int offset = this.storageOffsetInternal;
    offset += getOffset(d1 - 1, 1);
    offset += getOffset(d2 - 1, 2);
    return this.storageInternal.get(offset);
  }

  @Override
  public double valueAt(int d1, int d2, int d3) {
    Util.require(3 == this.nDimension(), "invalid size");
    int offset = this.storageOffsetInternal;
    offset += getOffset(d1 - 1, 1);
    offset += getOffset(d2 - 1, 2);
    offset += getOffset(d3 - 1, 3);
    return this.storageInternal.get(offset);
  }

  @Override
  public double valueAt(int d1, int d2, int d3, int d4) {
    throw new UnsupportedOperationException("operation not supported");
  }

  @Override
  public double valueAt(int d1, int d2, int d3, int d4, int d5) {
    throw new UnsupportedOperationException("operation not supported");
  }

  @Override
  public Tensor apply(Table t) {
    throw new UnsupportedOperationException("operation not supported");
  }

  @Override
  public void update(int index, double value) {
    Util.require(this.nDimension() > 0, "empty tensor");
    int _index = index - 1;
    if (_index < 0) _index = this.sizeInternal[0] + _index + 1;
    Util.require(_index >= 0 && _index < this.sizeInternal[0], "out of range");
    if (this.nDimension() == 1) {
      this.storageInternal.update(this.storageOffsetInternal + _index * this.strideInternal[0],
          value);
    } else {
      DenseTensor tensor = newWithTensor(this);
      narrow(tensor, null, 0, _index, 1);
      tensor.fill(value);
    }
  }

  @Override
  public void update(int index, Tensor src) {
    Util.require(this.nDimension() > 0, "empty or scalar tensor");
    int _index = index - 1;
    if (_index < 0) _index = this.sizeInternal[0] + _index + 1;
    Util.require(_index >= 0 && _index < this.sizeInternal[0], "out of range");
    DenseTensor tensor = newWithTensor(this);
    narrow(tensor, null, 0, _index, 1);
    tensor.copy(src);
  }

  @Override
  public void update(int[] indexes, double value) {
    Util.require(indexes.length == this.nDimension(), "invalid size");
    int offset = this.storageOffsetInternal;
    int d = 0;
    while (d < indexes.length) {
      offset += getOffset(indexes[d] - 1, d + 1);
      d += 1;
    }
    this.storageInternal.update(offset, value);
  }

  @Override
  public Tensor setValue(double value) {
    Util.require(0 == this.nDimension(), "invalid size, you can only call this on a scalar");
    int offset = this.storageOffsetInternal;
    this.storageInternal.update(offset, value);
    return this;
  }

  @Override
  public Tensor setValue(int d1, double value) {
    Util.require(1 == this.nDimension(), "invalid size");
    int offset = this.storageOffsetInternal;
    offset += getOffset(d1 - 1, 1);
    this.storageInternal.update(offset, value);
    return this;
  }

  @Override
  public Tensor setValue(int d1, int d2, double value) {
    Util.require(2 == this.nDimension(), "invalid size");
    int offset = this.storageOffsetInternal;
    offset += getOffset(d1 - 1, 1);
    offset += getOffset(d2 - 1, 2);
    this.storageInternal.update(offset, value);
    return this;
  }

  @Override
  public Tensor setValue(int d1, int d2, int d3, double value) {
    Util.require(3 == this.nDimension(), "invalid size");
    int offset = this.storageOffsetInternal;
    offset += getOffset(d1 - 1, 1);
    offset += getOffset(d2 - 1, 2);
    offset += getOffset(d3 - 1, 3);
    this.storageInternal.update(offset, value);
    return this;
  }

  @Override
  public Tensor setValue(int d1, int d2, int d3, int d4, double value) {
    throw new UnsupportedOperationException("operation not supported");
  }

  @Override
  public Tensor setValue(int d1, int d2, int d3, int d4, int d5, double value) {
    throw new UnsupportedOperationException("operation not supported");
  }

  @Override
  public void update(Table t, double value) {
    throw new UnsupportedOperationException("operation not supported");
  }

  @Override
  public void update(Table t, Tensor src) {
    throw new UnsupportedOperationException("operation not supported");
  }

  @Override
  public boolean isContiguous() {
    int s = 1;
    int d = this.nDimension() - 1;
    while (d >= 0) {
      if (this.sizeInternal[d] != 1) {
        if (s != this.strideInternal[d]) {
          return false;
        } else {
          s = s * this.sizeInternal[d];
        }
      }
      d -= 1;
    }
    return true;
  }

  @Override
  public Tensor contiguous() {
    if (!this.isContiguous()) {
      return this.clone();
    } else {
      return this;
    }
  }


  @Override
  public Tensor clone() {
    DenseTensor tensor = new DenseTensor();
    resizeAs(tensor, this);
    copy(tensor, this);
    return tensor;
  }

  @Override
  public boolean isSameSizeAs(Tensor other) {
    return false;
  }

  @Override
  public Tensor emptyInstance() {
    return null;
  }

  @Override
  public Tensor resizeAs(Tensor src) {
    resizeAs(this, src);
    return this;
  }

  @Override
  public Tensor resize(int[] sizes, int[] strides) {
    return resize(this, sizes, strides);
  }

  @Override
  public Tensor resize(int[] sizes) {
    return resize(this, sizes, null);
  }

  @Override
  public Tensor resize(int size1) {
    if (this.nDimensionInternal != 1 || this.size(1) != size1) {
      return resize(this, new int[]{size1}, null);
    } else {
      return this;
    }
  }

  @Override
  public Tensor resize(int size1, int size2) {
    if (this.nDimensionInternal != 2 || this.size(1) != size1 || this.size(2) != size2) {
      return resize(this, new int[]{size1, size2}, null);
    } else {
      return this;
    }
  }

  @Override
  public Tensor resize(int size1, int size2, int size3) {
    if (this.nDimensionInternal != 3 || this.size(1) != size1 || this.size(2) != size2 ||
        this.size(3) != size3) {
      return resize(this, new int[]{size1, size2, size3}, null);
    } else {
      return this;
    }
  }

  @Override
  public Tensor resize(int size1, int size2, int size3, int size4) {
    return null;
  }

  @Override
  public Tensor resize(int size1, int size2, int size3, int size4, int size5) {
    return null;
  }

  @Override
  public int nElement() {
    if (this.isEmpty()) {
      return 0;
    } else {
      int n = 1;
      int d = 0;
      while (d < this.nDimensionInternal) {
        n = n * this.sizeInternal[d];
        d += 1;
      }
      return n;
    }
  }

  @Override
  public Tensor select(int dim, int index) {
    int _dimension = dim - 1;
    int _sliceIndex = index - 1;

    Util.require(this.nDimension() > 0, "empty or scalar tensor cannot be selected");
    DenseTensor result = newWithTensor(this);
    select(result, null, _dimension, _sliceIndex);
    return result;
  }

  private void select(DenseTensor self, DenseTensor source, int dimension, int sliceIndex) {
    DenseTensor src = source;
    if (src == null) src = source;
    Util.require(src.nDimension() > 0, "cannot select on a scalar");
    Util.require(dimension >= 0 && dimension < src.nDimension(), "out of range");
    Util.require(sliceIndex >= 0 && sliceIndex < src.size(dimension + 1),
        "{_sliceIndex} out of range 0 to ${src.size(_dimension + 1) - 1}");

    set(self, src);
    narrow(self, null, dimension, sliceIndex, 1);

    int d = dimension;
    while (d < self.nDimension() - 1) {
      self.sizeInternal[d] = self.sizeInternal[d + 1];
      self.strideInternal[d] = self.strideInternal[d + 1];
      d += 1;
    }

    self.nDimensionInternal = self.nDimension() - 1;
  }

  private void narrow(DenseTensor self, DenseTensor source, int dimension, int firstIndex, int size) {
    DenseTensor src = source;
    if (src == null) {
      src = self;
    }

    Util.require(dimension >= 0 && dimension < src.nDimension(), "dimension out of range");
    Util.require(firstIndex >= 0 && firstIndex < src.size(dimension + 1),
        "firstIndex(${_firstIndex}) out of range [0, ${src.size(_dimension + 1)})");
    Util.require(size > 0 && firstIndex + size <= src.size(dimension + 1),
        "size out of range $size (0, ${src.size(_dimension + 1)} - ${_firstIndex}]");

    set(self, src);

    if (firstIndex > 0) {
      self.storageOffsetInternal = self.storageOffsetInternal
          + firstIndex * self.strideInternal[dimension];
    }
    self.sizeInternal[dimension] = size;
  }

  @Override
  public Tensor set(Tensor other) {
    this.storageInternal = (ArrayDoubleStorage) other.storage();
    this.storageOffsetInternal = other.storageOffset();
    return rawResize(this, other.nDimension(), other.size(), other.stride());
  }

  @Override
  public Tensor set(Storage storage, int storageOffset, int[] sizes, int[] strides) {
    if (sizes != null && strides != null) {
      Util.require(sizes.length == strides.length, "Invalid Size");
    }
    this.storageInternal = (ArrayDoubleStorage) storage;
    this.storageOffsetInternal = storageOffset;
    if (sizes == null) {
      return rawResize(this, 0, sizes, strides);
    } else {
      return rawResize(this, sizes.length, sizes, strides);
    }
  }

  @Override
  public Tensor set() {
    if (this.storageInternal != null) {
      this.storageInternal.resize(0);
    }
    this.nDimensionInternal = 0;
    this.sizeInternal = new int[0];
    return this;
  }

  @Override
  public ArrayDoubleStorage storage() {
    return storageInternal;
  }

  @Override
  public int storageOffset() {
    return this.storageOffsetInternal + 1;
  }

  @Override
  public Tensor narrow(int dim, int index, int size) {
    DenseTensor result = newWithTensor(this);
    narrow(result, null, dim - 1, index - 1, size);
    return result;
  }

  @Override
  public Tensor copy(Tensor other) {
    copy(this, other);
    return this;
  }

  private void copy(DenseTensor self, Tensor src) {
    Util.require(self.nElement() == src.nElement(), "self element"
        + " number(${self.nElement()}) is not"
        + " equal to source element number(${src.nElement()})");

    if (self.isEmpty()) {
      return;
    }
    if (self.isContiguous() && src.isContiguous() && sameStride(self.stride(), src.stride())) {
      System.arraycopy(src.storage().toDoubleArray(), src.storageOffset() - 1,
          self.storage().toDoubleArray(), self.storageOffset() - 1, self.nElement());
      return;
    } else {
      throw new UnsupportedOperationException("non Contiguous not supported");
    }
  }

  private boolean sameStride(int[] l, int[] r) {
    if (l.length != r.length) {
      return false;
    }

    int i = 0;
    while (i < l.length) {
      if (l[i] != r[i]) {
        return false;
      }
      i += 1;
    }
    return true;
  }

  @Override
  public Tensor squeeze() {
    int ndim = 0;
    int d = 0;
    while (d < this.nDimensionInternal) {
      if (this.sizeInternal[d] != 1) {
        if (d != ndim) {
          this.sizeInternal[ndim] = this.sizeInternal[d];
          this.strideInternal[ndim] = this.strideInternal[d];
        }
        ndim += 1;
      }
      d += 1;
    }

    if (ndim == 0 && this.nDimensionInternal > 0) {
      this.sizeInternal[0] = 1;
      this.strideInternal[0] = 1;
      ndim = 1;
    }

    this.nDimensionInternal = ndim;
    return this;
  }

  @Override
  public Tensor squeeze(int dim) {
    if (dim >= 0 && dim < this.nDimensionInternal) {
      throw new IllegalArgumentException("dimension out of range");
    }
    if (this.size(dim) == 1 && this.nDimensionInternal > 1) {
      int d = dim;
      while (d < this.nDimensionInternal - 1) {
        this.sizeInternal[d] = this.sizeInternal[d + 1];
        this.strideInternal[d] = this.strideInternal[d + 1];
        d += 1;
      }

      this.nDimensionInternal -= 1;
    }
    return this;
  }

  @Override
  public Tensor squeezeNewTensor() {
    DenseTensor result = new DenseTensor(this.storageInternal, this.storageOffset(), this.sizeInternal, this.strideInternal);
    return result.squeeze();
  }

  @Override
  public Tensor view(int[] sizes) {
    if (!this.isContiguous()) {
      throw new IllegalStateException("current tensor is not contiguous");
    }
    if (TensorNumeric.product(sizes) != this.nElement()) {
      throw new IllegalStateException("invalid size eElement");
    }
    return new DenseTensor(this.storageInternal, this.storageOffset(), sizes.clone());
  }

  @Override
  public Tensor unfold(int dim, int size, int step) {
    Util.require(this.nDimensionInternal > 0, "cannot unfold an empty tensor");
    Util.require(dim > 0 && dim <= this.nDimensionInternal, "out of range");
    Util.require(size <= this.size(dim), "out of range");
    Util.require(step > 0, "invalid step");

    int[] newSize = new int[this.nDimensionInternal + 1];
    int[] newStride = new int[this.nDimensionInternal + 1];

    newSize[this.nDimensionInternal] = size;
    newStride[this.nDimensionInternal] = this.stride(dim);

    int d = 0;
    while (d < this.nDimensionInternal) {
      if (d + 1 == dim) {
        newSize[d] = (this.size(d + 1) - size) / step + 1;
        newStride[d] = step * this.stride(d + 1);
      } else {
        newSize[d] = this.size(d + 1);
        newStride[d] = this.stride(d + 1);
      }
      d = d + 1;
    }

    return new DenseTensor(this.storageInternal, this.storageOffsetInternal, newSize,
        newStride, this.dim() + 1);
  }

  @Override
  public Tensor repeatTensor(int[] sizes) {
    return null;
  }

  @Override
  public Tensor expandAs(Tensor template) {
    return this.expand(template.size());
  }

  @Override
  public Tensor expand(int[] sizes) {
    Util.require(sizes.length == this.dim(),
        "the number of dimensions provided must equal ${this.dim()}");
    int tensorDim = this.dim();
    int[] tensorStride = this.stride();
    int[] tensorSize = this.size();

    int i = 0;
    while (i < tensorDim) {
      if (tensorSize[i] == 1) {
        tensorSize[i] = sizes[i];
        tensorStride[i] = 0;
      } else if (tensorSize[i] != sizes[i]) {
        throw new UnsupportedOperationException(
            "incorrect size: only supporting singleton expansion (size=1)");
      }
      i += 1;
    }

    return set(this.storage(), this.storageOffset(), tensorSize, tensorStride);
  }

  @Override
  public Tensor[] split(int size, int dim) {
    return new Tensor[0];
  }

  @Override
  public Tensor[] split(int dim) {
    return new Tensor[0];
  }

  @Override
  public boolean diff(Tensor other, int count, boolean reverse) {
    return false;
  }

  @Override
  public Tensor addSingletonDimension(Tensor t, int dim) {
    return null;
  }

  @Override
  public Tensor addMultiDimension(Tensor t, int[] dim) {
    return null;
  }

  @Override
  public Tensor reshape(int[] sizes) {
    Util.require(TensorNumeric.product(sizes) == this.nElement(),
        "DenseTensor: nElement of this tensor is not equal to nElement specified by sizes");
    DenseTensor result = new DenseTensor();
    result.resize(sizes);
    result.copy(this);
    return result;
  }

  @Override
  public Tensor save(String path, boolean overWrite) {
    throw new UnsupportedOperationException("operation not supported");
  }

  @Override
  public TensorNumeric getTensorNumeric() {
    return null;
  }

  @Override
  public double[] toArray() {
    Util.require(this.dim() == 1, "toArray only support 1D tensor");
    int n = this.nElement();
    double[] array = new double[n];
    int i = 0;
    while (i < n) {
      array[i] = this.valueAt(i + 1);
      i += 1;
    }

    return array;
  }

  @Override
  public Tensor addCopy(double s) {
    DenseTensor result = new DenseTensor();
    result.resizeAs(this);
    result.copy(this);

    TensorFunc2 addFunc = (data, index) -> data[index] = data[index] + s;

    DenseTensorApply.apply1(result, addFunc);
    return result;
  }

  @Override
  public Tensor addCopy(Tensor t) {
    DenseTensor result = new DenseTensor();
    result.resizeAs(this);
    result.copy(this);
    int n = result.nDimension();

    if (result.isContiguous() && t.isContiguous() && n == t.nElement()) {
      TensorNumeric.axpy(n, 1.0, t.storage().toDoubleArray(), t.storageOffset() - 1, 1,
          result.storage().toDoubleArray(), result.storageOffset() - 1, 1);
      return result;
    } else {
      TensorFunc4 addFunc = (data1, offset1, data2, offset2)
          -> data1[offset1] = data1[offset1] + data2[offset2];
      DenseTensorApply.apply2(result, t, addFunc);
      return result;
    }
  }

  @Override
  public Tensor subCopy(double s) {
    DenseTensor result = new DenseTensor();
    result.resizeAs(this);
    result.copy(this);
    TensorFunc2 subFunc = (data, index) -> data[index] = data[index] - s;

    DenseTensorApply.apply1(result, subFunc);
    return result;
  }

  @Override
  public Tensor subCopy(Tensor t) {
    DenseTensor result = new DenseTensor();
    result.resizeAs(this);
    result.copy(this);

    TensorFunc4 subFunc = (data1, offset1, data2, offset2)
        -> data1[offset1] = data1[offset1] - data2[offset2];
    DenseTensorApply.apply2(result, t, subFunc);
    return result;
  }

  @Override
  public Tensor negative() {
    DenseTensor result = new DenseTensor();
    result.resizeAs(this);
    result.copy(this);
    TensorFunc2 negFunc = (data, index) -> data[index] = -data[index];
    DenseTensorApply.apply1(result, negFunc);
    return result;
  }

  @Override
  public Tensor divCopy(double s) {
    DenseTensor result = new DenseTensor();
    result.resizeAs(this);
    result.copy(this);
    TensorFunc2 func = (data, index) -> data[index] = data[index] / s;

    DenseTensorApply.apply1(result, func);
    return result;
  }

  @Override
  public Tensor divCopy(Tensor t) {
    DenseTensor result = new DenseTensor();
    result.resizeAs(this);
    result.copy(this);

    TensorFunc4 func = (data1, offset1, data2, offset2)
        -> data1[offset1] = data1[offset1] / data2[offset2];
    DenseTensorApply.apply2(result, t, func);
    return result;
  }

  @Override
  public Tensor mulCopy(double s) {
    DenseTensor result = new DenseTensor();
    result.resizeAs(this);
    result.copy(this);
    TensorFunc2 func = (data, index) -> data[index] = data[index] * s;

    DenseTensorApply.apply1(result, func);
    return result;
  }

  @Override
  public Tensor mulCopy(Tensor t) {
    return DenseTensorMath.mul(this, t);
  }

  @Override
  public double sum() {
    return DenseTensorMath.sumAll(this);
  }

  @Override
  public double prod() {
    return DenseTensorMath.prodAll(this);
  }

  @Override
  public Tensor prod(Tensor x, int dim) {
    return DenseTensorMath.prod(this, x, dim - 1);
  }

  @Override
  public Tensor sum(int dim) {
    return DenseTensorMath.sum(null, this, dim - 1);
  }

  @Override
  public Tensor sum(Tensor x, int dim) {
    return null;
  }

  @Override
  public double mean() {
    return DenseTensorMath.meanAll(this);
  }

  @Override
  public Tensor mean(int dim) {
    return DenseTensorMath.mean(this, dim - 1);
  }

  @Override
  public double max() {
    return DenseTensorMath.maxAll(this);
  }

  @Override
  public TensorPair max(int dim) {
    throw new UnsupportedOperationException("operation not supported");
  }

  @Override
  public TensorPair max(Tensor values, Tensor indices, int dim) {
    throw new UnsupportedOperationException("operation not supported");
  }

  @Override
  public double min() {
    return DenseTensorMath.minAll(this);
  }

  @Override
  public TensorPair min(int dim) {
    throw new UnsupportedOperationException("operation not supported");
  }

  @Override
  public TensorPair min(Tensor values, Tensor indices, int dim) {
    throw new UnsupportedOperationException("operation not supported");
  }

  @Override
  public Tensor scatter(int dim, Tensor index, Tensor src) {
    return null;
  }

  @Override
  public Tensor gather(int dim, Tensor index, Tensor src) {
    return null;
  }

  @Override
  public Tensor conv2(Tensor kernel, char vf) {
    return null;
  }

  @Override
  public Tensor xcorr2(Tensor kernel, char vf) {
    return null;
  }

  @Override
  public Tensor sqrt() {
    return DenseTensorMath.sqrt(this, this);
  }

  @Override
  public Tensor tanh() {
    return DenseTensorMath.tanh(this, this);
  }

  @Override
  public Tensor abs() {
    return null;
  }

  @Override
  public Tensor add(double value, Tensor y) {
    return DenseTensorMath.cadd(this, this, value, y);
  }

  @Override
  public Tensor add(Tensor y) {
    throw new UnsupportedOperationException("operation not supported");
  }

  @Override
  public Tensor add(Tensor x, double value, Tensor y) {
    throw new UnsupportedOperationException("operation not supported");
  }

  @Override
  public Tensor add(double value) {
    throw new UnsupportedOperationException("operation not supported");
  }

  @Override
  public Tensor add(Tensor x, Tensor y) {
    throw new UnsupportedOperationException("operation not supported");
  }

  @Override
  public double dot(Tensor y) {
    return 0.0;
  }

  @Override
  public Tensor cmax(double value) {
    return null;
  }

  @Override
  public double dist(Tensor y, int norm) {
    return 0.0;
  }

  @Override
  public Tensor addcmul(double value, Tensor tensor1, Tensor tensor2) {
    return null;
  }

  @Override
  public Tensor addcmul(Tensor tensor1, Tensor tensor2) {
    return null;
  }

  @Override
  public Tensor addcdiv(double value, Tensor tensor1, Tensor tensor2) {
    return null;
  }

  @Override
  public Tensor sub(double value, Tensor y) {
    return DenseTensorMath.csub(this, this, TensorNumeric.negative(value), y);
  }

  @Override
  public Tensor sub(Tensor x, double value, Tensor y) {
    return DenseTensorMath.csub(this, x, value, y);
  }

  @Override
  public Tensor sub(Tensor y) {
    return null;
  }

  @Override
  public Tensor sub(Tensor x, Tensor y) {
    return null;
  }

  @Override
  public Tensor sub(double value) {
    return null;
  }

  @Override
  public Tensor cmul(Tensor y) {
    return DenseTensorMath.cmul(this, this, (DenseTensor) y);
  }

  @Override
  public Tensor cmul(Tensor x, Tensor y) {
    return DenseTensorMath.cmul(this, (DenseTensor) x, (DenseTensor) y);
  }

  @Override
  public Tensor cdiv(Tensor y) {
    return DenseTensorMath.cdiv(this, this, y);
  }

  @Override
  public Tensor cdiv(Tensor x, Tensor y) {
    return DenseTensorMath.cdiv(this, x, y);
  }

  @Override
  public Tensor mul(double value) {
    return null;
  }

  @Override
  public Tensor div(double value) {
    return null;
  }

  @Override
  public Tensor div(Tensor y) {
    return null;
  }

  @Override
  public Tensor mul(Tensor x, double value) {
    return null;
  }

  @Override
  public Tensor addmm(double v1, Tensor m, double v2, Tensor mat1, Tensor mat2) {
    return null;
  }

  @Override
  public Tensor addmm(Tensor m, Tensor mat1, Tensor mat2) {
    return null;
  }

  @Override
  public Tensor addmm(Tensor mat1, Tensor mat2) {
    return null;
  }

  @Override
  public Tensor addmm(double v2, Tensor mat1, Tensor mat2) {
    return null;
  }

  @Override
  public Tensor addmm(double v1, double v2, Tensor mat1, Tensor mat2) {
    return null;
  }

  @Override
  public Tensor mm(Tensor mat1, Tensor mat2) {
    return null;
  }

  @Override
  public Tensor addr(Tensor t1, Tensor t2) {
    return null;
  }

  @Override
  public Tensor addr(double v1, Tensor t1, Tensor t2) {
    return null;
  }

  @Override
  public Tensor addr(double v1, Tensor t1, double v2, Tensor t2) {
    return null;
  }

  @Override
  public Tensor addr(double v1, Tensor t1, double v2, Tensor t2, Tensor t3) {
    return null;
  }

  @Override
  public double uniform(double... args) {
    return 0.0;
  }

  @Override
  public Tensor addmv(double beta, Tensor vec1, double alpha, Tensor mat, Tensor vec2) {
    return null;
  }

  @Override
  public Tensor addmv(double beta, double alpha, Tensor mat, Tensor vec2) {
    return null;
  }

  @Override
  public Tensor addmv(double alpha, Tensor mat, Tensor vec2) {
    return null;
  }

  @Override
  public Tensor mv(Tensor mat, Tensor vec2) {
    return null;
  }

  @Override
  public Tensor baddbmm(double beta, Tensor m, double alpha, Tensor batch1, Tensor batch2) {
    return null;
  }

  @Override
  public Tensor baddbmm(double beta, double alpha, Tensor batch1, Tensor batch2) {
    return null;
  }

  @Override
  public Tensor baddbmm(double alpha, Tensor batch1, Tensor batch2) {
    return null;
  }

  @Override
  public Tensor bmm(Tensor batch1, Tensor batch2) {
    return null;
  }

  @Override
  public Tensor pow(Tensor y, double n) {
    return DenseTensorMath.pow(this, y, n);
  }

  @Override
  public Tensor pow(double n) {
    return DenseTensorMath.pow(this, this, n);
  }

  @Override
  public Tensor square() {
    return pow(2.0);
  }

  @Override
  public Tensor floor(Tensor y) {
    return null;
  }

  @Override
  public Tensor floor() {
    return null;
  }

  @Override
  public Tensor ceil() {
    return null;
  }

  @Override
  public Tensor inv() {
    return null;
  }

  @Override
  public Tensor erf() {
    return null;
  }

  @Override
  public Tensor erfc() {
    return null;
  }

  @Override
  public Tensor logGamma() {
    return null;
  }

  @Override
  public Tensor digamma() {
    return null;
  }

  @Override
  public TensorPair topk(int k, int dim, boolean increase,
                         Tensor result, Tensor indices, boolean sortedResult) {
    return null;
  }

  @Override
  public Tensor log(Tensor y) {
    return DenseTensorMath.log(this, y);
  }

  @Override
  public Tensor exp(Tensor y) {
    return DenseTensorMath.exp(this, y);
  }

  @Override
  public Tensor sqrt(Tensor y) {
    return DenseTensorMath.sqrt(this, y);
  }

  @Override
  public Tensor tanh(Tensor y) {
    return DenseTensorMath.tanh(this, y);
  }

  @Override
  public Tensor log1p(Tensor y) {
    return DenseTensorMath.log1p(this, y);
  }

  @Override
  public Tensor log() {
    return DenseTensorMath.log(this, this);
  }

  @Override
  public Tensor exp() {
    return DenseTensorMath.exp(this, this);
  }

  @Override
  public Tensor log1p() {
    return DenseTensorMath.log1p(this, this);
  }

  @Override
  public Tensor abs(Tensor x) {
    return null;
  }

  @Override
  public Tensor norm(Tensor y, int value, int dim) {
    return null;
  }

  @Override
  public Tensor gt(Tensor x, Tensor y) {
    return null;
  }

  @Override
  public Tensor lt(Tensor x, Tensor y) {
    return null;
  }

  @Override
  public Tensor le(Tensor x, Tensor y) {
    return null;
  }

  @Override
  public Tensor eq(Tensor x, double y) {
    return null;
  }

  @Override
  public Tensor maskedFill(Tensor mask, double e) {
    return null;
  }

  @Override
  public Tensor maskedCopy(Tensor mask, Tensor y) {
    return null;
  }

  @Override
  public Tensor maskedSelect(Tensor mask, Tensor y) {
    return null;
  }

  @Override
  public double norm(int value) {
    return 0.0;
  }

  @Override
  public Tensor sign() {
    return null;
  }

  @Override
  public Tensor ge(Tensor x, double value) {
    return null;
  }

  @Override
  public Tensor indexAdd(int dim, Tensor index, Tensor y) {
    return null;
  }

  @Override
  public Tensor index(int dim, Tensor index, Tensor y) {
    return null;
  }

  @Override
  public Tensor cmax(Tensor y) {
    return DenseTensorMath.cmax(this, this, y);
  }

  @Override
  public Tensor cmin(Tensor y) {
    return DenseTensorMath.cmin(this, this, y);
  }

  @Override
  public Tensor cmax(Tensor x, Tensor y) {
    return DenseTensorMath.cmax(this, x, y);
  }

  @Override
  public Tensor cmin(Tensor x, Tensor y) {
    return DenseTensorMath.cmin(this, x, y);
  }

  @Override
  public Tensor range(double xmin, double xmax, int step) {
    return null;
  }

  @Override
  public Tensor negative(Tensor x) {
    return null;
  }

  @Override
  public double sumSquare() {
    return 0.0;
  }

  @Override
  public Tensor clamp(double min, double max) {
    return null;
  }

  private void resizeAs(DenseTensor self, Tensor src) {
    if (!isSameSizeAs(self, src)) rawResize(self, src.nDimension(), src.size(), null);
  }

  private Tensor resize(DenseTensor self, int[] sizes, int[] strides) {
    if (sizes == null) {
      throw new IllegalStateException("Sizes cannot be null");
    }
    if (strides != null) {
      if (sizes.length == strides.length) {
        throw new IllegalStateException("invalid stride");
      }
    }
    return rawResize(self, sizes.length, sizes, strides);
  }

  private boolean isSameSizeAs(DenseTensor self, Tensor src) {
    if (self.nDimensionInternal != src.nDimension()) {
      return false;
    }

    if (self.isEmpty() != src.isEmpty()) {
      return false;
    }

    int d = 0;
    while (d < self.nDimensionInternal) {
      if (self.size(d + 1) != src.size(d + 1)) {
        return false;
      }
      d += 1;
    }
    return true;
  }

  private DenseTensor rawSet(DenseTensor self, ArrayDoubleStorage newStorage, int newStorageOffset,
                             int nDim, int[] newSize, int[] newStride) {
    Util.require(newStorageOffset >= 0, "Tensor: invalid storage offset");
    self.storageInternal = newStorage;
    self.storageOffsetInternal = newStorageOffset;
    return rawResize(self, nDim, newSize, newStride);
  }

  private DenseTensor rawResize(DenseTensor self, int nDim, int[] newSize, int[] newStride) {
    // resize as a scalar
    if (nDim == 0 && newSize.length == 0) {
      self.sizeInternal = new int[0];
      self.strideInternal = new int[0];
      self.nDimensionInternal = nDim;
      int totalSize = 1;
      if (self.storageInternal == null) {
        self.storageInternal = new ArrayDoubleStorage(new double[totalSize
            + self.storageOffsetInternal]);
      } else if (totalSize + self.storageOffsetInternal > self.storageInternal.length()) {
        self.storageInternal.resize(totalSize + self.storageOffsetInternal);
      }
      return self;
    }

    boolean hasCorrectSize = true;
    int nDim_ = 0;
    int d = 0;
    while (d < nDim) {
      nDim_ = nDim_ + 1;
      if (self.nDimensionInternal > d && newSize[d] != self.sizeInternal[d]) {
        hasCorrectSize = false;
      }
      if (self.nDimensionInternal > d && newStride != null && newStride[d] >= 0 &&
          newStride[d] != self.strideInternal[d]) {
        hasCorrectSize = false;
      }
      d += 1;
    }

    if (nDim_ != self.nDimensionInternal) hasCorrectSize = false;

    if (hasCorrectSize) return self;

    if (nDim_ > 0) {
      if (nDim_ != self.nDimensionInternal) {
        self.sizeInternal = new int[nDim];
        self.strideInternal = new int[nDim];
        self.nDimensionInternal = nDim;
      }

      int totalSize = 1;
      d = self.nDimensionInternal - 1;
      while (d >= 0) {
        self.sizeInternal[d] = newSize[d];
        if (newStride != null && newStride[d] >= 0) {
          self.strideInternal[d] = newStride[d];
        } else {
          if (d == self.nDimensionInternal - 1) {
            self.strideInternal[d] = 1;
          } else {
            self.strideInternal[d] = self.sizeInternal[d + 1] * self.strideInternal[d + 1];
          }
        }
        totalSize = totalSize + (self.sizeInternal[d] - 1) * self.strideInternal[d];

        d -= 1;
      }
      if (totalSize + self.storageOffsetInternal > 0) {
        if (self.storageInternal == null) {
          self.storageInternal = new ArrayDoubleStorage(new double[totalSize
              + self.storageOffsetInternal]);
        } else if (totalSize + self.storageOffsetInternal > self.storageInternal.length()) {
          self.storageInternal.resize(totalSize + self.storageOffsetInternal);
        }
      }
    } else {
      self.nDimensionInternal = 0;
    }

    return self;
  }

  public Tensor expandTensor(DenseTensor x) {
    int[] targetSize = DenseTensor.expandSize(this, x);
    int[] expandStrides = new int[targetSize.length];

    int[] expandStridesX = new int[targetSize.length];
    int i = targetSize.length - 1;
    int delta2 = targetSize.length - x.nDimension();
    while (i >= delta2) {
      if (x.size(i + 1 - delta2) != 1) expandStridesX[i] = x.stride(i + 1 - delta2);
      i -= 1;
    }
    DenseTensor expandX = new DenseTensor(
        x.storage(),
        x.storageOffset(),
        targetSize,
        expandStridesX
    );
    if (TensorNumeric.product(targetSize) != this.nElement()) {
      i = targetSize.length - 1;
      int delta1 = targetSize.length - this.nDimension();
      while (i >= delta1) {
        if (this.size(i + 1 - delta1) != 1) expandStrides[i] = this.stride(i + 1 - delta1);
        i -= 1;
      }
      DenseTensor tensor1 = new DenseTensor(
          this.storageInternal,
          this.storageOffset(),
          targetSize,
          expandStrides
      );
      DenseTensor newTensor = (DenseTensor) new DenseTensor().resize(targetSize).add(tensor1);
      this.set(newTensor);
    }
    return expandX;
  }

  private static int[] expandSize(DenseTensor tensor, DenseTensor other) {
    String errorMsg = "tensor size not match ${tensor.size.mkString()} " +
        "${other.size.mkString()}";
    DenseTensor longTensor = (tensor.dim() > other.dim()) ? tensor : other;
    DenseTensor shortTensor = (tensor.dim() > other.dim()) ? other : tensor;
    int ndim = longTensor.nDimension();
    int delta = longTensor.nDimension() - shortTensor.nDimension();
    int[] size = new int[ndim];
    int i = ndim - 1;
    while (i >= delta) {
      Util.require(longTensor.size(i + 1) == shortTensor.size(i + 1 - delta) ||
          longTensor.size(i + 1) == 1 ||
          shortTensor.size(i + 1 - delta) == 1, errorMsg);
      size[i] = Math.max(longTensor.size(i + 1), shortTensor.size(i + 1 - delta));
      i -= 1;
    }

    while (i >= 0) {
      size[i] = longTensor.size(i + 1);
      i -= 1;
    }

    return size;
  }
}

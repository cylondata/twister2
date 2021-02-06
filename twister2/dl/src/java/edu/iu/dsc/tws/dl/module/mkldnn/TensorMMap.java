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
package edu.iu.dsc.tws.dl.module.mkldnn;

import edu.iu.dsc.tws.comms.utils.Heap;
import edu.iu.dsc.tws.dl.data.Tensor;
import edu.iu.dsc.tws.dl.data.tensor.DnnTensor;
import edu.iu.dsc.tws.dl.module.mkldnn.memory.data.HeapData;
import edu.iu.dsc.tws.dl.module.mkldnn.memory.data.NativeData;
import edu.iu.dsc.tws.dl.utils.Util;

import java.io.Serializable;

/**
 * `TensorMMap` contains two tensors, dense and native, which are a map of each other.
 * It's used in the layer which contains weights. For the weight, we should sync the
 * dense tensor to native tensor before `submit`. For the gradient, we should sync the native
 * tensor to dense tensor after `submit`.
 *
 * @param _size the shape of Tensor, such as Array(4, 3, 224, 224)
 */
public class TensorMMap implements Serializable {
  private int[] _size;
  private MemoryOwner owner;
  // dense weight on heap is used to optimizer and so on, which is exposed to
  // AbstractModule level.
  Tensor dense = Tensor[Float](_size);

  // the native DnnTensor. It's allocate at runtime when do primitive initialization.
  // it has two benefits, the first is the clone will only clone one copy of weights and gradients
  // before primitive initialized and the second is that we can determined the size, type, format
  // when do initializing primitive.
  private transient DnnTensor _native = null;

  private transient MemoryData _from = null;
  private transient MemoryData _to = null;
  private transient ReorderMemory _reorder = null;

  private transient HeapData _heapData = null;
  public DnnTensor nativeDnn() {
    return (DnnTensor) _native;
  }

  public HeapData heapData(){
   return _heapData;
  }

  public void sync() {
    Util.require(_reorder != null && _native != null,
        "you should initialize the native relevant resources first");

    if(_from instanceof HeapData){
      _reorder.forward(this.dense);
    }else if(_from instanceof NativeData){
      _reorder.forward(this.native);
    }
  }

  /**
   * set the dense <-> native map, maintain the format to reorder
   *
   * Note, it will only create the native tensor based on the size and will not
   * do the reorder. So you should call `sync()` by manual.
   *
   * @param from the source tensor memory data, could be HeapData or NativeData
   * @param to the dest tensor memory data, could be HeapData or NativeData
   * @param runtime the mkldnn runtime for reorder operation
   */
  public void setMemoryData(MemoryData from,MemoryData to,MklDnnRuntime runtime) {
    Util.require(_from == null && _to == null, "you only can set once the memory data");
    _from = from;
    _to = to;

    _reorder = ReorderMemory(to);
    _reorder.setRuntime(runtime);
    _reorder.initFwdPrimitives(Array(_from), InferencePhase);

    _from match {
      case _: HeapData =>
        this._native = _reorder.output.asInstanceOf[DnnTensor[Float]]
        _heapData = _from.asInstanceOf[HeapData]
      case _: NativeData =>
        // the native tensor size should be determined by the memory description
        // other wise will be segment fault
        this._native = DnnTensor[Float](Memory.GetPaddingShape(_from.getMemoryDescription()));
        // the native initialize value should be all zeros.
        this._native.zero();
        _reorder.output.toTensor[Float].set(this.dense);
        _heapData = _to.asInstanceOf[HeapData];
    }
  }

  public void zero() {
    dense.zero();
    if (nativeDnn() != null) {
      nativeDnn().zero();
    }
  }

  public void copy(Tensor t) {
    dense.copy(t);
  }

  public int[] size() {
    return dense.size();
  }

  public int size(int index) {
    return dense.size(index);
  }

  public void release() {
    if (nativeDnn() != null) {
      nativeDnn().release();
    }
  }

  public void setNative(TensorMMap another) {
    if (nativeDnn() != null && another.nativeDnn() != null) {
      nativeDnn().set(another.nativeDnn());
    }
  }

}

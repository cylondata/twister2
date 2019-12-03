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
package edu.iu.dsc.tws.comms.functions.reduction;

import java.util.List;
import java.util.Map;

import edu.iu.dsc.tws.api.comms.DataFlowOperation;
import edu.iu.dsc.tws.api.comms.Op;
import edu.iu.dsc.tws.api.comms.ReduceFunction;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageType;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;

public class ReduceOperationFunction implements ReduceFunction {

  private MessageType messageType;
  private Op operation;

  public ReduceOperationFunction(Op operation, MessageType dtype) {
    if (dtype == MessageTypes.OBJECT || dtype == MessageTypes.STRING) {
      throw new RuntimeException("We don't support this message type for reduce function: "
          + dtype);
    }
    this.operation = operation;
    this.messageType = dtype;
  }

  @Override
  public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {

  }

  private static void validateArrayLength(int l1, int l2) {
    if (l1 != l2) {
      throw new Twister2RuntimeException(
          String.format("Arrays should be of equal length. Found : %d and %d", l1, l2)
      );
    }
  }

  /**
   * Applying the operation on data
   */
  public Object applyOp(Object data1, Object data2, AbstractOp op) {
    if (this.messageType == MessageTypes.INTEGER_ARRAY) {
      if (data1 instanceof int[] && data2 instanceof int[]) {
        int[] i1 = (int[]) data1;
        int[] i2 = (int[]) data2;
        validateArrayLength(i1.length, i2.length);
        int[] res = new int[i1.length];
        for (int i = 0; i < i1.length; i++) {
          res[i] = op.doInt(i1[i], i2[i]);
        }
        return res;
      } else {
        throw new RuntimeException(String.format("Message should be a %s array, got %s and %s",
            "int", data1.getClass(), data2.getClass()));
      }
    } else if (this.messageType == MessageTypes.INTEGER) {
      return op.doInt((int) data1, (int) data2);
    } else if (this.messageType == MessageTypes.DOUBLE_ARRAY) {
      if (data1 instanceof double[] && data2 instanceof double[]) {
        double[] i1 = (double[]) data1;
        double[] i2 = (double[]) data2;
        validateArrayLength(i1.length, i2.length);
        double[] res = new double[i1.length];
        for (int i = 0; i < i1.length; i++) {
          res[i] = op.doDouble(i1[i], i2[i]);
        }
        return res;
      } else {
        throw new RuntimeException(String.format("Message should be a %s array, got %s and %s",
            "double", data1.getClass(), data2.getClass()));
      }
    } else if (this.messageType == MessageTypes.DOUBLE) {
      return op.doDouble((double) data1, (double) data2);
    } else if (this.messageType == MessageTypes.SHORT_ARRAY) {
      if (data1 instanceof short[] && data2 instanceof short[]) {
        short[] i1 = (short[]) data1;
        short[] i2 = (short[]) data2;
        validateArrayLength(i1.length, i2.length);
        short[] res = new short[i1.length];
        for (int i = 0; i < i1.length; i++) {
          res[i] = op.doShort(i1[i], i2[i]);
        }
        return res;
      } else {
        throw new RuntimeException(String.format("Message should be a %s array, got %s and %s",
            "short", data1.getClass(), data2.getClass()));
      }
    } else if (this.messageType == MessageTypes.SHORT) {
      return op.doShort((short) data1, (short) data2);
    } else if (this.messageType == MessageTypes.BYTE_ARRAY) {
      if (data1 instanceof byte[] && data2 instanceof byte[]) {
        byte[] i1 = (byte[]) data1;
        byte[] i2 = (byte[]) data2;
        validateArrayLength(i1.length, i2.length);
        byte[] res = new byte[i1.length];
        for (int i = 0; i < i1.length; i++) {
          res[i] = op.doByte(i1[i], i2[i]);
        }
        return res;
      } else {
        throw new RuntimeException(String.format("Message should be a %s array, got %s and %s",
            "byte", data1.getClass(), data2.getClass()));
      }
    } else if (this.messageType == MessageTypes.BYTE) {
      return op.doByte((byte) data1, (byte) data2);
    } else if (this.messageType == MessageTypes.LONG_ARRAY) {
      if (data1 instanceof long[] && data2 instanceof long[]) {
        long[] i1 = (long[]) data1;
        long[] i2 = (long[]) data2;
        validateArrayLength(i1.length, i2.length);
        long[] res = new long[i1.length];
        for (int i = 0; i < i1.length; i++) {
          res[i] = op.doLong(i1[i], i2[i]);
        }
        return res;
      } else {
        throw new RuntimeException(String.format("Message should be a %s array, got %s and %s",
            "long", data1.getClass(), data2.getClass()));
      }
    } else if (this.messageType == MessageTypes.LONG) {
      return op.doLong((long) data1, (long) data2);
    } else {
      throw new Twister2RuntimeException("Message type is not supported for this operation");
    }
  }

  @Override
  public Object reduce(Object data1, Object data2) {
    if (this.operation == Op.SUM) { // Start SUM
      return this.applyOp(data1, data2, OpSum.getInstance());
    } else if (this.operation == Op.PRODUCT) {
      return this.applyOp(data1, data2, OpProduct.getInstance());
    } else if (this.operation == Op.DIVISION) {
      return this.applyOp(data1, data2, OpDivision.getInstance());
    } else if (this.operation == Op.MAX) {
      return this.applyOp(data1, data2, OpMax.getInstance());
    } else if (this.operation == Op.MIN) {
      return this.applyOp(data1, data2, OpMin.getInstance());
    } else {
      throw new Twister2RuntimeException("This operation is not supported.");
    }
  }
}



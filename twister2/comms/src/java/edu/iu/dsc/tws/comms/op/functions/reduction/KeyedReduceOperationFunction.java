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
package edu.iu.dsc.tws.comms.op.functions.reduction;

import java.util.List;
import java.util.Map;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.api.Op;
import edu.iu.dsc.tws.comms.api.ReduceFunction;
import edu.iu.dsc.tws.comms.dfw.io.KeyedContent;

/**
 * Provides support for keyed reduce operations
 */
public class KeyedReduceOperationFunction implements ReduceFunction {

  private MessageType messageType;
  private Op operation;

  public KeyedReduceOperationFunction(Op operation, MessageType dtype) {
    this.operation = operation;
    this.messageType = dtype;
  }

  @Override
  public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {

  }

  /**
   * Reduces the two values passed to the method and returns the reduced value. This method expects
   * the passed object to be of type KeyedContent. This method assumes that the key value is equal
   * for both messages and only uses the key value from t1 to create the return object
   *
   * @param t1 Value to be reduced
   * @param t2 value to be reduced
   * @return the reduced value ot t1 and t2
   */
  @Override
  public Object reduce(Object t1, Object t2) {
    if (!(t1 instanceof KeyedContent) || !(t2 instanceof KeyedContent)) {
      throw new IllegalArgumentException("The keyed operation function"
          + " requires KeyedContent objects " + t1 + " " + t2);
    }

    KeyedContent keyedContent1 = (KeyedContent) t1;
    KeyedContent keyedContent2 = (KeyedContent) t2;

    Object data1 = keyedContent1.getValue();
    Object data2 = keyedContent2.getValue();

    //It is assumed that the key is equal for both messages
    Object key = keyedContent1.getKey();

    Object result = null;
    if (this.operation == Op.SUM) { // Start SUM
      if (this.messageType == MessageType.INTEGER) {
        if (data1 instanceof int[] && data2 instanceof int[]) {
          int[] i1 = (int[]) data1;
          int[] i2 = (int[]) data2;
          int[] res = new int[i1.length];
          for (int i = 0; i < i1.length; i++) {
            res[i] = i1[i] + i2[i];
          }
          result = res;
        }
      } else if (this.messageType == MessageType.DOUBLE) {
        if (data1 instanceof double[] && data2 instanceof double[]) {
          double[] i1 = (double[]) data1;
          double[] i2 = (double[]) data2;
          double[] res = new double[i1.length];
          for (int i = 0; i < i1.length; i++) {
            res[i] = i1[i] + i2[i];
          }
          result = res;
        }
      } else if (this.messageType == MessageType.SHORT) {
        if (data1 instanceof short[] && data2 instanceof short[]) {
          short[] i1 = (short[]) data1;
          short[] i2 = (short[]) data2;
          short[] res = new short[i1.length];
          for (int i = 0; i < i1.length; i++) {
            res[i] = (short) (i1[i] + i2[i]);
          }
          result = res;
        }
      } else if (this.messageType == MessageType.BYTE) {
        if (data1 instanceof byte[] && data2 instanceof byte[]) {
          byte[] i1 = (byte[]) data1;
          byte[] i2 = (byte[]) data2;
          byte[] res = new byte[i1.length];
          for (int i = 0; i < i1.length; i++) {
            res[i] = (byte) (i1[i] + i2[i]);
          }
          result = res;
        }
      } else if (this.messageType == MessageType.LONG) {
        if (data1 instanceof long[] && data2 instanceof long[]) {
          long[] i1 = (long[]) data1;
          long[] i2 = (long[]) data2;
          long[] res = new long[i1.length];
          for (int i = 0; i < i1.length; i++) {
            res[i] = i1[i] + i2[i];
          }
          result = res;
        }

      }
    }
    if (this.operation == Op.PRODUCT) { // Start PRODUCT
      if (this.messageType == MessageType.INTEGER) {
        if (data1 instanceof int[] && data2 instanceof int[]) {
          int[] i1 = (int[]) data1;
          int[] i2 = (int[]) data2;
          int[] res = new int[i1.length];
          for (int i = 0; i < i1.length; i++) {
            res[i] = i1[i] * i2[i];
          }
          result = res;
        }
      } else if (this.messageType == MessageType.DOUBLE) {
        if (data1 instanceof double[] && data2 instanceof double[]) {
          double[] i1 = (double[]) data1;
          double[] i2 = (double[]) data2;
          double[] res = new double[i1.length];
          for (int i = 0; i < i1.length; i++) {
            res[i] = i1[i] * i2[i];
          }
          result = res;
        }
      } else if (this.messageType == MessageType.SHORT) {
        if (data1 instanceof short[] && data2 instanceof short[]) {
          short[] i1 = (short[]) data1;
          short[] i2 = (short[]) data2;
          short[] res = new short[i1.length];
          for (int i = 0; i < i1.length; i++) {
            res[i] = (short) (i1[i] * i2[i]);
          }
          result = res;
        }
      } else if (this.messageType == MessageType.BYTE) {
        if (data1 instanceof byte[] && data2 instanceof byte[]) {
          byte[] i1 = (byte[]) data1;
          byte[] i2 = (byte[]) data2;
          byte[] res = new byte[i1.length];
          for (int i = 0; i < i1.length; i++) {
            res[i] = (byte) (i1[i] * i2[i]);
          }
          result = res;
        }
      } else if (this.messageType == MessageType.LONG) {
        if (data1 instanceof long[] && data2 instanceof long[]) {
          long[] i1 = (long[]) data1;
          long[] i2 = (long[]) data2;
          long[] res = new long[i1.length];
          for (int i = 0; i < i1.length; i++) {
            res[i] = i1[i] * i2[i];
          }
          result = res;
        }
      }
    } // END PRODUCT
    if (this.operation == Op.DIVISION) { // Start DIVISION
      if (this.messageType == MessageType.INTEGER) {
        if (data1 instanceof int[] && data2 instanceof int[]) {
          int[] i1 = (int[]) data1;
          int[] i2 = (int[]) data2;
          int[] res = new int[i1.length];
          for (int i = 0; i < i1.length; i++) {
            res[i] = i1[i] / i2[i];
          }
          result = res;
        }
      } else if (this.messageType == MessageType.DOUBLE) {
        if (data1 instanceof double[] && data2 instanceof double[]) {
          double[] i1 = (double[]) data1;
          double[] i2 = (double[]) data2;
          double[] res = new double[i1.length];
          for (int i = 0; i < i1.length; i++) {
            res[i] = i1[i] / i2[i];
          }
          result = res;
        }
      } else if (this.messageType == MessageType.SHORT) {
        if (data1 instanceof short[] && data2 instanceof short[]) {
          short[] i1 = (short[]) data1;
          short[] i2 = (short[]) data2;
          short[] res = new short[i1.length];
          for (int i = 0; i < i1.length; i++) {
            res[i] = (short) (i1[i] / i2[i]);
          }
          result = res;
        }
      } else if (this.messageType == MessageType.BYTE) {
        if (data1 instanceof byte[] && data2 instanceof byte[]) {
          byte[] i1 = (byte[]) data1;
          byte[] i2 = (byte[]) data2;
          byte[] res = new byte[i1.length];
          for (int i = 0; i < i1.length; i++) {
            res[i] = (byte) (i1[i] / i2[i]);
          }
          result = res;
        }
      } else if (this.messageType == MessageType.LONG) {
        if (data1 instanceof long[] && data2 instanceof long[]) {
          long[] i1 = (long[]) data1;
          long[] i2 = (long[]) data2;
          long[] res = new long[i1.length];
          for (int i = 0; i < i1.length; i++) {
            res[i] = i1[i] / i2[i];
          }
          result = res;
        }
      }
    } // END DIVISION

    if (this.operation == Op.MAX) { // Start MAX
      if (this.messageType == MessageType.INTEGER) {
        if (data1 instanceof int[] && data2 instanceof int[]) {
          int[] i1 = (int[]) data1;
          int[] i2 = (int[]) data2;
          int[] res = new int[i1.length];
          for (int i = 0; i < i1.length; i++) {
            res[i] = Math.max(i1[i], i2[i]);
          }
          result = res;
        }
      } else if (this.messageType == MessageType.DOUBLE) {
        if (data1 instanceof double[] && data2 instanceof double[]) {
          double[] i1 = (double[]) data1;
          double[] i2 = (double[]) data2;
          double[] res = new double[i1.length];
          for (int i = 0; i < i1.length; i++) {
            res[i] = Math.max(i1[i], i2[i]);
          }
          result = res;
        }
      } else if (this.messageType == MessageType.SHORT) {
        if (data1 instanceof short[] && data2 instanceof short[]) {
          short[] i1 = (short[]) data1;
          short[] i2 = (short[]) data2;
          short[] res = new short[i1.length];
          for (int i = 0; i < i1.length; i++) {
            res[i] = (short) Math.max(i1[i], i2[i]);
          }
          result = res;
        }
      } else if (this.messageType == MessageType.BYTE) {
        if (data1 instanceof byte[] && data2 instanceof byte[]) {
          byte[] i1 = (byte[]) data1;
          byte[] i2 = (byte[]) data2;
          byte[] res = new byte[i1.length];
          for (int i = 0; i < i1.length; i++) {
            res[i] = (byte) Math.max(i1[i], i2[i]);
          }
          result = res;
        }
      } else if (this.messageType == MessageType.LONG) {
        if (data1 instanceof long[] && data2 instanceof long[]) {
          long[] i1 = (long[]) data1;
          long[] i2 = (long[]) data2;
          long[] res = new long[i1.length];
          for (int i = 0; i < i1.length; i++) {
            res[i] = Math.max(i1[i], i2[i]);
          }
          result = res;
        }
      }
    } // END MAX

    if (this.operation == Op.MIN) { // Start MIN
      if (this.messageType == MessageType.INTEGER) {
        if (data1 instanceof int[] && data2 instanceof int[]) {
          int[] i1 = (int[]) data1;
          int[] i2 = (int[]) data2;
          int[] res = new int[i1.length];
          for (int i = 0; i < i1.length; i++) {
            res[i] = Math.min(i1[i], i2[i]);
          }
          result = res;
        }
      } else if (this.messageType == MessageType.DOUBLE) {
        if (data1 instanceof double[] && data2 instanceof double[]) {
          double[] i1 = (double[]) data1;
          double[] i2 = (double[]) data2;
          double[] res = new double[i1.length];
          for (int i = 0; i < i1.length; i++) {
            res[i] = Math.min(i1[i], i2[i]);
          }
          result = res;
        }
      } else if (this.messageType == MessageType.SHORT) {
        if (data1 instanceof short[] && data2 instanceof short[]) {
          short[] i1 = (short[]) data1;
          short[] i2 = (short[]) data2;
          short[] res = new short[i1.length];
          for (int i = 0; i < i1.length; i++) {
            res[i] = (short) Math.min(i1[i], i2[i]);
          }
          result = res;
        }
      } else if (this.messageType == MessageType.BYTE) {
        if (data1 instanceof byte[] && data2 instanceof byte[]) {
          byte[] i1 = (byte[]) data1;
          byte[] i2 = (byte[]) data2;
          byte[] res = new byte[i1.length];
          for (int i = 0; i < i1.length; i++) {
            res[i] = (byte) Math.min(i1[i], i2[i]);
          }
          result = res;
        }
      } else if (this.messageType == MessageType.LONG) {
        if (data1 instanceof long[] && data2 instanceof long[]) {
          long[] i1 = (long[]) data1;
          long[] i2 = (long[]) data2;
          long[] res = new long[i1.length];
          for (int i = 0; i < i1.length; i++) {
            res[i] = Math.min(i1[i], i2[i]);
          }
          result = res;
        }
      }
    } // END MIN

    return new KeyedContent(key, result,
        keyedContent1.getKeyType(), keyedContent1.getContentType());
  }
}

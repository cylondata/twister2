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

public class OpMax implements AbstractOp {

  private static final OpMax INSTANCE = new OpMax();

  public static AbstractOp getInstance() {
    return INSTANCE;
  }

  @Override
  public int doInt(int o1, int o2) {
    return o1 > o2 ? o1 : o2;
  }

  @Override
  public long doLong(long o1, long o2) {
    return o1 > o2 ? o1 : o2;
  }

  @Override
  public short doShort(short o1, short o2) {
    return o1 > o2 ? o1 : o2;
  }

  @Override
  public float doFloat(float o1, float o2) {
    return o1 > o2 ? o1 : o2;
  }

  @Override
  public double doDouble(double o1, double o2) {
    return o1 > o2 ? o1 : o2;
  }

  @Override
  public byte doByte(byte o1, byte o2) {
    return o1 > o2 ? o1 : o2;
  }
}

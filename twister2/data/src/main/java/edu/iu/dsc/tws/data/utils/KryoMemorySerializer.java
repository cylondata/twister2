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
package edu.iu.dsc.tws.data.utils;

import java.io.InputStream;
import java.util.Map;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class KryoMemorySerializer {
  private Kryo kryo;
  private Output kryoOut;
  private Input kryoIn;

  public KryoMemorySerializer(Kryo kryo) {
    this.kryo = kryo;
    kryo.setReferences(false);
    kryoOut = new Output(2000, 2000000000);
    kryoIn = new Input(1);
  }

  public KryoMemorySerializer() {
    kryo = new Kryo();
    kryo.setReferences(false);
    kryoOut = new Output(2000, 2000000000);
    kryoIn = new Input(1);
  }

  public void init(Map<String, Object> config) {
  }

  public byte[] serialize(Object object) {
    kryoOut.clear();
    kryo.writeClassAndObject(kryoOut, object);
    return kryoOut.toBytes();
  }

  public Object deserialize(byte[] input) {
    kryoIn.setBuffer(input);
    return kryo.readClassAndObject(kryoIn);
  }

  public Object deserialize(InputStream inputStream) {
    Input input = new Input(inputStream);
    return kryo.readClassAndObject(input);
  }
}

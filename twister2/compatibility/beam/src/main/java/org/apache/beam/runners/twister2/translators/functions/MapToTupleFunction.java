/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.runners.twister2.translators.functions;

import java.util.logging.Logger;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.util.CoderUtils;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;

import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.api.tset.fn.MapFunc;

/**
 * Map to tuple function.
 */
public class MapToTupleFunction<K, V>
    implements MapFunc<Tuple<byte[], byte[]>, WindowedValue<KV<K, V>>> {

  private final Coder<K> keyCoder;
  private final WindowedValue.WindowedValueCoder<V> wvCoder;
  private static final Logger LOG = Logger.getLogger(MapToTupleFunction.class.getName());

  public MapToTupleFunction(Coder<K> inputKeyCoder, WindowedValue.WindowedValueCoder<V> wvCoder) {
    this.keyCoder = inputKeyCoder;
    this.wvCoder = wvCoder;
  }

  @Override
  public Tuple<byte[], byte[]> map(WindowedValue<KV<K, V>> input) {
    Tuple<byte[], byte[]> element = null;

    WindowedValue<KV<K, WindowedValue<V>>> temp =
        WindowedValue.of(
            KV.of(
                input.getValue().getKey(),
                WindowedValue.of(
                    input.getValue().getValue(),
                    input.getTimestamp(),
                    input.getWindows(),
                    input.getPane())),
            input.getTimestamp(),
            input.getWindows(),
            input.getPane());
    try {
      element =
          new Tuple<>(
              CoderUtils.encodeToByteArray(keyCoder, temp.getValue().getKey()),
              CoderUtils.encodeToByteArray(wvCoder, temp.getValue().getValue()));
    } catch (CoderException e) {
      LOG.info(e.getMessage());
    }
    return element;
  }

  @Override
  public void prepare(TSetContext context) {
  }
}

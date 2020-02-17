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
package edu.iu.dsc.tws.api.tset.schema;

import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;

public final class PrimitiveSchemas {
  private PrimitiveSchemas() {
  }

  public static final Schema INTEGER = new IntegerSchema();

  public static final Schema STRING = new StringSchema();

  public static final Schema OBJECT = new ObjectSchema();

  public static final Schema EMPTY = new EmptySchema();

  public static final Schema NULL = new NullSchema();

  public static final KeyedSchema OBJECT_TUPLE2 = new KeyedSchema(MessageTypes.OBJECT,
      MessageTypes.OBJECT);

  public static final JoinSchema OBJECT_TUPLE3 = new JoinSchema(MessageTypes.OBJECT,
      MessageTypes.OBJECT, MessageTypes.OBJECT);

  public static final KeyedSchema NULL_TUPLE2 = new KeyedSchema(null, null);
}

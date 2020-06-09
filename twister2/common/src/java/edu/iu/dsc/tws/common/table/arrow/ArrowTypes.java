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
package edu.iu.dsc.tws.common.table.arrow;

import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.FieldType;

public final class ArrowTypes {
  private ArrowTypes() {
  }

  public static final FieldType INT_FIELD_TYPE =
      new FieldType(false,
          new ArrowType.Int(32, true), null);
  public static final FieldType LONG_FIELD_TYPE =
      new FieldType(false,
          new ArrowType.Int(64, true), null);
  public static final FieldType SHORT_FIELD_TYPE =
      new FieldType(false,
          new ArrowType.Int(16, true), null);
  public static final FieldType FLOAT_FIELD_TYPE =
      new FieldType(false,
          new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE), null);
  public static final FieldType DOUBLE_FIELD_TYPE =
      new FieldType(false,
          new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), null);
  public static final FieldType STRING_FIELD_TYPE =
      new FieldType(false,
          new ArrowType.Binary(), null);
  public static final FieldType BINARY_FILED_TYPE =
      new FieldType(false,
          new ArrowType.Binary(), null);
}

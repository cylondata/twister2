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
package org.apache.storm.tuple;

import java.util.List;

import org.apache.storm.generated.GlobalStreamId;

import edu.iu.dsc.tws.task.api.IMessage;

public class Twister2Tuple implements Tuple {


  private final Fields fields;
  private final Values values;
  private final IMessage iMessage;

  public Twister2Tuple(Fields fields, Values values, IMessage iMessage) {
    this.fields = fields;
    this.values = values;
    this.iMessage = iMessage;
  }

  @Override
  public int size() {
    return values.size();
  }

  @Override
  public boolean contains(String field) {
    int fieldIndex = fields.fieldIndex(field);
    return values.get(fieldIndex) != null;
  }

  @Override
  public Fields getFields() {
    return this.fields;
  }

  @Override
  public int fieldIndex(String field) {
    return this.fields.fieldIndex(field);
  }

  @Override
  public List<Object> select(Fields selector) {
    return this.fields.select(selector, values);
  }

  @Override
  public Object getValue(int i) {
    return this.values.get(i);
  }

  @Override
  public String getString(int i) {
    return String.valueOf(this.values.get(i));
  }

  @Override
  public Integer getInteger(int i) {
    return (Integer) this.values.get(i);
  }

  @Override
  public Long getLong(int i) {
    return (Long) this.values.get(i);
  }

  @Override
  public Boolean getBoolean(int i) {
    return (Boolean) this.values.get(i);
  }

  @Override
  public Short getShort(int i) {
    return (Short) this.values.get(i);
  }

  @Override
  public Byte getByte(int i) {
    return (Byte) this.values.get(i);
  }

  @Override
  public Double getDouble(int i) {
    return (Double) this.values.get(i);
  }

  @Override
  public Float getFloat(int i) {
    return (Float) this.values.get(i);
  }

  @Override
  public byte[] getBinary(int i) {
    return (byte[]) this.values.get(i);
  }

  @Override
  public Object getValueByField(String field) {
    return this.getValue(this.fields.fieldIndex(field));
  }

  @Override
  public String getStringByField(String field) {
    return this.getString(this.fields.fieldIndex(field));
  }

  @Override
  public Integer getIntegerByField(String field) {
    return this.getInteger(this.fields.fieldIndex(field));
  }

  @Override
  public Long getLongByField(String field) {
    return this.getLong(this.fields.fieldIndex(field));
  }

  @Override
  public Boolean getBooleanByField(String field) {
    return this.getBoolean(this.fields.fieldIndex(field));
  }

  @Override
  public Short getShortByField(String field) {
    return this.getShort(this.fields.fieldIndex(field));
  }

  @Override
  public Byte getByteByField(String field) {
    return this.getByte(this.fields.fieldIndex(field));
  }

  @Override
  public Double getDoubleByField(String field) {
    return this.getDouble(this.fields.fieldIndex(field));
  }

  @Override
  public Float getFloatByField(String field) {
    return this.getFloat(this.fields.fieldIndex(field));
  }

  @Override
  public byte[] getBinaryByField(String field) {
    return this.getBinary(this.fields.fieldIndex(field));
  }

  @Override
  public List<Object> getValues() {
    return this.values;
  }

  @Override
  public GlobalStreamId getSourceGlobalStreamId() {
    return new GlobalStreamId(getSourceComponent(), getSourceStreamId());
  }

  @Override
  public String getSourceComponent() {
    return null;
  }

  @Override
  public int getSourceTask() {
    return 0;
  }

  @Override
  public String getSourceStreamId() {
    return this.iMessage.edge();
  }

  @Override
  public void resetValues() {
    this.values.clear();
  }
}

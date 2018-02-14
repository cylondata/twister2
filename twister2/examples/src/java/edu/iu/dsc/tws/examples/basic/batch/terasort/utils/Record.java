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
package edu.iu.dsc.tws.examples.basic.batch.terasort.utils;

import org.apache.hadoop.io.Text;

public class Record implements Comparable<Record> {
  public static final int KEY_SIZE = 10;
  public static final int DATA_SIZE = 90;
  public static final int RECORD_LENGTH = KEY_SIZE + DATA_SIZE;

  private Text key;
  private Text text;

  public Record(Text key) {
    this.key = key;
  }

  public Record(Text key, Text text) {
    this.key = key;
    this.text = text;
  }

  public Text getKey() {
    return key;
  }

  public Text getText() {
    return text;
  }

  @Override
  public int compareTo(Record o) {
    return this.key.compareTo(o.getKey());
  }
}

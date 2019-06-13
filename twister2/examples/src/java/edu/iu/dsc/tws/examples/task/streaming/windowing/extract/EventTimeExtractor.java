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
package edu.iu.dsc.tws.examples.task.streaming.windowing.extract;

import edu.iu.dsc.tws.examples.task.streaming.windowing.data.EventTimeData;
import edu.iu.dsc.tws.task.api.window.api.TimestampExtractor;

public class EventTimeExtractor extends TimestampExtractor<EventTimeData> {
  @Override
  public long extractTimestamp(EventTimeData eventTimeData) {
    return eventTimeData.getTime();
  }
}

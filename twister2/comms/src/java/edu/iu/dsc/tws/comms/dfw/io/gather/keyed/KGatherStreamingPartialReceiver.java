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
package edu.iu.dsc.tws.comms.dfw.io.gather.keyed;

/**
 * Abstract class that is extended by keyed reduce batch receivers
 */
public class KGatherStreamingPartialReceiver extends KGatherStreamingReceiver {
  public KGatherStreamingPartialReceiver(int dest, int limitPerKey, int windowSize) {
    this.destination = dest;
    this.limitPerKey = limitPerKey;
    this.windowSize = windowSize;
    this.localWindowCount = 0;
  }

}

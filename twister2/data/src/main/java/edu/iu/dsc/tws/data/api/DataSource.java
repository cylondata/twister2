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
package edu.iu.dsc.tws.data.api;

import edu.iu.dsc.tws.common.config.Config;

/**
 *  Bass class of all twister2 data sources.
 */

public interface DataSource {

    /**
     * configures this datasource with initial values.
     * @param config contains the task ID and reference to the input queue
     * @return This object.
     */
    DataSource init(Config config);

    /**
     * this method contains the actual readings from data sources
     * this method should have a while loop that continuously poll from the datasource and pushes the input queue.
     * the implementation should contain a volatile variable to break the loop whenever needed, which is triggered by
     * close() method
     */
    void run();

    /**
     * this method is called to break the while loop in the run method.
     * @return this object
     */
    DataSource close();

}

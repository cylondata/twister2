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
package edu.iu.dsc.tws.python;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.tset.TSetEnvironment;
import edu.iu.dsc.tws.api.tset.sets.BaseTSet;
import edu.iu.dsc.tws.api.tset.sets.batch.SourceTSet;
import edu.iu.dsc.tws.python.processors.PythonLambdaProcessor;
import edu.iu.dsc.tws.python.tset.PLambdaTSetSource;

public class Twister2Context {

    private TSetEnvironment tSetEnvironment;

    Twister2Context(TSetEnvironment tSetEnvironment) {
        this.tSetEnvironment = tSetEnvironment;
    }

    public int getWorkerId() {
        return this.tSetEnvironment.getWorkerID();
    }

    public Config getConfig() {
        return this.tSetEnvironment.getConfig();
    }

    public SourceTSet createLambdaSource(byte[] lambda, int parallelism) {
        PLambdaTSetSource pLambdaTSetSource = new PLambdaTSetSource(lambda);
        return (SourceTSet) tSetEnvironment.createSource(pLambdaTSetSource, parallelism);
    }

    public void executePyFunction(byte[] lambda, Object input) {
        Object o = new PythonLambdaProcessor(lambda).process(input);
        System.out.println("Printing output in java side : " + o);
    }
}

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
package edu.iu.dsc.tws.tsched.taskprogrammingmodel;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import edu.iu.dsc.tws.tsched.taskprogrammingmodel.parameter.Direction;
import edu.iu.dsc.tws.tsched.taskprogrammingmodel.parameter.Stream;
import edu.iu.dsc.tws.tsched.taskprogrammingmodel.parameter.Type;


@Retention (RetentionPolicy.RUNTIME)
@Target (ElementType.PARAMETER)

public @interface Parameter {

    Type type() default Type.UNSPECIFIED;

    Direction direction() default Direction.IN;

    Stream stream() default Stream.UNSPECIFIED;

    String prefix() default Constants.PREFIX_EMTPY;

}

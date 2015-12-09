/*
 * Copyright 1&1 Internet AG, https://github.com/1and1/
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.oneandone.reactive.utils.problem;



import javax.ws.rs.WebApplicationException;

import com.google.common.collect.ImmutableMap;



public class StdProblem extends Problem {
    
    private StdProblem(ImmutableMap<String, String> problemData) {
        super(problemData);
    }

    public static StdProblem of(WebApplicationException wae) {
        return new StdProblem(parseProblemData(wae));
    }

    
    
    public boolean isMalformedRequestDataProblem() {
        return is(400, "urn:problem:formed-request-data");
    }

    public static StdProblem newMalformedRequestDataProblem() {
        return new StdProblem(newProblemdata(400, "urn:problem:formed-request-data"));
    }
    
    
    public boolean isUnsupportedMimeTypeProblem() {
        return is(415, "urn:problem:unsupported-mimetype");
    }

    public static StdProblem newUnsupportedMimeTypeProblem() {
        return new StdProblem(newProblemdata(415, "urn:problem:unsupported-mimetype"));
    }


    public boolean isUnacceptedMimeTypeProblem() {
        return is(406, "urn:problem:unaccepted-mimetype");
    }

    public static StdProblem newUnacceptedMimeTypeProblem() {
        return new StdProblem(newProblemdata(406, "urn:problem:unaccepted-mimetype"));
    }
    
    
    public boolean isServerErrorProblem() {
        return is(500, "urn:problem:server-error");
    }
   
    public static StdProblem newServerErrorProblem() {
        return new StdProblem(newProblemdata(500, "urn:problem:server-error"));
    }    
}
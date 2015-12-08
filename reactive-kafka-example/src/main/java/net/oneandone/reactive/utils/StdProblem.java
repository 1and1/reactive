package net.oneandone.reactive.utils;



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
package net.oneandone.reactive.utils;



import javax.ws.rs.ProcessingException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;

import static com.google.common.base.Preconditions.checkNotNull;
import java.util.Map;


public final class ProblemTester {

    private ProblemTester() {}

    public static boolean isProblem(WebApplicationException e, int responseStatusCode, String problemTypeUrn) {
        checkNotNull(e);
        return isProblem(e.getResponse(), responseStatusCode, problemTypeUrn);
    }

    public static boolean isProblem(Response response, int responseStatusCode, String problemTypeUrn) {
        checkNotNull(response);
        checkNotNull(problemTypeUrn);
        if (response.getStatus() == responseStatusCode) {
            try {
                response.bufferEntity();
                Map<String, String> simpleProblem = response.readEntity(new GenericType<Map<String, String>>() { });
                if (simpleProblem != null && simpleProblem.get("type").equals(problemTypeUrn)) {
                    return true;
                }
            } catch(ProcessingException e) {
                return false;
            }
        }
        return false;
    }
}
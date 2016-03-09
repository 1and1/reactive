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
package net.oneandone.incubator.neo.http.sink;



import java.io.Closeable;

import java.io.File;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

import javax.ws.rs.core.MediaType;
import javax.xml.bind.annotation.XmlRootElement;



public class MyExampleService implements Closeable {
	private final HttpSink httpSink;
	// ...
	
	public MyExampleService(final URI sinkUri) {
		//...
		
	  	this.httpSink = HttpSink.target(sinkUri)
	  						    .withRetryAfter(Duration.ofSeconds(2), Duration.ofSeconds(30), Duration.ofMinutes(5), Duration.ofMinutes(30))
	  							.withPersistency(true) 
	  							.withRetryBufferSize(20000)    
	  							.open();
	}
	
	@Override
	public void close() {
		httpSink.close();
	}
	
	public void myBusinessMethod() {
		// ...
		final MyChangedEvent myMessage = new MyChangedEvent(34343434);
		
		
		httpSink.submitAsync(myMessage, MediaType.APPLICATION_JSON); // submits in n async way in background
	}

	
	
	   

    @XmlRootElement 
    public static class MyChangedEvent {
        public Header header = new Header();
        public int accountid;
        
        public MyChangedEvent() { }
    
         
        public MyChangedEvent(int accountid) {
            this.accountid = accountid;
        }
    }

    public static class Header {
        public String eventId = UUID.randomUUID().toString().replace("-", ""); 
        public String timestamp = Instant.now().toString();
        public AuthenticationInfo authInfo;
        
        
        public Header() {  }
        

        public Header(String eventId, String timestamp, AuthenticationInfo authInfo) { 
            this.eventId = eventId;
            this.timestamp = timestamp;
            this.authInfo = authInfo;
        }
        
        
        public Header withAuthInfo(AuthenticationInfo.AuthenticationScheme scheme, String principalname) {
            return new Header(eventId, timestamp, new AuthenticationInfo(scheme, principalname));
        }
    }
    
    public static final class AuthenticationInfo  {
        public static enum AuthenticationScheme { BASIC, CLIENTCERT, DIGEST, PLAIN, FORM, OAUTHBEARER };
        
        public AuthenticationInfo() {  }
        
        public AuthenticationInfo(AuthenticationScheme scheme, String principalname) {
            this.principalname = principalname;
            this.scheme = scheme;
        }
        
        public String principalname;
        public AuthenticationScheme scheme;
    }
}
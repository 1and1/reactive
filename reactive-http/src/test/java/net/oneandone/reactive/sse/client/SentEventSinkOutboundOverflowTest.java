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
package net.oneandone.reactive.sse.client;



import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;

import net.oneandone.reactive.ReactiveSink;
import net.oneandone.reactive.ReactiveSource;
import net.oneandone.reactive.sse.ServerSentEvent;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Splitter;


public class SentEventSinkOutboundOverflowTest extends TestServletbasedTest  {
    
    private static final String LARGE_TEXT = "DSDGSRHEDHGSFDFADFWSFSADFQEWRTSFDGASFDFSADFASTFRWAERTWSGSDFSDFSDGFSRGTWRTWERGSFGSDFsdfaser" +
                                             "gfsdfgsdfgsagadfgsafgsgsgasfgasfdgasdfgdsfgaerzqtehdbycbnsfthastrhdfadfbyfxbadfgaehgatedhd" +
                                             "affdaffbdfadfhadthadhdatrhdadfsrzsfietzurthadthatehzutrzhadthadfadgtghtarhzqethadthadthadg" +
                                             "gfsdfgsdfgsagadfgsafgsgsgasfgasfdgasdfgdsfgaerzqtehdbycbnsfthastrhdfadfbyfxbadfgaehgatedhd" +
                                             "affdaffbdfadfhadthadhdatrhdadfsrzsfietzurthadthatehzutrzhadthadfadgtghtarhzqethadthadthadg" +
                                             "gfsdfgsdfgsagadfgsafgsgsgasfgasfdgasdfgdsfgaerzqtehdbycbnsfthastrhdfadfbyfxbadfgaehgatedhd" +
                                             "affdaffbdfadfhadthadhdatrhdadfsrzsfietzurthadthatehzutrzhadthadfadgtghtarhzqethadthadthadg" +
                                             "gfsdfgsdfgsagadfgsafgsgsgasfgasfdgasdfgdsfgaerzqtehdbycbnsfthastrhdfadfbyfxbadfgaehgatedhd" +
                                             "affdaffbdfadfhadthadhdatrhdadfsrzsfietzurthadthatehzutrzhadthadfadgtghtarhzqethadthadthadg";

    
    /*
    public ServerSentEventSinkTest() {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "DEBUG");
    }
    */
  
    
    
    @Test
    public void testWriteBufferOverflow() throws Exception {        
        URI uri = URI.create(getServer().getBaseUrl() + "/sink/");
        System.out.println(uri);
        
        ReactiveSink<ServerSentEvent> reactiveSink = new ClientSseSink(uri).buffer(Integer.MAX_VALUE).open();
        
        int numLoops = 1000;
        
        for (int i = 0; i < numLoops; i++) {
            reactiveSink.write(ServerSentEvent.newEvent().data(i + "_" + LARGE_TEXT));
        }
        
        sleep(1000);

        
        Assert.assertTrue(reactiveSink.toString().contains("numSent: 1000"));
        Assert.assertFalse(reactiveSink.toString().contains("numSendErrors: 0"));
        System.out.println(reactiveSink.toString());
        
        
        Client client = ClientBuilder.newClient();
        String result = client.target(uri).request().get(String.class);
        List<Integer> ids = Splitter.on("\r\n").splitToList(result).stream().map(id -> Integer.parseInt(id)).collect(Collectors.toList());
        Collections.sort(ids);
        
        for (int i = 0; i < numLoops; i++) {
            Assert.assertEquals((int) ids.get(i), (int) i);
        }
        
        
        
        client.close();
        reactiveSink.shutdown();
    }
}
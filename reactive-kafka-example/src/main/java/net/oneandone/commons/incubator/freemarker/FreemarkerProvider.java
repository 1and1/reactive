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
package net.oneandone.commons.incubator.freemarker;





import java.io.IOException;

import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.net.URI;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;

import com.google.common.collect.Maps;




@Provider
public class FreemarkerProvider implements MessageBodyWriter<Page> {

    private final FreemarkerRenderer renderer;

    private @Context HttpServletRequest httpReq;
     

    /**
     * constructor
     */
    public FreemarkerProvider() {
        renderer = new FreemarkerRenderer();
    }


    @Override
    public long getSize(Page page, Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return -1;
    }

    @Override
    public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return Page.class.isAssignableFrom(type);
    }
    
    

    @Override
    public void writeTo(Page page,
                        Class<?> type,
                        Type genericType,
                        Annotation[] annotations,
                        MediaType mediaType,
                        MultivaluedMap<String, Object> httpHeaders,
                        OutputStream entityStream) throws IOException, WebApplicationException {

        // get charset and if not present add utf-8 charset
        MediaType contentType = mediaType;
        String charset = mediaType.getParameters().get("charset");
        if (charset == null) {
            charset = "UTF-8";
            Map<String, String> params = Maps.newHashMap(mediaType.getParameters());
            params.put("charset", charset);
            contentType = new MediaType(mediaType.getType(), mediaType.getSubtype(), params);
        }
        httpHeaders.putSingle("Content-Type", contentType.toString());

        renderer.writePage(entityStream, charset, page, httpReq.getUserPrincipal(), URI.create(httpReq.getRequestURL().toString()));
    }
 }

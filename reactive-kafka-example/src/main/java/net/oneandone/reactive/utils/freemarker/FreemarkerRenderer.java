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
package net.oneandone.reactive.utils.freemarker;







import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.InetAddress;
import java.net.URI;
import java.security.Principal;
import java.util.Locale;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import freemarker.cache.ClassTemplateLoader;
import freemarker.template.Configuration;
import freemarker.template.Template;
import freemarker.template.TemplateException;




public class FreemarkerRenderer  {

    private static final Logger LOG = LoggerFactory.getLogger(FreemarkerRenderer.class);
    
    private final Configuration cfg;
    private final InetAddress localhost;

    

    public FreemarkerRenderer() {
        cfg = new Configuration(Configuration.VERSION_2_3_23);

        cfg.setLocale(Locale.US);          
        cfg.setNumberFormat("0.######"); // see http://freemarker.sourceforge.net/docs/app_faq.html#faq_number_grouping

        cfg.setTemplateLoader(new ClassTemplateLoader(this.getClass(), "/"));
        cfg.setURLEscapingCharset("utf-8");

        
        localhost = getLocalAddress();
    }



    public void writePage(OutputStream os, 
                          String charset, 
                          Page page, 
                          Principal principal,
                          URI requestUri) throws IOException {
        
        
        // add some system data
        final Map<String, Object> systemData = Maps.newHashMap();
        systemData.put("_localhost", localhost);
        systemData.put("_requestURL", requestUri);
        if (principal != null) {
            systemData.put("_principal", principal);
        }
        final ImmutableMap<String, Object> data = ImmutableMap.<String, Object>builder().putAll(systemData).putAll(page.getModelMap()).build();

        
        try (Writer writer = new OutputStreamWriter(os, charset)) {
            
            final Template tpl = cfg.getTemplate(page.getPath());
            tpl.process(data, writer);
            tpl.setOutputEncoding("UTF-8");
            
        } catch (TemplateException te) {
            LOG.warn("error occured by processing template " + page.getPath(), te);
            throw new IOException(te); 
        }
    }
    
    

    private InetAddress getLocalAddress() {
        try {
            return InetAddress.getLocalHost();
        } catch (IOException ioe) {
            return null;
        }
    }

 }

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
package net.oneandone.commons.incubator.hammer.http.client;


import java.security.KeyStore;
import java.util.Map;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Configuration;

import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.JerseyClientBuilder;

import net.oneandone.commons.incubator.neo.http.client.AddAppHeaderClientFilter;



public class RestClientBuilder extends ClientBuilder {

    private final JerseyClientBuilder delegate;
    private final AddAppHeaderClientFilter addAppnamefilter;

    public RestClientBuilder() {        
        this.delegate = new JerseyClientBuilder();
        
        // TODO find a good way to fetch the app name and version
        // This should support springtboot apps as well as webapps
        // addAppnamefilter = new AddAppHeaderClientFilter(...); 
        this.addAppnamefilter = new AddAppHeaderClientFilter("X-APP", "", "");
    }
    
  
    public static RestClientBuilder newBuilder() {
        return new RestClientBuilder();
    }
 
    public static Client newClient() {
        return RestClientBuilder.newBuilder().build();
    }
    
    public Client build() {
        return delegate.build()
                       .register(addAppnamefilter);
                     //.register(data swap protection filter); 
                     //.register(...);
    }

    public boolean equals(Object obj) {
        return delegate.equals(obj);
    }

    public RestClientBuilder keyStore(KeyStore keyStore, char[] password) {
        delegate.keyStore(keyStore, password);
        return this;
    }

    public ClientConfig getConfiguration() {
        return delegate.getConfiguration();
    }

    public int hashCode() {
        return delegate.hashCode();
    }

    public RestClientBuilder sslContext(SSLContext sslContext) {
        delegate.sslContext(sslContext);
        return this;
    }

    public RestClientBuilder trustStore(KeyStore trustStore) {
        delegate.trustStore(trustStore);
        return this;
    }

    public RestClientBuilder hostnameVerifier(HostnameVerifier hostnameVerifier) {
        delegate.hostnameVerifier(hostnameVerifier);
        return this;
    }

    public RestClientBuilder property(String name, Object value) {
        delegate.property(name, value);
        return this;
    }

    public RestClientBuilder register(Class<?> componentClass) {
        delegate.register(componentClass);
        return this;
    }

    public RestClientBuilder register(Class<?> componentClass, int priority) {
        delegate.register(componentClass, priority);
        return this;
    }

    public RestClientBuilder register(Class<?> componentClass, Class<?>... contracts) {
        delegate.register(componentClass, contracts);
        return this;
    }

    public RestClientBuilder register(Class<?> componentClass, Map<Class<?>, Integer> contracts) {
        delegate.register(componentClass, contracts);
        return this;
    }

    public RestClientBuilder register(Object component) {
        delegate.register(component);
        return this;
    }

    public RestClientBuilder register(Object component, int priority) {
        delegate.register(component, priority);
        return this;
    }

    public RestClientBuilder register(Object component, Class<?>... contracts) {
        delegate.register(component, contracts);
        return this;
    }

    public RestClientBuilder register(Object component, Map<Class<?>, Integer> contracts) {
        delegate.register(component, contracts);
        return this;
    }

    public RestClientBuilder withConfig(Configuration config) {
        delegate.withConfig(config);
        return this;
    }

    public RestClientBuilder keyStore(KeyStore keyStore, String password) {
        delegate.keyStore(keyStore, password);
        return this;
    }

    public String toString() {
        return delegate.toString();
    }
}
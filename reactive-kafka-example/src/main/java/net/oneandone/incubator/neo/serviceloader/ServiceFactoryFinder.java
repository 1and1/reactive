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
package net.oneandone.incubator.neo.serviceloader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Closeables;

import joptsimple.internal.Strings;


public class ServiceFactoryFinder { 
    private static final Logger LOG = LoggerFactory.getLogger(ServiceFactoryFinder.class);

    
    public static Optional<Object> find(String factoryId) {
        final Optional<ClassLoader> cl = getContextClassLoader(); 
        
        
        // Use the system property first
        try {
            final String factoryClassName = System.getProperty(factoryId);
            if (factoryClassName != null) {
                Optional<Object> obj = cl.map(classloader -> newInstance(factoryClassName, classloader)).orElseGet(() -> newInstance(factoryClassName));
                if (obj.isPresent()) {
                    return obj;
                }
            }
        } catch (RuntimeException re) {
            LOG.warn("Failed to load service " + factoryId + " from a system property", re);
        }

        
        // than service dir
        String serviceId = "META-INF/services/" + factoryId;
        InputStream is = null;
        try {
            is = cl.map(classloader -> classloader.getResourceAsStream(serviceId)).orElseGet(() -> ClassLoader.getSystemResourceAsStream(serviceId));                     
            if (is != null) {
                final BufferedReader rd = new BufferedReader(new InputStreamReader(is, "UTF-8"));
                final String factoryClassName = rd.readLine();
                rd.close();

                if (!Strings.isNullOrEmpty(factoryClassName)) {
                    return cl.map(classloader -> newInstance(factoryClassName, classloader)).orElseGet(() -> newInstance(factoryClassName)); 
                }
            }
            
        } catch (IOException | RuntimeException ex) {
            LOG.warn("Failed to load service " + factoryId + " from " + serviceId, ex);
        } finally {
            Closeables.closeQuietly(is);
        } 
        
        
        // not factory found
        return Optional.empty();
    }
    
    
    
    private static Optional<Object> newInstance(final String className) {
        try {
            return newInstance(Class.forName(className));
        } catch (ClassNotFoundException nfe) {
            LOG.warn("Provider " + className + " could not found", nfe);
            return Optional.empty();
        }
    }
    
    private static Optional<Object> newInstance(final String className, final ClassLoader classLoader) {
        try {
            return newInstance(Class.forName(className, false, classLoader));
        } catch (ClassNotFoundException ex) {
            return newInstance(className);
        }
    }

    private static Optional<Object> newInstance(final Class<?> clazz) {
        try {
            Constructor<?> constr = clazz.getDeclaredConstructor();
            constr.setAccessible(true);
            return Optional.of(constr.newInstance());
        } catch (NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException | RuntimeException e) {
            LOG.warn("Provider " + clazz.getName() + " could not be instantiated", e);
            return Optional.empty();
        }
    }
    
    private static Optional<ClassLoader> getContextClassLoader() {
        
        return AccessController.doPrivileged(
                new PrivilegedAction<Optional<ClassLoader>>() {

                    @Override
                    public Optional<ClassLoader> run() {
                        try {
                            return Optional.of(Thread.currentThread().getContextClassLoader());
                        } catch (SecurityException ex) {
                            LOG.warn("Unable to get context classloader instance", ex);
                            return Optional.empty();
                        }
                    }
                });
    }
}
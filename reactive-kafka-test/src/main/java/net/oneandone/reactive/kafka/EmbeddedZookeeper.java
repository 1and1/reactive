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
package net.oneandone.reactive.kafka;


import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Random;

import kafka.utils.Utils;

import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;


public class EmbeddedZookeeper {

    private final int port;
    private final File snapshotDir;
    private final File logDir;
    private final NIOServerCnxnFactory factory;

   
    public EmbeddedZookeeper(int port) throws IOException {
        this.port = port;
        this.snapshotDir = getTempDir();
        this.logDir = getTempDir();
        this.factory = new NIOServerCnxnFactory();
        factory.configure(new InetSocketAddress("localhost", port), 1024);
   
    }

    public int getPort() {
        return port;
    }
    
    
    public void start() throws IOException {
        try {
            int tickTime = 500;
            factory.startup(new ZooKeeperServer(snapshotDir, logDir, tickTime));
        } catch (InterruptedException e) {
            throw new IOException(e);
        }     
    }
    
    
    /**
     * Shuts down the embedded Zookeeper instance.
     */ 
    public void shutdown() {
        factory.shutdown();
        Utils.rm(snapshotDir);
        Utils.rm(logDir);
    }
    
    public String getConnection() {
        return "localhost:" + port;
    }
    
    
    
    private static File getTempDir() {
        File file = new File(System.getProperty("java.io.tmpdir"), "_test_" + new Random().nextInt(10000000));
        if (!file.mkdirs()) {
            throw new RuntimeException("could not create temp directory: " + file.getAbsolutePath());
        }
        file.deleteOnExit();
        return file;
    }
 }




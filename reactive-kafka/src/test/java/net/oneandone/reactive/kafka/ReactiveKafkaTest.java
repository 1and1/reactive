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
import java.util.Random;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import net.oneandone.reactive.ReactiveSink;



public class ReactiveKafkaTest {

    private static EmbeddedZookeeper zookeeper;
    private static EmbeddedKafka kafka;
    
    
    @BeforeClass
    public static void setUp() throws Exception {
        int zookeeperPort = 8643;
        zookeeper = new EmbeddedZookeeper(zookeeperPort);
        zookeeper.start();
        
        int kafkaPort = 8543;
        kafka = new EmbeddedKafka(ImmutableMap.of("broker.id", Integer.toString(new Random().nextInt(100000)),
                                                  "port", Integer.toString(kafkaPort),
                                                  "log.dirs", new File("kafkalog").getAbsolutePath(),
                                                  "zookeeper.connect", "localhost:" + zookeeperPort));
        kafka.start();
    }

    
    @AfterClass
    public static void tearDown() throws Exception {
        kafka.shutdown();
        zookeeper.shutdown();
    }

    
    @Test
    public void testSimple() throws Exception {

        
        ReactiveSink<ProducerRecord<String, String>> sink = new KafkaSink<String, String>().withProperty("bootstrap.servers", "localhost:" + kafka.getPort())
                                                                                           .withProperty("key.serializer", org.apache.kafka.common.serialization.StringSerializer.class)
                                                                                           .withProperty("value.serializer", org.apache.kafka.common.serialization.StringSerializer.class)
                                                                                           .open();
        sink.write(new ProducerRecord<String, String>("testtopic", "key", "value"));
        
        
        
        /*
        ReactiveSource<ConsumerRecord<String, String>> source =  new KafkaSource<String, String>("testtopic").withProperty("zookeeper.connect", "localhost:" + zookeeper.getPort())
                                                                                                             .withProperty("group.id", "2")
                                                                                                             .open();
        ConsumerRecord<String, String> record = source.read();
        System.out.println(record);*/
    }
}
/*
 * Copyright (c)  2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.siddhi.extension.map.text.sourcemapper;

import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.event.Event;
import io.siddhi.core.query.output.callback.QueryCallback;
import io.siddhi.core.stream.output.StreamCallback;
import io.siddhi.core.util.EventPrinter;
import io.siddhi.core.util.SiddhiTestHelper;
import io.siddhi.core.util.persistence.InMemoryPersistenceStore;
import io.siddhi.core.util.persistence.PersistenceStore;
import io.siddhi.core.util.transport.InMemoryBroker;
import io.siddhi.extension.io.tcp.transport.TCPNettyClient;
import io.siddhi.extension.map.text.sourcemapper.util.HttpTestUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;


/**
 * Test case for input text mapper.
 */
public class TextCustomSourceMapperTestCase {
    private static final Logger log = LogManager.getLogger(TextCustomSourceMapperTestCase.class);
    private int waitTime = 50;
    private int timeout = 30000;
    private AtomicInteger count = new AtomicInteger();

    @BeforeMethod
    public void init() {
        count.set(0);
    }


    @Test
    public void testTextCustomSourceMapper() throws Exception {
        log.info("Test for custom source mapping");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', @map(type='text',fail.on.missing.attribute='true'," +
                "regex.A='(\\w+)\\s([-.0-9]+)',regex.B='volume=([-0-9]+)'," +
                "@attributes(symbol = 'A[1]', price = 'A[2]', volume = 'B'))) " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    switch (count.incrementAndGet()) {
                        case 1:
                            assertEquals(event.getData(1), 55.6f);
                            break;
                        case 2:
                            assertEquals(event.getData(1), 75.6f);
                            break;
                        default:
                            fail();
                    }
                }
            }
        });

        siddhiAppRuntime.start();

        InMemoryBroker.publish("stock", "wso2 55.6 volume=45");
        InMemoryBroker.publish("stock", "IBM 75.6 volume=45");
        SiddhiTestHelper.waitForEvents(waitTime, 2, count, timeout);
        //assert event count
        assertEquals(count.get(), 2);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testTextCustomSourceMapperOnBinaryMessage() throws Exception {
        log.info("Test for custom source mapping");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', @map(type='text',fail.on.missing.attribute='true'," +
                "regex.A='(\\w+)\\s([-.0-9]+)',regex.B='volume=([-0-9]+)'," +
                "@attributes(symbol = 'A[1]', price = 'A[2]', volume = 'B'))) " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    switch (count.incrementAndGet()) {
                        case 1:
                            assertEquals(event.getData(1), 55.6f);
                            break;
                        case 2:
                            assertEquals(event.getData(1), 75.6f);
                            break;
                        default:
                            fail();
                    }
                }
            }
        });

        byte[] event1 = "wso2 55.6 volume=45".getBytes(StandardCharsets.UTF_8);
        byte[] event2 = "IBM 75.6 volume=45".getBytes(StandardCharsets.UTF_8);

        siddhiAppRuntime.start();

        InMemoryBroker.publish("stock", event1);
        InMemoryBroker.publish("stock", event2);
        SiddhiTestHelper.waitForEvents(waitTime, 2, count, timeout);
        //assert event count
        assertEquals(count.get(), 2);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testTextCustomSourceMapperOneRegex() throws Exception {
        log.info("Test for custom mapping for regex.");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', @map(type='text',fail.on.missing.attribute='true'," +
                "regex.A='(\\w+)\\s([-.0-9]+):([-.0-9]+)'," +
                "@attributes(symbol = 'A[1]', price = 'A[2]', volume = 'A[3]'))) " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    switch (count.incrementAndGet()) {
                        case 1:
                            assertEquals(event.getData(1), 55.6f);
                            break;
                        case 2:
                            assertEquals(event.getData(1), 75.6f);
                            break;
                        default:
                            fail();
                    }
                }
            }
        });

        siddhiAppRuntime.start();

        InMemoryBroker.publish("stock", "wso2 55.6:45");
        InMemoryBroker.publish("stock", "IBM 75.6:45");
        SiddhiTestHelper.waitForEvents(waitTime, 2, count, timeout);
        //assert event count
        assertEquals(count.get(), 2);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testTextCustomSourceMapperNoRegex() throws Exception {
        log.info("Test for custom mapping for regex.");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', @map(type='text',fail.on.missing.attribute='true'," +
                "@attributes(symbol = 'A[1]', price = 'A[2]', volume = 'A[3]'))) " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    count.incrementAndGet();
                }
            }
        });

        siddhiAppRuntime.start();

        InMemoryBroker.publish("stock", "wso2 55.6:45");
        InMemoryBroker.publish("stock", "IBM 75.6:45");
        SiddhiTestHelper.waitForEvents(waitTime, 0, count, timeout);
        //assert event count
        assertEquals(count.get(), 0);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testTextCustomSourceMapperSpecialCharacters() throws Exception {
        log.info("Test for events with special charaters.");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', @map(type='text',fail.on.missing.attribute='true'," +
                "regex.A='(.{0,})\\s([-.0-9]+):([-.0-9]+)'," +
                "@attributes(symbol = 'A[1]', price = 'A[2]', volume = 'A[3]'))) " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    switch (count.incrementAndGet()) {
                        case 1:
                            assertEquals(event.getData(1), 55.6f);
                            break;
                        case 2:
                            assertEquals(event.getData(1), 75.6f);
                            break;
                        default:
                            fail();
                    }
                }
            }
        });

        siddhiAppRuntime.start();

        InMemoryBroker.publish("stock", "w#@so2 55.6:45");
        InMemoryBroker.publish("stock", "I&BM 75.6:45");
        SiddhiTestHelper.waitForEvents(waitTime, 2, count, timeout);
        //assert event count
        assertEquals(count.get(), 2);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testTextCustomSourceMapperTcp() throws Exception {
        log.info("Test for events with special charaters.");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='tcp', topic='stock', @map(type='text',fail.on.missing.attribute='true'," +
                "regex.A='(.{0,})\\s([-.0-9]+):([-.0-9]+)'," +
                "@attributes(symbol = 'A[1]', price = 'A[2]', volume = 'A[3]'))) " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    switch (count.incrementAndGet()) {
                        case 1:
                            assertEquals(event.getData(1), 55.6f);
                            break;
                        case 2:
                            assertEquals(event.getData(1), 75.6f);
                            break;
                        default:
                            fail();
                    }
                }
            }
        });

        siddhiAppRuntime.start();

        TCPNettyClient tcpNettyClient = new TCPNettyClient();
        tcpNettyClient.connect("localhost", 9892);

        byte[] event1 = "w#@so2 55.6:45".getBytes(StandardCharsets.UTF_8);
        byte[] event2 = "I&BM 75.6:45".getBytes(StandardCharsets.UTF_8);

        tcpNettyClient.send("TestSiddhiApp/FooStream", "w#@so2 55.6:45".getBytes(StandardCharsets.UTF_8)).await();
        tcpNettyClient.send("TestSiddhiApp/FooStream", "I&BM 75.6:45".getBytes(StandardCharsets.UTF_8)).await();

        SiddhiTestHelper.waitForEvents(waitTime, 2, count, timeout);
        //assert event count
        assertEquals(count.get(), 2);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testTextCustomSourceMapperEventGroup() throws Exception {
        log.info("Test for custom mapping for event grouping");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', @map(type='text',fail.on.missing.attribute='true'," +
                "event.grouping.enabled='true'," +
                "regex.A='(\\w+)\\s([-.0-9]+):([-.0-9]+)'," +
                "@attributes(symbol = 'A[1]', price = 'A[2]', volume = 'A[3]'))) " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    switch (count.incrementAndGet()) {
                        case 1:
                            assertEquals(event.getData(1), 55.6f);
                            break;
                        case 2:
                            assertEquals(event.getData(1), 75.6f);
                            break;
                        case 3:
                            assertEquals(event.getData(1), 65.6f);
                            break;
                        case 4:
                            assertEquals(event.getData(1), 85.6f);
                            break;
                        default:
                            fail();
                    }
                }
            }
        });

        siddhiAppRuntime.start();
        String event1 = "wso2 55.6:45\n" +
                "~~~~~~~~~~\n" +
                "IBM 75.6:45\n";
        String event2 = "IFS 65.6:45\n" +
                "~~~~~~~~~~\n" +
                "MIT 85.6:45";
        InMemoryBroker.publish("stock", event1);
        InMemoryBroker.publish("stock", event2);
        SiddhiTestHelper.waitForEvents(waitTime, 4, count, timeout);
        //assert event count
        assertEquals(count.get(), 4);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void sample() throws Exception {
        log.info("test for sample");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', @map(type='text',fail.on.missing.attribute='true'," +
                "regex.A='houseId:([-.0-9]+),\\nmaxVal:([-.0-9]+),\\nminVal:([-.0-9]+),\\navgVal:([-.0-9]+)'," +
                "@attributes(houseId = 'A[1]', maxVal = 'A[2]', minVal = 'A[3]' ,avgVal='A[4]'))) " +
                "define stream FooStream (houseId int, maxVal float, minVal float, avgVal double); " +
                "define stream BarStream (houseId int, maxVal float, minVal float, avgVal double); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    switch (count.incrementAndGet()) {
                        case 1:
                            assertEquals(event.getData(1), 100f);
                            break;
                        case 2:
                            assertEquals(event.getData(1), 75.6f);
                            break;
                        default:
                            fail();
                    }
                }
            }
        });

        siddhiAppRuntime.start();

        InMemoryBroker.publish("stock", "houseId:1,\nmaxVal:100,\nminVal:0,\navgVal:55.5");
        InMemoryBroker.publish("stock", "houseId:1,\nmaxVal:75.6,\nminVal:0,\navgVal:55.5");
        SiddhiTestHelper.waitForEvents(waitTime, 2, count, timeout);
        //assert event count
        assertEquals(count.get(), 2);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void sampleOnBinaryMessage() throws Exception {
        log.info("test for sample");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', @map(type='text',fail.on.missing.attribute='true'," +
                "regex.A='houseId:([-.0-9]+),\\nmaxVal:([-.0-9]+),\\nminVal:([-.0-9]+),\\navgVal:([-.0-9]+)'," +
                "@attributes(houseId = 'A[1]', maxVal = 'A[2]', minVal = 'A[3]' ,avgVal='A[4]'))) " +
                "define stream FooStream (houseId int, maxVal float, minVal float, avgVal double); " +
                "define stream BarStream (houseId int, maxVal float, minVal float, avgVal double); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    switch (count.incrementAndGet()) {
                        case 1:
                            assertEquals(event.getData(1), 100f);
                            break;
                        case 2:
                            assertEquals(event.getData(1), 75.6f);
                            break;
                        default:
                            fail();
                    }
                }
            }
        });

        siddhiAppRuntime.start();

        byte[] event1 = "houseId:1,\nmaxVal:100,\nminVal:0,\navgVal:55.5".getBytes(StandardCharsets.UTF_8);
        byte[] event2 = "houseId:1,\nmaxVal:75.6,\nminVal:0,\navgVal:55.5".getBytes(StandardCharsets.UTF_8);

        InMemoryBroker.publish("stock", event1);
        InMemoryBroker.publish("stock", event2);
        SiddhiTestHelper.waitForEvents(waitTime, 2, count, timeout);
        //assert event count
        assertEquals(count.get(), 2);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testTextCustomSourceWrongRegexGroup() throws Exception {
        log.info("Test for applying wrong regex group.");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', @map(type='text',fail.on.missing.attribute='true'," +
                "regex.A='(\\w+)\\s([-.0-9]+)'," +
                "@attributes(symbol = 'A[1]', price = 'A[2]', volume = 'D'))) " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
            }
        });

        siddhiAppRuntime.start();

        InMemoryBroker.publish("stock", "wso2 55.6 45");
        InMemoryBroker.publish("stock", "IBM 75.6 45");
        SiddhiTestHelper.waitForEvents(waitTime, 0, count, timeout);
        //assert event count
        assertEquals(count.get(), 0);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testTextCustomSourceMapperFailOnMissingFalse() throws Exception {
        log.info("Test for fail.on.missing attribute false");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', @map(type='text',fail.on.missing.attribute='false'," +
                "regex.A='(\\w+)\\s([-.0-9]+)',regex.B='volume=([-0-9]+)'," +
                "@attributes(symbol = 'A[1]', price = 'A[2]', volume = 'B'))) " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    switch (count.incrementAndGet()) {
                        case 1:
                            assertEquals(event.getData(1), null);
                            break;
                        case 2:
                            assertEquals(event.getData(1), null);
                            break;
                        default:
                            fail();
                    }
                }
            }
        });

        siddhiAppRuntime.start();

        InMemoryBroker.publish("stock", "55.6 volume=45");
        InMemoryBroker.publish("stock", "75.6 volume=45");
        SiddhiTestHelper.waitForEvents(waitTime, 2, count, timeout);
        //assert event count
        assertEquals(count.get(), 2);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testTextCustomSourceWrongRegexGroupIdx() throws Exception {
        log.info("Test for wrong regex group ID.");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', @map(type='text',fail.on.missing.attribute='true'," +
                "regex.A='(\\w+)\\s([-.0-9]+)',regex.B='volume=([-0-9]+)'," +
                "@attributes(symbol = 'A[4]', price = 'A[3]', volume = 'B'))) " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
            }
        });

        siddhiAppRuntime.start();

        InMemoryBroker.publish("stock", "wso2 55.6 volume=45");
        InMemoryBroker.publish("stock", "IBM 75.6 volume=45");
        SiddhiTestHelper.waitForEvents(waitTime, 0, count, timeout);
        //assert event count
        assertEquals(count.get(), 0);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testTextCustomSourceWithoutAttribute() throws Exception {
        //this will consider as default mapping and drop event if not default format
        log.info("Test for custom mapping without attributes.");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', @map(type='text'," +
                "regex.A='(\\w+)\\s([-.0-9]+)',regex.B='volume=([-0-9]+)')) " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
            }
        });

        siddhiAppRuntime.start();

        InMemoryBroker.publish("stock", "wso2 55.6 volume=45");
        InMemoryBroker.publish("stock", "IBM 75.6 volume=45");
        SiddhiTestHelper.waitForEvents(waitTime, 0, count, timeout);
        //assert event count
        assertEquals(count.get(), 0);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testTextCustomSourceMapperDifferentEventOrder() throws Exception {
        log.info("Test for different event order present and it will effected for regex");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', @map(type='text',fail.on.missing.attribute='true'," +
                "regex.A='(\\w+)\\s([-.0-9]+)',regex.B='volume=([-0-9]+)'," +
                "@attributes(symbol = 'A[1]', price = 'A[2]', volume = 'B'))) " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
            }
        });

        siddhiAppRuntime.start();

        InMemoryBroker.publish("stock", " 55.6 wso2 volume=45");
        InMemoryBroker.publish("stock", "75.6 IBM volume=45");
        SiddhiTestHelper.waitForEvents(waitTime, 0, count, timeout);
        //assert event count
        assertEquals(count.get(), 0);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testTextCustomSourceMapperDifferentEventOrder2() throws Exception {
        log.info("Test for different event order present and it will not effected for regex");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', @map(type='text',fail.on.missing.attribute='true'," +
                "regex.A='(\\w+)\\s([-.0-9]+)',regex.B='volume=([-0-9]+)'," +
                "@attributes(symbol = 'A[1]', price = 'A[2]', volume = 'B'))) " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    switch (count.incrementAndGet()) {
                        case 1:
                            assertEquals(event.getData(1), 55.6f);
                            break;
                        case 2:
                            assertEquals(event.getData(1), 75.6f);
                            break;
                        default:
                            fail();
                    }
                }
            }
        });

        siddhiAppRuntime.start();

        InMemoryBroker.publish("stock", "volume=45 wso2 55.6");
        InMemoryBroker.publish("stock", "volume=45 IBM 75.6");
        SiddhiTestHelper.waitForEvents(waitTime, 2, count, timeout);
        //assert event count
        assertEquals(count.get(), 2);
        siddhiAppRuntime.shutdown();
    }

    /**
     * Creating test for publishing events with Text mapping.
     *
     * @throws Exception Interrupted exception
     */
    @Test
    public void testTextMappingSingleCustom() throws Exception {
        AtomicInteger eventCount = new AtomicInteger(0);
        log.info("Creating test for publishing events with Text mapping through http.");
        URI baseURI = URI.create(String.format("http://%s:%d", "localhost", 8005));
        List<String> receivedEventNameList = new ArrayList<>(2);
        PersistenceStore persistenceStore = new InMemoryPersistenceStore();
        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(persistenceStore);
        siddhiManager.setExtension("text", TextSourceMapper.class);
        String inStreamDefinition = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='http', receiver.url='http://localhost:8005/endpoints/RecPro', " +
                "@map(type='text',fail" +
                ".on.missing.attribute='true'," +
                "regex.A='(\\w+)\\s([-.0-9]+)',regex.B='volume=([-0-9]+)'," +
                "@attributes(symbol = 'A[1]', price = 'A[2]', volume = 'B'))) " +
                "define stream inputStream (symbol string, price float, volume long); " +
                "define stream outputStream (symbol string, price float, volume long); ";
        String query = (
                "@info(name = 'query') "
                        + "from inputStream "
                        + "select *  "
                        + "insert into outputStream;"
        );
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager
                .createSiddhiAppRuntime(inStreamDefinition + query);

        siddhiAppRuntime.addCallback("query", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event event : inEvents) {
                    eventCount.incrementAndGet();
                    receivedEventNameList.add(event.getData(0).toString());
                }
            }
        });
        siddhiAppRuntime.start();

        // publishing events
        List<String> expected = new ArrayList<>(2);
        expected.add("wso2");
        expected.add("IBM");
        String event1 = "volume=45 wso2 55.6";
        String event2 = "volume=45 IBM 55.6";
        HttpTestUtil.httpPublishEvent(event1, baseURI);
        HttpTestUtil.httpPublishEvent(event2, baseURI);
        SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);
        Assert.assertEquals(receivedEventNameList.toString(), expected.toString());
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testTextCustomSourceMapper2() throws Exception {
        log.info("Test for custom source mapping2");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', @map(type='text', " +
                "regex.A='(\\w+)\\s([-.0-9]+)',regex.B='volume=([-0-9]+)'," +
                "@attributes('A[1]', 'A[2]', 'B'))) " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    switch (count.incrementAndGet()) {
                        case 1:
                            assertEquals(event.getData(1), 55.6f);
                            break;
                        case 2:
                            assertEquals(event.getData(1), 75.6f);
                            break;
                        default:
                            fail();
                    }
                }
            }
        });

        siddhiAppRuntime.start();

        InMemoryBroker.publish("stock", "wso2 55.6 volume=45");
        InMemoryBroker.publish("stock", "IBM 75.6 volume=45");
        SiddhiTestHelper.waitForEvents(waitTime, 2, count, timeout);
        //assert event count
        assertEquals(count.get(), 2);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testTextCustomSourceMapper3() throws Exception {
        log.info("Test for custom source mapping3");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='testTrpInMemory', prop1='foo', prop2='bar', topic='stock', @map(type='text', " +
                "regex.A='(\\w+)\\s([-.0-9]+)',regex.B='volume=([-0-9]+)'," +
                "@attributes('trp:symbol', 'A[2]', 'B'))) " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    Assert.assertEquals("foo", event.getData(0));
                    switch (count.incrementAndGet()) {
                        case 1:
                            assertEquals(event.getData(1), 55.6f);
                            break;
                        case 2:
                            assertEquals(event.getData(1), 75.6f);
                            break;
                        default:
                            fail();
                    }
                }
            }
        });

        siddhiAppRuntime.start();

        InMemoryBroker.publish("stock", "wso2 55.6 volume=45");
        InMemoryBroker.publish("stock", "IBM 75.6 volume=45");
        SiddhiTestHelper.waitForEvents(waitTime, 2, count, timeout);
        //assert event count
        assertEquals(count.get(), 2);
        siddhiAppRuntime.shutdown();
    }


    @Test
    public void testTextCustomSourceMapper4() throws Exception {
        log.info("Test for custom source mapping4");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='testTrpInMemory', prop1='foo', prop2='bar', topic='stock', @map(type='text', " +
                "regex.A='(\\w+)\\s([-.0-9]+)',regex.B='volume=([-0-9]+)'," +
                "@attributes(symbol='trp:symbol', price='A[2]', volume='B'))) " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);

        siddhiAppRuntime.addCallback("BarStream", new StreamCallback() {

            @Override
            public void receive(Event[] events) {
                EventPrinter.print(events);
                for (Event event : events) {
                    Assert.assertEquals("foo", event.getData(0));
                    switch (count.incrementAndGet()) {
                        case 1:
                            assertEquals(event.getData(1), 55.6f);
                            break;
                        case 2:
                            assertEquals(event.getData(1), 75.6f);
                            break;
                        default:
                            fail();
                    }
                }
            }
        });

        siddhiAppRuntime.start();

        InMemoryBroker.publish("stock", "wso2 55.6 volume=45");
        InMemoryBroker.publish("stock", "IBM 75.6 volume=45");
        SiddhiTestHelper.waitForEvents(waitTime, 2, count, timeout);
        //assert event count
        assertEquals(count.get(), 2);
        siddhiAppRuntime.shutdown();
    }
}

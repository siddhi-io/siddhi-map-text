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

package org.wso2.extension.siddhi.map.text.sourcemapper;

import org.apache.log4j.Logger;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.exception.SiddhiAppCreationException;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.core.util.SiddhiTestHelper;
import org.wso2.siddhi.core.util.transport.InMemoryBroker;

import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;


/**
 * Test case for input text mapper.
 */
public class TextCustomSourceMapperTestCase {
    private static final Logger log = Logger.getLogger(TextCustomSourceMapperTestCase.class);
    private int waitTime = 50;
    private int timeout = 30000;
    private AtomicInteger count = new AtomicInteger();

    @BeforeMethod
    public void init() {
        count.set(0);
    }


    @Test
    public void testTextCustomSourceMapper() throws InterruptedException {
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
    public void testTextCustomSourceMapperOneRegex() throws InterruptedException {
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
    public void testTextCustomSourceMapperNoRegex() throws InterruptedException {
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
    public void testTextCustomSourceMapperSpecialCharacters() throws InterruptedException {
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

    @Test(expectedExceptions = SiddhiAppCreationException.class)
    public void testTextCustomSourceMapperTcp() throws InterruptedException {
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

        InMemoryBroker.publish("stock", "w#@so2 55.6:45");
        InMemoryBroker.publish("stock", "I&BM 75.6:45");
        SiddhiTestHelper.waitForEvents(waitTime, 2, count, timeout);
        //assert event count
        assertEquals(count.get(), 2);
        siddhiAppRuntime.shutdown();
    }
    @Test
    public void testTextCustomSourceMapperEventGroup() throws InterruptedException {
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
    public void sample() throws InterruptedException {
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
    public void testTextCustomSourceWrongRegexGroup() throws InterruptedException {
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
    public void testTextCustomSourceMapperFailOnMissingFalse() throws InterruptedException {
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
    public void testTextCustomSourceWrongRegexGroupIdx() throws InterruptedException {
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
    public void testTextCustomSourceWithoutAttribute() throws InterruptedException {
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
    public void testTextCustomSourceMapperDifferentEventOrder() throws InterruptedException {
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
    public void testTextCustomSourceMapperDifferentEventOrder2() throws InterruptedException {
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
}

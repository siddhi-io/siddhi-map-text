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
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.extension.siddhi.map.text.sourcemapper.util.HttpTestUtil;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.core.util.SiddhiTestHelper;
import org.wso2.siddhi.core.util.persistence.InMemoryPersistenceStore;
import org.wso2.siddhi.core.util.persistence.PersistenceStore;
import org.wso2.siddhi.core.util.transport.InMemoryBroker;

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
public class TextDefaultSourceMapperTestCase {
    private static final Logger log = Logger.getLogger(TextDefaultSourceMapperTestCase.class);
    private int waitTime = 50;
    private int timeout = 30000;
    private AtomicInteger count = new AtomicInteger();

    @BeforeMethod
    public void init() {
        count.set(0);
    }

    /**
     * DASC5-1113:Configure 'in-Memory' event receiver with default text mapping.
     * @throws InterruptedException the InterruptedException.
     */
    @Test
    public void  defaultTextMapping() throws InterruptedException {
        log.info("Test for default text mapping");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', @map(type='text',fail.on.missing.attribute='true')) " +
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
        String event1 = "symbol:\"WSO2\",\n" +
                "price:55.6,\n" +
                "volume:100";

        String event2 = "symbol:\"IBM\",\n" +
                "price:75.6,\n" +
                "volume:10";

        InMemoryBroker.publish("stock", event1);
        InMemoryBroker.publish("stock", event2);
        SiddhiTestHelper.waitForEvents(waitTime, 2, count, timeout);
        //assert event count
        assertEquals(count.get(), 2);
        siddhiAppRuntime.shutdown();
    }

    /**
     * DASC5-1113:Configure 'in-Memory' event receiver with default text mapping.
     * @throws InterruptedException the InterruptedException.
     */
    @Test
    public void  defaultTextMappingAttributeOrderDifferent() throws InterruptedException {
        log.info("Test for default text mapping with attribute oder of event is different.");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', @map(type='text',fail.on.missing.attribute='true')) " +
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
        String event1 = "price:55.6,\n" +
                        "symbol:\"WSO2\",\n" +
                        "volume:100";

        String event2 = "volume:10,\n" +
                "symbol:\"IBM\",\n" +
                "price:75.6";

        InMemoryBroker.publish("stock", event1);
        InMemoryBroker.publish("stock", event2);
        SiddhiTestHelper.waitForEvents(waitTime, 2, count, timeout);
        //assert event count
        assertEquals(count.get(), 2);
        siddhiAppRuntime.shutdown();
    }
    /**
     * DASC5-1114:Configure event receiver with default text mapping when fail.on.missing.attribute=false.
     * @throws InterruptedException the InterruptedException.
     */
    @Test
    public void defaultTextMappingFallonMissingFalse() throws InterruptedException {
        log.info("test for fail on missing attribute false");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', @map(type='text',fail.on.missing.attribute='false')) " +
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
        String event1 = "symbol:\"WSO2\",\n" +
                "price:55.6,\n" +
                "volume:100";

        String event2 = "symbol:\"IBM\",\n" +
                "price:75.6,\n" +
                "volume:10";
        siddhiAppRuntime.start();
        InMemoryBroker.publish("stock", event1);
        InMemoryBroker.publish("stock", event2);
        SiddhiTestHelper.waitForEvents(waitTime, 2, count, timeout);
        //assert event count
        assertEquals(count.get(), 2);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testTextSourceMapperGroupedEvents() throws InterruptedException {
        log.info("test for group of events");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', @map(type='text', event.grouping.enabled='true')) " +
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
                            assertEquals(event.getData(1), 50.6f);
                            break;
                        case 4:
                            assertEquals(event.getData(1), 70.6f);
                            break;
                        default:
                            fail();
                    }
                }
            }
        });

        siddhiAppRuntime.start();
        String event1 = "symbol:\"wso2\",\n" +
                "price:55.6,\n" +
                "volume:200\n" +
                "~~~~~~~~~~\n" +
                "symbol:\"IBM\",\n" +
                "price:75.6,\n" +
                "volume:100";
        String event2 = "symbol:\"IFS\",\n" +
                "price:50.6,\n" +
                "volume:200\n" +
                "~~~~~~~~~~\n" +
                "symbol:\"Virtusa\",\n" +
                "price:70.6,\n" +
                "volume:100";
        InMemoryBroker.publish("stock", event1);
        InMemoryBroker.publish("stock", event2);
        SiddhiTestHelper.waitForEvents(waitTime, 4, count, timeout);
        //assert event count
        assertEquals(count.get(), 4);
        siddhiAppRuntime.shutdown();

    }
    @Test
    public void testTextSourceMapperGroupedEventsCustom() throws InterruptedException {
        log.info("Test gropup event with custom delimiter.");
        String streams =
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', @map(type='text', event.grouping.enabled='true'" +
                ",delimiter='####')) " +
                "define stream FooStream (symbol string, price float, volume long); " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query =
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
                            assertEquals(event.getData(1), 50.6f);
                            break;
                        case 4:
                            assertEquals(event.getData(1), 70.6f);
                            break;
                        default:
                            fail();
                    }
                }
            }
        });

        siddhiAppRuntime.start();
        String event1 = "symbol:\"wso2\",\n" +
                "price:55.6,\n" +
                "volume:200\n" +
                "####\n" +
                "symbol:\"IBM\",\n" +
                "price:75.6,\n" +
                "volume:100";
        String event2 = "symbol:\"IFS\",\n" +
                "price:50.6,\n" +
                "volume:200\n" +
                "####\n" +
                "symbol:\"Virtusa\",\n" +
                "price:70.6,\n" +
                "volume:100";
        InMemoryBroker.publish("stock", event1);
        InMemoryBroker.publish("stock", event2);
        SiddhiTestHelper.waitForEvents(waitTime, 4, count, timeout);
        //assert event count
        assertEquals(count.get(), 4);
        siddhiAppRuntime.shutdown();

    }

    @Test
    public void testTextSourceMapperGroupedEventsCustomNewline() throws InterruptedException {
        log.info("Test gropup event with custom delimiter.");
        String streams =
                "@App:name('TestSiddhiApp')" +
                        "@source(type='inMemory', topic='stock', @map(type='text', event.grouping.enabled='true'" +
                        ",delimiter='####',new.line.character='\\n\\r')) " +
                        "define stream FooStream (symbol string, price float, volume long); " +
                        "define stream BarStream (symbol string, price float, volume long); ";

        String query =
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
        String event1 = "symbol:\"wso2\",\n" +
                "price:55.6,\n" +
                "volume:200\n" +
                "####\n" +
                "symbol:\"IBM\",\n" +
                "price:75.6,\n" +
                "volume:100";
        String event2 = "symbol:\"IFS\",\n" +
                "price:50.6,\n" +
                "volume:200\n" +
                "####\n" +
                "symbol:\"Virtusa\",\n" +
                "price:70.6,\n" +
                "volume:100";
        InMemoryBroker.publish("stock", event1);
        InMemoryBroker.publish("stock", event2);
        SiddhiTestHelper.waitForEvents(waitTime, 0, count, timeout);
        //assert event count
        assertEquals(count.get(), 0);
        siddhiAppRuntime.shutdown();

    }
    @Test
    public void testTextSourceMapperMissingAttribute() throws InterruptedException {
        log.info("Test for missing attributes.");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', @map(type='text', event.grouping.enabled='true'" +
                ",delimiter='####')) " +
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
                            assertEquals(event.getData(1), 50.6f);
                            break;
                        case 2:
                            assertEquals(event.getData(1), 70.6f);
                            break;
                        default:
                            fail();
                    }
                }
            }
        });

        siddhiAppRuntime.start();
        String event1 = "symbol:\"wso2\",\n" +
                "price:55.6\n" +
                "####\n" +
                "symbol:\"IBM\",\n" +
                "price:75.6";
        InMemoryBroker.publish("stock", event1);
        SiddhiTestHelper.waitForEvents(waitTime, 0, count, timeout);
        //assert event count
        assertEquals(count.get(), 0);
        siddhiAppRuntime.shutdown();

    }
    @Test
    public void testTextSourceMapperExtraMapping() throws InterruptedException {
        log.info("Test for extra mapping present.");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', @map(type='text', event.grouping.enabled='true'" +
                ",delimiter='####')) " +
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
                            assertEquals(event.getData(1), 50.6f);
                            break;
                        case 2:
                            assertEquals(event.getData(1), 70.6f);
                            break;
                        case 3:
                            assertEquals(event.getData(1), 50.6f);
                            break;
                        case 4:
                            assertEquals(event.getData(1), 70.6f);
                            break;
                        default:
                            fail();
                    }
                }
            }
        });

        siddhiAppRuntime.start();

        String event1 = "symbol:\"IFS\",\n" +
                "price:50.6,\n" +
                "volume:200,\n" +
                "age:20\n" +
                "####\n" +
                "symbol:\"Virtusa\",\n" +
                "price:70.6,\n" +
                "volume:100,\n" +
                "age:20";

        String event2 = "symbol:\"IFS\",\n" +
                "price:50.6,\n" +
                "age:20,\n" +
                "volume:200\n" +
                "####\n" +
                "symbol:\"Virtusa\",\n" +
                "price:70.6,\n" +
                "age:20,\n" +
                "volume:100";

        InMemoryBroker.publish("stock", event1);
        InMemoryBroker.publish("stock", event2);
        SiddhiTestHelper.waitForEvents(waitTime, 4, count, timeout);
        //assert event count
        assertEquals(count.get(), 4);
        siddhiAppRuntime.shutdown();

    }
    @Test
    public void testTextSourceMapperGroupedEventsWithoutEnable() throws InterruptedException {
        log.info("test for present group of events with delimiter when event.grouping is false.");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', @map(type='text', event.grouping.enabled='false')) " +
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
        String event1 = "symbol:\"wso2\",\n" +
                "price:55.6,\n" +
                "volume:200\n" +
                "~~~~~~~~~~\n" +
                "symbol:\"IBM\",\n" +
                "price:75.6,\n" +
                "volume:100";
        String event2 = "symbol:\"IFS\",\n" +
                "price:50.6,\n" +
                "volume:200\n" +
                "~~~~~~~~~~\n" +
                "symbol:\"Virtusa\",\n" +
                "price:70.6,\n" +
                "volume:100";
        InMemoryBroker.publish("stock", event1);
        InMemoryBroker.publish("stock", event2);
        SiddhiTestHelper.waitForEvents(waitTime, 0, count, timeout);
        //assert event count
        assertEquals(count.get(), 0);
        siddhiAppRuntime.shutdown();

    }
    @Test
    public void testTextSourceMapperGroupedEventsWithoutEnable2() throws InterruptedException {
        log.info("test for present group of events when event.grouping is false.");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', @map(type='text', event.grouping.enabled='false')) " +
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
                            assertEquals(event.getData(1), 50.6f);
                            break;
                        default:
                            fail();
                    }
                }
            }
        });

        siddhiAppRuntime.start();
        String event1 = "symbol:\"wso2\",\n" +
                "price:55.6,\n" +
                "price:58.6,\n" +
                "volume:200";
        String event2 = "symbol:\"IFS\",\n" +
                "price:50.6,\n" +
                "volume:200,\n" +
                "symbol:\"IBM\"";

        InMemoryBroker.publish("stock", event1);
        InMemoryBroker.publish("stock", event2);
        SiddhiTestHelper.waitForEvents(waitTime, 2, count, timeout);
        //assert event count
        assertEquals(count.get(), 2);
        siddhiAppRuntime.shutdown();

    }
@Test
public void testTextSourceMapperSingleEventForEventGroup() throws InterruptedException {
    log.info("test testTextSourceMapperSingleEventForEventGroup");
    String streams = "" +
            "@App:name('TestSiddhiApp')" +
            "@source(type='inMemory', topic='stock', @map(type='text', event.grouping.enabled='true')) " +
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
                        assertEquals(event.getData(1), 50.6f);
                        break;
                    default:
                        fail();
                }
            }
        }
    });

    siddhiAppRuntime.start();
    String event1 = "symbol:\"wso2\",\n" +
            "price:55.6,\n" +
            "volume:200";
    String event2 = "symbol:\"IFS\",\n" +
            "price:50.6,\n" +
            "volume:200";
    InMemoryBroker.publish("stock", event1);
    InMemoryBroker.publish("stock", event2);
    SiddhiTestHelper.waitForEvents(waitTime, 2, count, timeout);
    //assert event count
    assertEquals(count.get(), 2);
    siddhiAppRuntime.shutdown();

}
    @Test
    public void  sampleTest() throws InterruptedException {
        log.info("test default mapping for sample");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', @map(type='text')) " +
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
                            assertEquals(event.getData(1), 200f);
                            break;
                        case 2:
                            assertEquals(event.getData(1), 100f);
                            break;
                        default:
                            fail();
                    }
                }
            }
        });

        siddhiAppRuntime.start();
        String event1 = "houseId:1,\nmaxVal:200f,\nminVal:8,\navgVal:100";

        String event2 = "houseId:1,\nmaxVal:100f,\nminVal:8,\navgVal:100";

        InMemoryBroker.publish("stock", event1);
        InMemoryBroker.publish("stock", event2);
        SiddhiTestHelper.waitForEvents(waitTime, 2, count, timeout);
        //assert event count
        assertEquals(count.get(), 2);
        siddhiAppRuntime.shutdown();
    }

    @Test
    public void testTextSourceMapperAttributeSameName() throws InterruptedException {
        log.info("test multiple time same attrbute is pressent.");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', @map(type='text', event.grouping.enabled='false')) " +
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
                            assertEquals(event.getData(1), 50.6f);
                            break;
                        default:
                            fail();
                    }
                }
            }
        });

        siddhiAppRuntime.start();
        String event1 = "symbol:\"wso2\",\n" +
                "price:55.6,\n" +
                "price:58.6,\n" +
                "volume:200";
        String event2 = "symbol:\"IFS\",\n" +
                "price:50.6,\n" +
                "volume:200,\n" +
                "symbol:\"IBM\"";

        InMemoryBroker.publish("stock", event1);
        InMemoryBroker.publish("stock", event2);
        SiddhiTestHelper.waitForEvents(waitTime, 2, count, timeout);
        //assert event count
        assertEquals(count.get(), 2);
        siddhiAppRuntime.shutdown();

    }

    @Test
    public void testTextSourceMapperGroupedEventsLargeGroup() throws InterruptedException {
        log.info("test for large event group.");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', @map(type='text', event.grouping.enabled='true')) " +
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
                            assertEquals(event.getData(0), "A");
                            break;
                        case 2:
                            assertEquals(event.getData(0), "B");
                            break;
                        case 3:
                            assertEquals(event.getData(0), "C");
                            break;
                        case 4:
                            assertEquals(event.getData(0), "D");
                            break;
                        case 5:
                            assertEquals(event.getData(0), "E");
                            break;
                        case 6:
                            assertEquals(event.getData(0), "F");
                            break;
                        case 7:
                            assertEquals(event.getData(0), "G");
                            break;
                        case 8:
                            assertEquals(event.getData(0), "H");
                            break;
                        case 9:
                            assertEquals(event.getData(0), "I");
                            break;
                        case 10:
                            assertEquals(event.getData(0), "J");
                            break;
                        case 11:
                            assertEquals(event.getData(0), "K");
                            break;
                        case 12:
                            assertEquals(event.getData(0), "L");
                            break;
                        case 13:
                            assertEquals(event.getData(0), "M");
                            break;
                        case 14:
                            assertEquals(event.getData(0), "N");
                            break;
                        case 15:
                            assertEquals(event.getData(0), "O");
                            break;
                        case 16:
                            assertEquals(event.getData(0), "P");
                            break;
                        default:
                            fail();
                    }
                }
            }
        });

        siddhiAppRuntime.start();
        String event1 = "symbol:\"A\",\n" +
                "price:55.6,\n" +
                "volume:200\n" +
                "~~~~~~~~~~\n" +
                "symbol:\"B\",\n" +
                "price:75.6,\n" +
                "volume:100\n" +
                "~~~~~~~~~~\n" +
                "symbol:\"C\",\n" +
                "price:75.6,\n" +
                "volume:100\n" +
                "~~~~~~~~~~\n" +
                "symbol:\"D\",\n" +
                "price:75.6,\n" +
                "volume:100\n" +
                "~~~~~~~~~~\n" +
                "symbol:\"E\",\n" +
                "price:55.6,\n" +
                "volume:200\n" +
                "~~~~~~~~~~\n" +
                "symbol:\"F\",\n" +
                "price:75.6,\n" +
                "volume:100\n" +
                "~~~~~~~~~~\n" +
                "symbol:\"G\",\n" +
                "price:75.6,\n" +
                "volume:100\n" +
                "~~~~~~~~~~\n" +
                "symbol:\"H\",\n" +
                "price:75.6,\n" +
                "volume:100\n" +
                "~~~~~~~~~~\n" +
                "symbol:\"I\",\n" +
                "price:55.6,\n" +
                "volume:200\n" +
                "~~~~~~~~~~\n" +
                "symbol:\"J\",\n" +
                "price:75.6,\n" +
                "volume:100\n" +
                "~~~~~~~~~~\n" +
                "symbol:\"K\",\n" +
                "price:75.6,\n" +
                "volume:100\n" +
                "~~~~~~~~~~\n" +
                "symbol:\"L\",\n" +
                "price:75.6,\n" +
                "volume:100\n" +
                "~~~~~~~~~~\n" +
                "symbol:\"M\",\n" +
                "price:55.6,\n" +
                "volume:200\n" +
                "~~~~~~~~~~\n" +
                "symbol:\"N\",\n" +
                "price:75.6,\n" +
                "volume:100\n" +
                "~~~~~~~~~~\n" +
                "symbol:\"O\",\n" +
                "price:75.6,\n" +
                "volume:100\n" +
                "~~~~~~~~~~\n" +
                "symbol:\"P\",\n" +
                "price:75.6,\n" +
                "volume:100";
        InMemoryBroker.publish("stock", event1);
        SiddhiTestHelper.waitForEvents(waitTime, 16, count, timeout);
        //assert event count
        assertEquals(count.get(), 16);
        siddhiAppRuntime.shutdown();

    }

    @Test
    public void testTextSourceMapperGroupedEventsLargeGroupOnbinaryMessage() throws InterruptedException {
        log.info("test for large event group.");
        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "@source(type='inMemory', topic='stock', @map(type='text', event.grouping.enabled='true')) " +
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
                        assertEquals(event.getData(0), "A");
                        break;
                    case 2:
                        assertEquals(event.getData(0), "B");
                        break;
                    case 3:
                        assertEquals(event.getData(0), "C");
                        break;
                    case 4:
                        assertEquals(event.getData(0), "D");
                        break;
                    case 5:
                        assertEquals(event.getData(0), "E");
                        break;
                    case 6:
                        assertEquals(event.getData(0), "F");
                        break;
                    case 7:
                        assertEquals(event.getData(0), "G");
                        break;
                    case 8:
                        assertEquals(event.getData(0), "H");
                        break;
                    case 9:
                        assertEquals(event.getData(0), "I");
                        break;
                    case 10:
                        assertEquals(event.getData(0), "J");
                        break;
                    case 11:
                        assertEquals(event.getData(0), "K");
                        break;
                    case 12:
                        assertEquals(event.getData(0), "L");
                        break;
                    case 13:
                        assertEquals(event.getData(0), "M");
                        break;
                    case 14:
                        assertEquals(event.getData(0), "N");
                        break;
                    case 15:
                        assertEquals(event.getData(0), "O");
                        break;
                    case 16:
                        assertEquals(event.getData(0), "P");
                        break;
                    default:
                        fail();
                    }
                }
            }
        });

        siddhiAppRuntime.start();
        String event1 = "symbol:\"A\",\n" +
                "price:55.6,\n" +
                "volume:200\n" +
                "~~~~~~~~~~\n" +
                "symbol:\"B\",\n" +
                "price:75.6,\n" +
                "volume:100\n" +
                "~~~~~~~~~~\n" +
                "symbol:\"C\",\n" +
                "price:75.6,\n" +
                "volume:100\n" +
                "~~~~~~~~~~\n" +
                "symbol:\"D\",\n" +
                "price:75.6,\n" +
                "volume:100\n" +
                "~~~~~~~~~~\n" +
                "symbol:\"E\",\n" +
                "price:55.6,\n" +
                "volume:200\n" +
                "~~~~~~~~~~\n" +
                "symbol:\"F\",\n" +
                "price:75.6,\n" +
                "volume:100\n" +
                "~~~~~~~~~~\n" +
                "symbol:\"G\",\n" +
                "price:75.6,\n" +
                "volume:100\n" +
                "~~~~~~~~~~\n" +
                "symbol:\"H\",\n" +
                "price:75.6,\n" +
                "volume:100\n" +
                "~~~~~~~~~~\n" +
                "symbol:\"I\",\n" +
                "price:55.6,\n" +
                "volume:200\n" +
                "~~~~~~~~~~\n" +
                "symbol:\"J\",\n" +
                "price:75.6,\n" +
                "volume:100\n" +
                "~~~~~~~~~~\n" +
                "symbol:\"K\",\n" +
                "price:75.6,\n" +
                "volume:100\n" +
                "~~~~~~~~~~\n" +
                "symbol:\"L\",\n" +
                "price:75.6,\n" +
                "volume:100\n" +
                "~~~~~~~~~~\n" +
                "symbol:\"M\",\n" +
                "price:55.6,\n" +
                "volume:200\n" +
                "~~~~~~~~~~\n" +
                "symbol:\"N\",\n" +
                "price:75.6,\n" +
                "volume:100\n" +
                "~~~~~~~~~~\n" +
                "symbol:\"O\",\n" +
                "price:75.6,\n" +
                "volume:100\n" +
                "~~~~~~~~~~\n" +
                "symbol:\"P\",\n" +
                "price:75.6,\n" +
                "volume:100";

        byte[] byteEvent = event1.getBytes(StandardCharsets.UTF_8);
        InMemoryBroker.publish("stock", event1);
        SiddhiTestHelper.waitForEvents(waitTime, 16, count, timeout);
        //assert event count
        assertEquals(count.get(), 16);
        siddhiAppRuntime.shutdown();

    }

    /**
     * Creating test for publishing events with Text mapping.
     *
     * @throws Exception Interrupted exception
     */
    @Test
    public void testTextMappingSingle() throws Exception {
        AtomicInteger eventCount = new AtomicInteger(0);
        int waitTime = 50;
        int timeout = 30000;
        log.info("Creating test for publishing events with Text mapping through http.");
        URI baseURI = URI.create(String.format("http://%s:%d", "localhost", 8005));
        List<String> receivedEventNameList = new ArrayList<>(2);
        PersistenceStore persistenceStore = new InMemoryPersistenceStore();
        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(persistenceStore);
        siddhiManager.setExtension("text", TextSourceMapper.class);
        String inStreamDefinition = "" + "@source(type='http',  @map(type='text'), "
                + "receiver.url='http://localhost:8005/endpoints/RecPro', " + "basic.auth.enabled='false'" + ")"
                + "define stream inputStream (name string, age int, country string);";
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
        expected.add("John");
        expected.add("Mike");
        String event1 = "name:\"John\",\n" +
                "age:100,\n" +
                "country:\"USA\"";
        String event2 = "name:\"Mike\",\n" +
                "age:100,\n" +
                "country:\"USA\"";
        HttpTestUtil.httpPublishEvent(event1, baseURI);
        HttpTestUtil.httpPublishEvent(event2, baseURI);
        SiddhiTestHelper.waitForEvents(waitTime, 2, eventCount, timeout);
        Assert.assertEquals(receivedEventNameList.toString(), expected.toString());
        siddhiAppRuntime.shutdown();
    }

    /**
     * Creating test for publishing events with Text mapping.
     *
     * @throws Exception Interrupted exception
     */
    @Test
    public void testTextMappingMultiple() throws Exception {
        AtomicInteger eventCount = new AtomicInteger(0);
        int waitTime = 50;
        int timeout = 30000;
        log.info("Creating test for publishing events with Text mapping through http.");
        URI baseURI = URI.create(String.format("http://%s:%d", "localhost", 8005));
        List<String> receivedEventNameList = new ArrayList<>(2);
        PersistenceStore persistenceStore = new InMemoryPersistenceStore();
        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setPersistenceStore(persistenceStore);
        siddhiManager.setExtension("text", TextSourceMapper.class);
        String inStreamDefinition = "" + "@source(type='http',  @map(type='text',event.grouping.enabled='true'), "
                + "receiver.url='http://localhost:8005/endpoints/RecPro', " + "basic.auth.enabled='false'" + ")"
                + "define stream inputStream (name string, age int, country string);";
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
        List<String> expected = new ArrayList<>(4);
        expected.add("John1");
        expected.add("John2");
        expected.add("Mike1");
        expected.add("Mike2");
        String event1 = "name:\"John1\",\n" +
                "age:100,\n" +
                "country:\"USA\"\n"
                + "~~~~~~~~~~\n"
                + "name:\"John2\",\n" +
                "age:100,\n" +
                "country:\"USA\"";
        String event2 = "name:\"Mike1\",\n" +
                "age:100,\n" +
                "country:\"USA\"\n"
                + "~~~~~~~~~~\n"
                + "name:\"Mike2\",\n" +
                "age:100,\n" +
                "country:\"USA\"";
        HttpTestUtil.httpPublishEvent(event1, baseURI);
        HttpTestUtil.httpPublishEvent(event2, baseURI);
        SiddhiTestHelper.waitForEvents(waitTime, 4, eventCount, timeout);
        Assert.assertEquals(receivedEventNameList.toString(), expected.toString());
        siddhiAppRuntime.shutdown();
    }

}

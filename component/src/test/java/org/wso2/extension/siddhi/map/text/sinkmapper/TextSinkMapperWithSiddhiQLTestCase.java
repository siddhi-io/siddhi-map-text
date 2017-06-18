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

package org.wso2.extension.siddhi.map.text.sinkmapper;

import org.apache.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.wso2.siddhi.core.SiddhiAppRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.exception.NoSuchAttributeException;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.sink.InMemorySink;
import org.wso2.siddhi.core.util.transport.InMemoryBroker;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test Case for QLT type output text mapper.
 */
public class TextSinkMapperWithSiddhiQLTestCase {
    private static final Logger log = Logger.getLogger(TextSinkMapperWithSiddhiQLTestCase.class);
    private AtomicInteger wso2Count = new AtomicInteger(0);
    private AtomicInteger ibmCount = new AtomicInteger(0);

    @BeforeMethod
    public void init() {
        wso2Count.set(0);
        ibmCount.set(0);
    }

    @Test
    public void testTextSinkmapperDefaultMappingWithSiddhiQL() throws InterruptedException {
        log.info("Test default text mapping with SiddhiQL");

        InMemoryBroker.Subscriber subscriberWSO2 = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                wso2Count.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "WSO2";
            }
        };

        InMemoryBroker.Subscriber subscriberIBM = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                ibmCount.incrementAndGet();
            }

            @Override
            public String getTopic() {
                return "IBM";
            }
        };

        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriberWSO2);
        InMemoryBroker.subscribe(subscriberIBM);

        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='inMemory', topic='{{symbol}}', @map(type='text')) " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setExtension("sink:inMemory", InMemorySink.class);
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");

        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
        Thread.sleep(100);

        //assert event count
        Assert.assertEquals(wso2Count.get(), 2);
        Assert.assertEquals(ibmCount.get(), 1);
        siddhiAppRuntime.shutdown();

        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriberWSO2);
        InMemoryBroker.unsubscribe(subscriberIBM);
    }

    @Test
    public void testTextOutputCustomMappingWithSiddhiQL() throws InterruptedException {
        log.info("Test custom text mapping with SiddhiQL");
        List<Object> onMessageList = new ArrayList<Object>();

        InMemoryBroker.Subscriber subscriberWSO2 = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                wso2Count.incrementAndGet();
                onMessageList.add(msg);
            }

            @Override
            public String getTopic() {
                return "WSO2";
            }
        };

        InMemoryBroker.Subscriber subscriberIBM = new InMemoryBroker.Subscriber() {
            @Override
            public void onMessage(Object msg) {
                ibmCount.incrementAndGet();
                onMessageList.add(msg);
            }

            @Override
            public String getTopic() {
                return "IBM";
            }
        };

        //subscribe to "inMemory" broker per topic
        InMemoryBroker.subscribe(subscriberWSO2);
        InMemoryBroker.subscribe(subscriberIBM);

        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='inMemory', topic='{{symbol}}', @map(type='text', @payload(\"Stock price of {{symbol}} is" +
                " {{price}}\"))) " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setExtension("sink:inMemory", InMemorySink.class);
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");

        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
        Thread.sleep(100);

        //assert event count
        Assert.assertEquals(wso2Count.get(), 2);
        Assert.assertEquals(ibmCount.get(), 1);
        //assert custom text
        Assert.assertEquals(onMessageList.get(0).toString(), "Stock price of WSO2 is 55.6");
        Assert.assertEquals(onMessageList.get(1).toString(), "Stock price of IBM is 75.6");
        Assert.assertEquals(onMessageList.get(2).toString(), "Stock price of WSO2 is 57.6");
        siddhiAppRuntime.shutdown();

        //unsubscribe from "inMemory" broker per topic
        InMemoryBroker.unsubscribe(subscriberWSO2);
        InMemoryBroker.unsubscribe(subscriberIBM);
    }


    @Test(expectedExceptions = NoSuchAttributeException.class)
    public void testNoSuchAttributeExceptionForTextOutputMapping() throws InterruptedException {
        log.info("Test for non-existing attribute in text mapping with SiddhiQL - expects NoSuchAttributeException");

        String streams = "" +
                "@App:name('TestSiddhiApp')" +
                "define stream FooStream (symbol string, price float, volume long); " +
                "@sink(type='inMemory', topic='{{symbol}}', @map(type='text', @payload(\"Stock price of {{non-exist}}" +
                " is {{price}}\"))) " +
                "define stream BarStream (symbol string, price float, volume long); ";

        String query = "" +
                "from FooStream " +
                "select * " +
                "insert into BarStream; ";

        SiddhiManager siddhiManager = new SiddhiManager();
        siddhiManager.setExtension("sink:inMemory", InMemorySink.class);
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler stockStream = siddhiAppRuntime.getInputHandler("FooStream");

        siddhiAppRuntime.start();
        stockStream.send(new Object[]{"WSO2", 55.6f, 100L});
        stockStream.send(new Object[]{"IBM", 75.6f, 100L});
        stockStream.send(new Object[]{"WSO2", 57.6f, 100L});
        Thread.sleep(100);
        siddhiAppRuntime.shutdown();
    }
}

/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.exception.NoSuchAttributeException;
import org.wso2.siddhi.core.stream.output.sink.SinkListener;
import org.wso2.siddhi.core.stream.output.sink.SinkMapper;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.core.util.transport.OptionHolder;
import org.wso2.siddhi.core.util.transport.TemplateBuilder;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

import java.util.List;

/**
 * Text output mapper implementation. This will convert Siddhi Event to it's string representation.
 */
@Extension(
        name = "text",
        namespace = "sinkMapper",
        description = "Text to Event input mapper. Transports which accepts text messages can utilize this extension"
                + "to convert the incoming text message to Siddhi event. Users can either use  a pre-defined "
                + "text format where event conversion will happen without any configs or use placeholders to map from" +
                " a custom text message.",
        parameters = {
                @Parameter(name = "event.grouping.enabled",
                        description =
                                "This attribute is used to specify whether the event grouping is enabled or not." +
                                        " If user needs to publish a group of events together user can enable this " +
                                        "by specifying 'true'.",
                        type = {DataType.BOOL},
                        optional = true,
                        defaultValue = "false"),
                @Parameter(name = "delimiter",
                        description = "This attribute indicates the delimiter of the grouped event which is expected" +
                                " to be received. This should be a whole line not a single character.",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "~~~~~~~~~~"),
                @Parameter(name = "new.line.character",
                        description = "This attribute indicates the new line character ofthe event which is " +
                                "expected to be received. This is used mostly when communication between 2 types of " +
                                "operating systems is expected. For instance as the end of line character " +
                                "linux uses '\n' while windows use '\r\n'.",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "\n")
        },
        examples = {
                @Example(
                        syntax = "@sink(type='inMemory', topic='stock', @map(type='text'))\n"
                                + "define stream FooStream (symbol string, price float, volume long);\n",
                        description = "Above configuration will perform a default text input mapping. " +
                                "Expected output would be as follows,"

                        + "symbol:\"WSO2\",\n"
                        + "price:55.6,\n"
                        + "volume:100"

                        + "or"

                        + "symbol:'WSO2',\n"
                        + "price:55.6,\n"
                        + "volume:100"

                        + "If group events is enabled then the output would be as follows,"

                        + "symbol:'WSO2',\n"
                        + "price:55.6,\n"
                        + "volume:100\n"
                        + "~~~~~~~~~~\n"
                        + "symbol:'WSO2',\n"
                        + "price:55.6,\n"
                        + "volume:100"
                ),
                @Example(
                        syntax = "@sink(type='inMemory', topic='stock', @map(type='text', " +
                                " @payload(" +
                                "SensorID : {{symbol}}/{{Volume}},\n" +
                                "SensorPrice : Rs{{price}}/=,\n" +
                                "Value : {{Volume}}ml”)))",
                        description = "Above configuration will perform a custom text mapping. The output will "
                                + "be as follows,"

                                + "SensorID : wso2/100,\n"
                                + "SensorPrice : Rs1000/=,\n"
                                + "Value : 100ml"

                                + "for the following siddhi event."

                                + "{wso2,1000,100}"
                )
        }
)
public class TextSinkMapper extends SinkMapper {
    private static final Logger log = Logger.getLogger(TextSinkMapper.class);
    private static final String EVENT_ATTRIBUTE_SEPARATOR = ",";
    private static final String STRING_ENCLOSING_ELEMENT = "\"";
    private static final String EVENT_ATTRIBUTE_VALUE_SEPARATOR = ":";
    private static final String OPTION_GROUP_EVENTS = "event.grouping.enabled";
    private static final String OPTION_GROUP_EVENTS_DELIMITER = "delimiter";
    private static final String DEFAULT_EVENTS_DELIMITER = "~~~~~~~~~~";
    private static final String DEFAULT_GROUP_EVENTS = "false";
    private static final String OPTION_NEW_LINE = "new.line.character";
    private static final String DEFAULT_NEW_LINE = "\n";

    private boolean eventGroupEnabled;
    private String eventDelimiter;
    private List<Attribute> attributeList;
    private String endOfLine;
    private String streamID;

    @Override
    public void init(StreamDefinition streamDefinition, OptionHolder optionHolder, TemplateBuilder
            templateBuilder, ConfigReader configReader, SiddhiAppContext siddhiAppContext) {
        this.streamID = streamDefinition.getId();
        this.attributeList = streamDefinition.getAttributeList();
        this.eventGroupEnabled = Boolean.valueOf(optionHolder
                .validateAndGetStaticValue(OPTION_GROUP_EVENTS, DEFAULT_GROUP_EVENTS));
        this.endOfLine = optionHolder.validateAndGetStaticValue(OPTION_NEW_LINE, DEFAULT_NEW_LINE);
        this.eventDelimiter = optionHolder.validateAndGetStaticValue(OPTION_GROUP_EVENTS_DELIMITER,
                DEFAULT_EVENTS_DELIMITER) + endOfLine;
    }

    @Override
    public String[] getSupportedDynamicOptions() {
        return new String[0];
    }

    @Override
    public Class[] getOutputEventClasses() {
        return new Class[]{String.class};
    }

    @Override
    public void mapAndSend(Event[] events, OptionHolder optionHolder, TemplateBuilder templateBuilder,
                           SinkListener sinkListener) {
        if (!eventGroupEnabled) { //Event not grouping
            if (templateBuilder != null) { //custom mapping case
                for (Event event : events) {
                    if (event != null) {
                        sinkListener.publish(templateBuilder.build(event));
                    }
                }
            } else { //default mapping case
                for (Event event : events) {
                    if (event != null) {  
                        sinkListener.publish(constructDefaultMapping(event, false));
                    }
                }
            }
        } else { //events group scenario
            StringBuilder eventData = new StringBuilder();
            if (templateBuilder != null) { //custom mapping case
                for (Event event : events) {
                    if (event != null) {
                        eventData.append(templateBuilder.build(event)).append(endOfLine)
                                .append(eventDelimiter);
                    }
                }
                int idx = eventData.lastIndexOf(eventDelimiter);
                eventData.delete(idx - endOfLine.length(), idx + eventDelimiter.length());
            } else { //default mapping case
                for (Event event : events) {
                    if (event != null) {
                        eventData.append(constructDefaultMapping(event, true))
                                .append(eventDelimiter);
                    }
                }
                int idx = eventData.lastIndexOf(eventDelimiter);
                eventData.delete(idx, idx + eventDelimiter.length());
            }
            sinkListener.publish(eventData.toString());
        }

    }

    @Override
    public void mapAndSend(Event event, OptionHolder optionHolder, TemplateBuilder templateBuilder,
                           SinkListener sinkListener) {
        if (templateBuilder != null) { //custom mapping case
            if (event != null) {
                try {
                    sinkListener.publish(templateBuilder.build(event));
                } catch (NoSuchAttributeException e) {
                    log.error("Malformed event " + event.toString() + ". Hence proceed with null values" +
                            " in the stream " + streamID + " of siddhi text output mapper.");
                    //drop the event
                }
            }
        } else { //default mapping case
            if (event != null) {
                sinkListener.publish(constructDefaultMapping(event, false));
            }
        }
    }

    /**
     * Convert the given {@link Event} to Text string.
     *
     * @param event Event object
     * @return the constructed TEXT string
     */
    private Object constructDefaultMapping(Event event, boolean isEventGroup) {
        StringBuilder eventText = new StringBuilder();
        Object[] data = event.getData();
        for (int i = 0; i < data.length; i++) {
            Object attributeValue = data[i];
            Attribute attribute = attributeList.get(i);
            if ((attributeValue != null) && attribute.getType().equals(Attribute.Type.STRING)) {
                eventText.append(attribute.getName()).append(EVENT_ATTRIBUTE_VALUE_SEPARATOR)
                        .append(STRING_ENCLOSING_ELEMENT).append(attributeValue.toString())
                        .append(STRING_ENCLOSING_ELEMENT).append(EVENT_ATTRIBUTE_SEPARATOR).append(endOfLine);
            } else {
                eventText.append(attribute.getName()).append(EVENT_ATTRIBUTE_VALUE_SEPARATOR)
                        .append(attributeValue).append(EVENT_ATTRIBUTE_SEPARATOR).append(endOfLine);
            }
        }
        int idx = eventText.lastIndexOf(EVENT_ATTRIBUTE_SEPARATOR);
        if (!isEventGroup) {
            eventText.delete(idx, idx + (EVENT_ATTRIBUTE_SEPARATOR + endOfLine).length());
        } else {
            eventText.delete(idx, idx + EVENT_ATTRIBUTE_SEPARATOR.length());
        }
        return eventText.toString();
    }
}

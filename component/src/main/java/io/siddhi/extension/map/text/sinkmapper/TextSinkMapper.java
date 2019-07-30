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

package io.siddhi.extension.map.text.sinkmapper;

import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import com.github.mustachejava.MustacheFactory;
import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.event.Event;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.stream.output.sink.SinkListener;
import io.siddhi.core.stream.output.sink.SinkMapper;
import io.siddhi.core.util.SiddhiConstants;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.core.util.transport.TemplateBuilder;
import io.siddhi.query.api.annotation.Annotation;
import io.siddhi.query.api.annotation.Element;
import io.siddhi.query.api.definition.Attribute;
import io.siddhi.query.api.definition.StreamDefinition;
import io.siddhi.query.api.util.AnnotationHelper;
import org.apache.log4j.Logger;

import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Text output mapper implementation. This will convert Siddhi Event to it's string representation.
 */
@Extension(
        name = "text",
        namespace = "sinkMapper",
        description = "This extension is a Event to Text output mapper. Transports that publish text messages can" +
                " utilize this extension to convert the Siddhi events to text messages. Users can use" +
                " a pre-defined text format where event conversion is carried out without any additional " +
                "configurations, or use custom placeholder(using `{{` and `}}` or `{{{` and `}}}`) to map custom text" +
                " messages. All variables are HTML escaped by default.\n" +
                "For example:\n`&` is replaced with `&amp;amp;`" + "\n" +
                "`\"` is replaced with `&amp;quot;`\n" +
                "`=` is replaced with `&amp;#61;`\n" +
                "If you want to return unescaped HTML, use the triple mustache `{{{` instead of" +
                " double `{{`.",
        parameters = {
                @Parameter(name = "event.grouping.enabled",
                        description = "If this parameter is set to `true`, events are grouped via a delimiter when " +
                                "multiple events are received. It is required to specify a value for the " +
                                "`delimiter` parameter when the value for this parameter is `true`.",
                        type = {DataType.BOOL},
                        optional = true,
                        defaultValue = "false"),

                @Parameter(name = "delimiter",
                        description = "This parameter specifies how events are separated when a grouped event is" +
                                " received. This must be a whole line and not a single character.",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "~~~~~~~~~~"),

                @Parameter(name = "new.line.character",
                        description = "This attribute indicates the new line character of the event that is " +
                                "expected to be received. This is used mostly when communication between 2 types of " +
                                "operating systems is expected. For example, Linux uses `\\n` whereas Windows" +
                                " uses `\\r\\n` as the end of line character.",
                        type = {DataType.STRING},
                        optional = true,
                        defaultValue = "\\n")
        },
        examples = {
                @Example(
                        syntax = "@sink(type='inMemory', topic='stock', @map(type='text'))\n"
                                + "define stream FooStream (symbol string, price float, volume long);",
                        description = "This query performs a default text input mapping. The expected output is " +
                                "as follows:\n"

                                + "symbol:\"WSO2\",\n"
                                + "price:55.6,\n"
                                + "volume:100"
                ),
                @Example(
                        syntax = "@sink(type='inMemory', topic='stock', @map(type='text', " +
                                "event.grouping.enabled='true'))\n"
                                + "define stream FooStream (symbol string, price float, volume long);",
                        description = "This query performs a default text input mapping with event grouping. The " +
                                "expected output is as follows:\n"

                                + "symbol:\"WSO2\",\n"
                                + "price:55.6,\n"
                                + "volume:100\n"
                                + "~~~~~~~~~~\n"
                                + "symbol:\"WSO2\",\n"
                                + "price:55.6,\n"
                                + "volume:100"
                ),
                @Example(
                        syntax = "@sink(type='inMemory', topic='stock', @map(type='text', " +
                                " @payload(\"SensorID : {{symbol}}/{{volume}}, SensorPrice : Rs{{price}}/=, " +
                                "Value : {{volume}}ml\")))\n"
                                + "define stream FooStream (symbol string, price float, volume long);",
                        description = "This query performs a custom text mapping. The expected output is as follows:\n"

                                + "SensorID : wso2/100, "
                                + "SensorPrice : Rs1000/=, "
                                + "Value : 100ml \n"

                                + "for the following siddhi event.\n"
                                + "{wso2,1000,100}"
                ),
                @Example(
                        syntax = "@sink(type='inMemory', topic='stock', @map(type='text', event.grouping.enabled=" +
                                "'true', @payload(\"Stock price of {{symbol}} is {{price}}\")))\n"
                                + "define stream FooStream (symbol string, price float, volume long);",
                        description = "This query performs a custom text mapping with event grouping. The expected " +
                                "output is as follows:\n"

                                + "Stock price of WSO2 is 55.6\n"
                                + "~~~~~~~~~~\n"
                                + "Stock price of WSO2 is 55.6\n"
                                + "~~~~~~~~~~\n"
                                + "Stock price of WSO2 is 55.6\n"

                                + "for the following siddhi event.\n"
                                + "{WSO2,55.6,10}"
                ),
                @Example(
                        syntax = "@sink(type='inMemory', topic='stock', @map(type='text', " +
                                " @payload(\"SensorID : {{{symbol}}}/{{{volume}}}, SensorPrice : Rs{{{price}}}/=, " +
                                "Value : {{{volume}}}ml\")))\n"
                                + "define stream FooStream (symbol string, price float, volume long);",
                        description = "This query performs a custom text mapping to return unescaped HTML. " +
                                "The expected output is as follows:\n"

                                + "SensorID : a&b/100, "
                                + "SensorPrice : Rs1000/=, "
                                + "Value : 100ml \n"

                                + "for the following siddhi event.\n"
                                + "{a&b,1000,100}"
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
    private Mustache mustache;
    private Map<String, Object> scopes;

    @Override
    public void init(StreamDefinition streamDefinition, OptionHolder optionHolder, Map<String,
            TemplateBuilder> payloadTemplateBuilderMap, ConfigReader mapperConfigReader,
                     SiddhiAppContext siddhiAppContext) {

        MustacheFactory mf = new DefaultMustacheFactory();
        scopes = new HashMap<String, Object>();
        this.streamID = streamDefinition.getId();
        this.attributeList = streamDefinition.getAttributeList();
        this.eventGroupEnabled = Boolean.valueOf(optionHolder
                .validateAndGetStaticValue(OPTION_GROUP_EVENTS, DEFAULT_GROUP_EVENTS));
        this.endOfLine = optionHolder.validateAndGetStaticValue(OPTION_NEW_LINE, DEFAULT_NEW_LINE);
        this.eventDelimiter = optionHolder.validateAndGetStaticValue(OPTION_GROUP_EVENTS_DELIMITER,
                DEFAULT_EVENTS_DELIMITER) + endOfLine;

        //if @payload() is added there must be at least 1 element in it, otherwise a SiddhiParserException raised
        if (payloadTemplateBuilderMap != null && payloadTemplateBuilderMap.size() != 1) {
            throw new SiddhiAppCreationException("Text sink-mapper does not support multiple @payload mappings, " +
                    "error at the mapper of '" + streamDefinition.getId() + "'");
        }
        if (payloadTemplateBuilderMap != null &&
                payloadTemplateBuilderMap.get(payloadTemplateBuilderMap.keySet().iterator().next()).isObjectMessage()) {
            throw new SiddhiAppCreationException("Text sink-mapper does not support object @payload mappings, " +
                    "error at the mapper of '" + streamDefinition.getId() + "'");
        }
        //if it is custom mapping, create the custom template and compile
        if (payloadTemplateBuilderMap != null) {
            String customTemplate = createCustomTemplate(getTemplateFromPayload(streamDefinition), eventGroupEnabled);
            mustache = mf.compile(new StringReader(customTemplate), "customEvent");
        }
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
    public void mapAndSend(Event[] events, OptionHolder optionHolder, Map<String,
            TemplateBuilder> payloadTemplateBuilderMap, SinkListener sinkListener) {
        if (!eventGroupEnabled) { //Event not grouping
            if (payloadTemplateBuilderMap != null) { //custom mapping case
                for (Event event : events) {
                    if (event != null) {
                        sinkListener.publish(constructCustomMapping(event));
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
            if (payloadTemplateBuilderMap != null) { //custom mapping case
                for (Event event : events) {
                    if (event != null) {
                        eventData.append(constructCustomMapping(event));
                    }
                }
            } else { //default mapping case
                for (Event event : events) {
                    if (event != null) {
                        eventData.append(constructDefaultMapping(event, true)).append(eventDelimiter);
                    }
                }
            }
            int idx = eventData.lastIndexOf(eventDelimiter);
            eventData.delete(idx - endOfLine.length(), idx + eventDelimiter.length());
            sinkListener.publish(eventData.toString());
        }
    }

    @Override
    public void mapAndSend(Event event, OptionHolder optionHolder, Map<String,
            TemplateBuilder> payloadTemplateBuilderMap, SinkListener sinkListener) {
        if (payloadTemplateBuilderMap != null) { //custom mapping case
            if (event != null) {
                if (!eventGroupEnabled) { //event not grouping
                    sinkListener.publish(constructCustomMapping(event));
                } else { //event grouping
                    StringBuilder eventData = new StringBuilder();
                    eventData.append(constructCustomMapping(event));
                    int idx = eventData.lastIndexOf(eventDelimiter);
                    eventData.delete(idx - endOfLine.length(), idx + eventDelimiter.length());
                    sinkListener.publish(eventData.toString());
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

    /**
     * Convert the given {@link Event} to Text string.
     *
     * @param event Event object
     * @return the constructed TEXT string
     */
    private Object constructCustomMapping(Event event) {
        Writer writer = new StringWriter();
        Object[] data = event.getData();
        for (int i = 0; i < data.length; i++) {
            Object attributeValue = data[i];
            Attribute attribute = attributeList.get(i);
            scopes.put(attribute.getName(), attributeValue);
        }
        mustache.execute(writer, scopes);
        return writer.toString();
    }

    /**
     * Create the template based on the payload.
     *
     * @param streamDefinition associated streamDefinition
     * @return the payloadString given by the user
     */
    private String getTemplateFromPayload(StreamDefinition streamDefinition) {
        List<Element> elements = null;
        for (Annotation sinkAnnotation : streamDefinition.getAnnotations()) {
            Annotation mapAnnotation = AnnotationHelper.getAnnotation(SiddhiConstants.ANNOTATION_MAP,
                    sinkAnnotation.getAnnotations());
            if (mapAnnotation != null) {
                List<Annotation> attributeAnnotations = mapAnnotation.
                        getAnnotations(SiddhiConstants.ANNOTATION_PAYLOAD);
                if (attributeAnnotations.size() == 1) {
                    elements = attributeAnnotations.get(0).getElements();
                }
            }
        }
        if (elements != null) { //remove the start and end quotes and get the payload
            return elements.get(0).toString().substring(1, elements.get(0).toString().length() - 1);
        } else {
            throw new SiddhiAppCreationException("There is no template given in the @payload in" + streamID);
        }
    }

    /**
     * Create the template based on the payload.
     *
     * @param customTemplate the template given by the user
     * @param isEventGroup   events are grouped or not
     * @return the custom template according to event grouping
     */
    private String createCustomTemplate(String customTemplate, boolean isEventGroup) {
        StringBuilder template = new StringBuilder();
        if (!isEventGroup) { //template for not grouping
            template.append(customTemplate);
        } else { //template for grouping
            template.append(customTemplate).append(endOfLine).append(eventDelimiter);
        }
        return template.toString();
    }
}

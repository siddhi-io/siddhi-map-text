siddhi-map-text
======================================

The **siddhi-map-text extension** is an extension to <a target="_blank" href="https://wso2.github.io/siddhi">Siddhi</a> that supports mapping incoming text messages to a stream at the source, and mapping a stream to text messages at the sink.

Find some useful links below:

* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-map-text">Source code</a>
* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-map-text/releases">Releases</a>
* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-map-text/issues">Issue tracker</a>

## Latest API Docs 

Latest API Docs is <a target="_blank" href="https://wso2-extensions.github.io/siddhi-map-text/api/2.0.0">2.0.0</a>.

## How to use 

**Using the extension in <a target="_blank" href="https://github.com/wso2/product-sp">WSO2 Stream Processor</a>**

* You can use this extension in the latest <a target="_blank" href="https://github.com/wso2/product-sp/releases">WSO2 Stream Processor</a> that is a part of <a target="_blank" href="http://wso2.com/analytics?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">WSO2 Analytics</a> offering, with editor, debugger and simulation support. 

* This extension is shipped by default with WSO2 Stream Processor, if you wish to use an alternative version of this extension you can replace the component <a target="_blank" href="https://github.com/wso2-extensions/siddhi-map-text/releases">jar</a> that can be found in the `<STREAM_PROCESSOR_HOME>/lib` directory.

**Using the extension as a <a target="_blank" href="https://wso2.github.io/siddhi/documentation/running-as-a-java-library">java library</a>**

* This extension can be added as a maven dependency along with other Siddhi dependencies to your project.

```
     <dependency>
        <groupId>io.siddhi.extension.map.text</groupId>
        <artifactId>siddhi-map-text</artifactId>
        <version>x.x.x</version>
     </dependency>
```

## Jenkins Build Status

---

|  Branch | Build Status |
| :------ |:------------ | 
| master  | [![Build Status](https://wso2.org/jenkins/view/All%20Builds/job/siddhi/job/siddhi-map-text/badge/icon)](https://wso2.org/jenkins/view/All%20Builds/job/siddhi/job/siddhi-map-text/) |

---

## Features

* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-map-text/api/2.0.0/#text-sink-mapper">text</a> *<a target="_blank" href="http://siddhi.io/documentation/siddhi-5.x/query-guide-5.x/#sink-mapper">(Sink Mapper)</a>*<br><div style="padding-left: 1em;"><p>This extension is a Event to Text output mapper. Transports that publish text messages can utilize this extension to convert the Siddhi events to text messages. Users can use a pre-defined text format where event conversion is carried out without any additional configurations, or use custom placeholder(using <code>{{</code> and <code>}}</code> or <code>{{{</code> and <code>}}}</code>) to map custom text messages. All variables are HTML escaped by default.<br>For example:<br><code>&</code> is replaced with <code>&amp;amp;</code><br><code>"</code> is replaced with <code>&amp;quot;</code><br><code>=</code> is replaced with <code>&amp;#61;</code><br>If you want to return unescaped HTML, use the triple mustache <code>{{{</code> instead of double <code>{{</code>.</p></div>
* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-map-text/api/2.0.0/#text-source-mapper">text</a> *<a target="_blank" href="http://siddhi.io/documentation/siddhi-5.x/query-guide-5.x/#source-mapper">(Source Mapper)</a>*<br><div style="padding-left: 1em;"><p>This extension is a text to Siddhi event input mapper. Transports that accept text messages can utilize this extension to convert the incoming text message to Siddhi event. Users can either use a pre-defined text format where event conversion happens without any additional configurations, or specify a regex to map a text message using custom configurations.</p></div>

## How to Contribute
 
  * Please report issues at <a target="_blank" href="https://github.com/wso2-extensions/siddhi-map-text/issues">GitHub Issue Tracker</a>.
  
  * Send your contributions as pull requests to <a target="_blank" href="https://github.com/wso2-extensions/siddhi-map-text/tree/master">master branch</a>. 
 
## Contact us 

 * Post your questions with the <a target="_blank" href="http://stackoverflow.com/search?q=siddhi">"Siddhi"</a> tag in <a target="_blank" href="http://stackoverflow.com/search?q=siddhi">Stackoverflow</a>. 
 
 * Siddhi developers can be contacted via the mailing lists:
 
    Developers List   : [dev@wso2.org](mailto:dev@wso2.org)
    
    Architecture List : [architecture@wso2.org](mailto:architecture@wso2.org)
 
## Support 

* We are committed to ensuring support for this extension in production. Our unique approach ensures that all support leverages our open development methodology and is provided by the very same engineers who build the technology. 

* For more details and to take advantage of this unique opportunity contact us via <a target="_blank" href="http://wso2.com/support?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">http://wso2.com/support/</a>. 

Siddhi Map Text
===================

  [![Jenkins Build Status](https://wso2.org/jenkins/job/siddhi/job/siddhi-map-text/badge/icon)](https://wso2.org/jenkins/job/siddhi/job/siddhi-map-text/)
  [![GitHub Release](https://img.shields.io/github/release/siddhi-io/siddhi-map-text.svg)](https://github.com/siddhi-io/siddhi-map-text/releases)
  [![GitHub Release Date](https://img.shields.io/github/release-date/siddhi-io/siddhi-map-text.svg)](https://github.com/siddhi-io/siddhi-map-text/releases)
  [![GitHub Open Issues](https://img.shields.io/github/issues-raw/siddhi-io/siddhi-map-text.svg)](https://github.com/siddhi-io/siddhi-map-text/issues)
  [![GitHub Last Commit](https://img.shields.io/github/last-commit/siddhi-io/siddhi-map-text.svg)](https://github.com/siddhi-io/siddhi-map-text/commits/master)
  [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

The **siddhi-map-text extension** is an extension to <a target="_blank" href="https://wso2.github.io/siddhi">Siddhi</a> that converts text messages to/from Siddhi events.

For information on <a target="_blank" href="https://siddhi.io/">Siddhi</a> and it's features refer <a target="_blank" href="https://siddhi.io/redirect/docs.html">Siddhi Documentation</a>. 

## Download

* Versions 2.x and above with group id `io.siddhi.extension.*` from <a target="_blank" href="https://mvnrepository.com/artifact/io.siddhi.extension.map.text/siddhi-map-text/">here</a>.
* Versions 1.x and lower with group id `org.wso2.extension.siddhi.*` from <a target="_blank" href="https://mvnrepository.com/artifact/org.wso2.extension.siddhi.map.text/siddhi-map-text">here</a>.

## Latest API Docs 

Latest API Docs is <a target="_blank" href="https://siddhi-io.github.io/siddhi-map-text/api/2.1.0">2.1.0</a>.

## Features

* <a target="_blank" href="https://siddhi-io.github.io/siddhi-map-text/api/2.1.0/#text-sink-mapper">text</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#sink-mapper">Sink Mapper</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;">This extension is a Event to Text output mapper. Transports that publish text messages can utilize this extension to convert the Siddhi events to text messages. Users can use a pre-defined text format where event conversion is carried out without any additional configurations, or use custom placeholder(using <code>{{</code> and <code>}}</code>) to map custom text messages. Again, you can also enable mustache based custom mapping. In mustache based custom mapping you can use custom placeholder (using <code>{{</code> and <code>}}</code> or <code>{{{</code> and <code>}}}</code>) to map custom text. In mustache based custom mapping, all variables are HTML escaped by default.<br>For example:<br><code>&</code> is replaced with <code>&amp;amp;</code><br><code>"</code> is replaced with <code>&amp;quot;</code><br><code>=</code> is replaced with <code>&amp;#61;</code><br>If you want to return unescaped HTML, use the triple mustache <code>{{{</code> instead of double <code>{{</code>.</p></p></div>
* <a target="_blank" href="https://siddhi-io.github.io/siddhi-map-text/api/2.1.0/#text-source-mapper">text</a> *(<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#source-mapper">Source Mapper</a>)*<br> <div style="padding-left: 1em;"><p><p style="word-wrap: break-word;margin: 0;">This extension is a text to Siddhi event input mapper. Transports that accept text messages can utilize this extension to convert the incoming text message to Siddhi event. Users can either use a pre-defined text format where event conversion happens without any additional configurations, or specify a regex to map a text message using custom configurations.</p></p></div>

## Dependencies 

There are no other dependencies needed for this extension. 

## Installation

For installing this extension on various siddhi execution environments refer Siddhi documentation section on <a target="_blank" href="https://siddhi.io/redirect/add-extensions.html">adding extensions</a>.

## Support and Contribution

* We encourage users to ask questions and get support via <a target="_blank" href="https://stackoverflow.com/questions/tagged/siddhi">StackOverflow</a>, make sure to add the `siddhi` tag to the issue for better response.

* If you find any issues related to the extension please report them on <a target="_blank" href="https://github.com/siddhi-io/siddhi-execution-string/issues">the issue tracker</a>.

* For production support and other contribution related information refer <a target="_blank" href="https://siddhi.io/community/">Siddhi Community</a> documentation.


# siddhi-map-text
======================================
---
|  Branch | Build Status |
| :------ |:------------ | 
| master  | [![Build Status](https://wso2.org/jenkins/view/All%20Builds/job/siddhi/job/siddhi-map-text/badge/icon)](https://wso2.org/jenkins/view/All%20Builds/job/siddhi/job/siddhi-map-text/) |
---
##### New version of Siddhi v4.0.0 is built in Java 8.

This extension provides the functionality to convert canonical events of the server in the WSO2Event format to any text message format
and convert events of any text format to the server's canonical event format (WSO2Event) for processing.

Features Supported
------------------
This extension is used to convert Text message to/from Siddhi events.

  - Text source mapper :Text source mapping allows user to convert events of any text format to the server's canonical event format (WSO2Event) for processing.
  - Text sink mapper : Text sink mapping converts canonical events of the server in the WSO2Event format to any text message format. A sample mapping configuration is shown below.

Prerequisites for using the feature
------------------
  - Siddhi Stream should be defined

Deploying the feature
------------------
   Feature can be deploy as a OSGI bundle by putting jar file of component to DAS_HOME/lib directory of DAS 4.0.0 pack.

Example Siddhi Queries
------------------
Default Mapping
--------------
      - @source(type='inMemory', topic='home', @map(type='text'))
        define stream UsageStream (houseId int, maxVal float, minVal float, avgVal double);

      - @sink(type='inMemory', topic='home', @map(type='text'))
        define stream InMemorySmartHomeInputData (houseId int, maxVal float, minVal float, avgVal double);
Custom Mapping
--------------
       -@source(type='inMemory', topic='home', @map(type='text' , regex.A='houseId:([-,.0-9E]+),\nmaxVal:([-,.0-9E]+),\nminVal:([-,.0-9E]+),\navgVal:([-,.0-9E]+)',
        @attributes(houseId = 'A[1]', maxVal = 'A[2]', minVal = 'A[3]' ,avgVal='A[4]')))
        define stream UsageStream2 (houseId int, maxVal float, minVal float, avgVal double);

       -@sink(type='inMemory', topic='home', @map(type='text',
        @payload("""houseId:{{houseId}},
        maxVal:{{maxVal}},
        minVal:{{minVal}},
        avgVal:{{avgVal}}""")))
        define stream InMemorySmartHomeInputData2 (houseId int, maxVal float, minVal float, avgVal double);

How to Contribute
------------------
   * Send your bug fixes pull requests to [master branch] (https://github.com/wso2-extensions/siddhi-map-text/tree/master)

Contact us
------------------
   Siddhi developers can be contacted via the mailing lists:

   * Carbon Developers List : dev@wso2.org
   * Carbon Architecture List : architecture@wso2.org

We welcome your feedback and contribution.
------------------
SP Team

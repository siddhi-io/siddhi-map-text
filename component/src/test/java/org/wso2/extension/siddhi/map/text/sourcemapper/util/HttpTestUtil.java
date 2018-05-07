/*
 *  Copyright (c) 2017 WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 *
 */
package org.wso2.extension.siddhi.map.text.sourcemapper.util;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URI;
/**
 * Util class for test cases.
 */
public class HttpTestUtil {
    private static final org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(HttpTestUtil.class);

    public static void httpPublishEvent(String event, URI baseURI) {
        try {
            HttpURLConnection urlConn = null;
            try {
                urlConn = HttpServerUtil.request(baseURI);
            } catch (IOException e) {
                logger.error("IOException occurred while running the HttpsSourceTestCaseForSSL", e);
            }
            assert urlConn != null;
            HttpServerUtil.writeContent(urlConn, event);
            logger.info("Event response code " + urlConn.getResponseCode());
            logger.info("Event response message " + urlConn.getResponseMessage());
            urlConn.disconnect();
        } catch (IOException e) {
            logger.error("IOException occurred while running the HttpsSourceTestCaseForSSL", e);
        }
    }

}

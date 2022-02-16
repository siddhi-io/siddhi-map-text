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
package io.siddhi.extension.map.text.sinkmapper.util;

import com.sun.net.httpserver.HttpServer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Http test sever listener.
 */
public class HttpServerListenerHandler implements Runnable {
    private static final Logger logger = LogManager.getLogger(HttpServerListenerHandler.class);
    private HttpServerListener serverListener;
    private HttpServer server;
    private int port;

    public HttpServerListenerHandler(int port) {
        this.serverListener = new HttpServerListener();
        this.port = port;
    }

    @Override
    public void run() {
        try {
            server = HttpServer.create(new InetSocketAddress(port), 5);
            server.createContext("/abc", serverListener);
            server.start();
        } catch (IOException e) {
            logger.error("Error in creating test server.", e);
        }
    }

    public void shutdown() {
        if (server != null) {
            server.stop(1);
        }
    }

    public HttpServerListener getServerListener() {
        return serverListener;
    }
}

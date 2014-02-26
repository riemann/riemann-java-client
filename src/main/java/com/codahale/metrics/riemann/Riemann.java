/*
 * Copyright 2014 Dominic LoBue
 * Copyright 2014 Brandon Seibel
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.codahale.metrics.riemann;

import com.aphyr.riemann.client.EventDSL;
import com.aphyr.riemann.client.RiemannClient;
import com.aphyr.riemann.client.AbstractRiemannClient;
import com.aphyr.riemann.client.RiemannBatchClient;
import com.aphyr.riemann.client.UnsupportedJVMException;


import java.io.Closeable;
import java.io.IOException;
import java.net.UnknownHostException;


public class Riemann implements Closeable {

    String riemannHost;
    Integer riemannPort;

    AbstractRiemannClient client;

    public Riemann(String host, Integer port) throws IOException, UnknownHostException {
        this(host, port, 10);
    }
    public Riemann(String host, Integer port, int batchSize) throws IOException, UnknownHostException {
        this.riemannHost = host;
        this.riemannPort = port;
        RiemannClient c = RiemannClient.tcp(riemannHost, riemannPort);
        try {
            this.client = new RiemannBatchClient(batchSize,c);
        } catch (UnsupportedJVMException e) {
            this.client = c;
        }
    }

    public void connect() throws IOException {
        if (!client.isConnected()) {
            client.connect();
        }
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            client.disconnect();
        }

    }

}

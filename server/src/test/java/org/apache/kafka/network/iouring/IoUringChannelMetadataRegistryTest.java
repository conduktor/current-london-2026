/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.network.iouring;

import org.apache.kafka.common.network.CipherInformation;
import org.apache.kafka.common.network.ClientInformation;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class IoUringChannelMetadataRegistryTest {

    @Test
    void roundTripsCipherAndClientInformation() {
        IoUringChannelMetadataRegistry r = new IoUringChannelMetadataRegistry();
        assertNull(r.cipherInformation());
        assertNull(r.clientInformation());

        CipherInformation c = new CipherInformation("TLS_AES_128_GCM_SHA256", "TLSv1.3");
        r.registerCipherInformation(c);
        assertEquals(c, r.cipherInformation());

        ClientInformation client = new ClientInformation("librdkafka", "2.4.0");
        r.registerClientInformation(client);
        assertEquals(client, r.clientInformation());
    }

    @Test
    void overwriteReplaces() {
        IoUringChannelMetadataRegistry r = new IoUringChannelMetadataRegistry();
        r.registerCipherInformation(new CipherInformation("c1", "p1"));
        r.registerCipherInformation(new CipherInformation("c2", "p2"));
        assertEquals("c2", r.cipherInformation().cipher());
    }

    @Test
    void closeClears() {
        IoUringChannelMetadataRegistry r = new IoUringChannelMetadataRegistry();
        r.registerCipherInformation(new CipherInformation("c", "p"));
        r.registerClientInformation(new ClientInformation("name", "ver"));
        r.close();
        assertNull(r.cipherInformation());
        assertNull(r.clientInformation());
    }
}

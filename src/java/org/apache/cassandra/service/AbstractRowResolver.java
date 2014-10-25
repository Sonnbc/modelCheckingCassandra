/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.service;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Set;

import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.ReadResponse;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.net.MessageIn;
import org.cliffc.high_scale_lib.NonBlockingHashSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractRowResolver implements IResponseResolver<ReadResponse, Row>
{
    protected static final Logger logger = LoggerFactory.getLogger(AbstractRowResolver.class);

    protected final String table;
    protected final Set<MessageIn<ReadResponse>> replies = new NonBlockingHashSet<MessageIn<ReadResponse>>();
    protected final DecoratedKey key;

    public AbstractRowResolver(ByteBuffer key, String table)
    {
        //logger.debug("key = {}", new String(key.array(), Charset.forName("UTF-8")));
        this.key = StorageService.getPartitioner().decorateKey(key);
        this.table = table;
    }

    protected static void appendMismatchInfo(DecoratedKey key, ColumnFamily cf, boolean isMismatched)
    {
        String keystr = new String(key.key.array(), Charset.forName("UTF-8"));
        //logger.debug("appendMismatchedInfo {}", keystr);
        if (keystr.contains("user") && (!keystr.equals("usertable"))) {
            String status = isMismatched ? "Mismatched" : "Good";
            ByteBuffer field = ByteBuffer.wrap("zextra".getBytes(Charset.forName("UTF-8")));
            ByteBuffer value = ByteBuffer.wrap(status.getBytes(Charset.forName("UTF-8")));
            Column c = new Column(field, value);
            cf.addColumn(null, c);
        }
    }

    public void preprocess(MessageIn<ReadResponse> message)
    {
        replies.add(message);
    }

    public Iterable<MessageIn<ReadResponse>> getMessages()
    {
        return replies;
    }
}

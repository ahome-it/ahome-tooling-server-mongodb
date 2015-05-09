/*
   Copyright (c) 2014,2015 Ahome' Innovation Technologies. All rights reserved.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */

package com.ait.tooling.server.mongodb.support.spring;

import java.io.IOException;
import java.util.Objects;

import com.ait.tooling.common.api.java.util.StringOps;
import com.ait.tooling.server.mongodb.MongoDB;
import com.mongodb.MongoClientOptions;

public class MongoDBDescriptor implements IMongoDBDescriptor
{
    private String       m_name;

    private MongoDB      m_modb;

    private boolean      m_id        = false;

    private String       m_mongohost = "localhost";

    private int          m_mongoport = 27017;

    private int          m_poolsize  = 100;

    private int          m_multiple  = 100;

    private int          m_ctimeout  = 10000;

    private final String m_defaultd;

    public MongoDBDescriptor(final String defaultd)
    {
        m_defaultd = StringOps.requireTrimOrNull(defaultd);
    }

    @Override
    public boolean isAddingID()
    {
        return m_id;
    }

    @Override
    public void setAddingID(final boolean id)
    {
        m_id = id;
    }

    @Override
    public void close() throws IOException
    {
        if (null != m_modb)
        {
            m_modb.close();
        }
    }

    @Override
    public String getName()
    {
        return m_name;
    }

    @Override
    public void setName(final String name)
    {
        m_name = Objects.requireNonNull(StringOps.toTrimOrNull(name), "MongoDBDescriptor name is null or empty");
    }

    @Override
    public synchronized MongoDB getMongoDB()
    {
        if (null == m_modb)
        {
            final MongoClientOptions opts = MongoClientOptions.builder().connectionsPerHost(getConnectionPoolSize()).threadsAllowedToBlockForConnectionMultiplier(getConnectionMultiplier()).connectTimeout(getConnectionTimeout()).build();

            m_modb = new MongoDB(opts, getHost(), getPort(), getDefaultDB());

            m_modb.setAddingID(isAddingID());
        }
        return m_modb;
    }

    @Override
    public int getConnectionTimeout()
    {
        return m_ctimeout;
    }

    @Override
    public int getConnectionMultiplier()
    {
        return m_multiple;
    }

    @Override
    public int getConnectionPoolSize()
    {
        return m_poolsize;
    }

    @Override
    public void setConnectionTimeout(final int timeout)
    {
        m_ctimeout = Math.max(0, timeout);
    }

    @Override
    public void setConnectionMultiplier(final int multiplier)
    {
        m_multiple = Math.max(0, multiplier);
    }

    @Override
    public void setConnectionPoolSize(final int poolsize)
    {
        m_poolsize = Math.max(1, poolsize);
    }

    @Override
    public String getHost()
    {
        return m_mongohost;
    }

    @Override
    public void setHost(String host)
    {
        host = StringOps.toTrimOrNull(host);

        if (host != null)
        {
            m_mongohost = host;
        }
    }

    @Override
    public int getPort()
    {
        return m_mongoport;
    }

    @Override
    public void setPort(final int port)
    {
        m_mongoport = Math.max(1024 + 1, port);
    }

    @Override
    public String getDefaultDB()
    {
        return m_defaultd;
    }
}

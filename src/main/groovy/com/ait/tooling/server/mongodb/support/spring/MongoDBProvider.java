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
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.log4j.Logger;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.BeanFactoryAware;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.jmx.export.annotation.ManagedOperation;
import org.springframework.jmx.export.annotation.ManagedResource;

import com.ait.tooling.common.api.java.util.StringOps;

@ManagedResource(objectName = "com.ait.tooling.server.mongodb:name=MongoDBProvider", description = "Manage MongoDB Descriptors.")
public class MongoDBProvider implements BeanFactoryAware, IMongoDBProvider
{
    private static final Logger                             logger        = Logger.getLogger(MongoDBProvider.class);

    private final String                                    m_descriptor;

    private final LinkedHashMap<String, IMongoDBDescriptor> m_descriptors = new LinkedHashMap<String, IMongoDBDescriptor>();

    public MongoDBProvider(final String descriptor)
    {
        m_descriptor = StringOps.requireTrimOrNull(descriptor);
    }

    @Override
    public String getDefaultMongoDBDescriptorName()
    {
        return m_descriptor;
    }

    @Override
    public IMongoDBDescriptor getMongoDBDescriptor(final String name)
    {
        if (null != name)
        {
            return m_descriptors.get(name);
        }
        return null;
    }

    @Override
    public List<String> getMongoDBDescriptorNames()
    {
        return Collections.unmodifiableList(new ArrayList<String>(m_descriptors.keySet()));
    }

    @Override
    @ManagedOperation(description = "Close all MongoDB Descriptors")
    public void close() throws IOException
    {
        for (IMongoDBDescriptor descriptor : m_descriptors.values())
        {
            try
            {
                logger.info("Closing MongoDB Descriptor " + descriptor.getName());

                descriptor.close();
            }
            catch (Exception e)
            {
                logger.error("Error closing MongoDB Descriptor " + descriptor.getName(), e);
            }
        }
    }

    @Override
    public void setBeanFactory(final BeanFactory factory) throws BeansException
    {
        if (factory instanceof DefaultListableBeanFactory)
        {
            for (String name : ((DefaultListableBeanFactory) factory).getBeansOfType(IMongoDBDescriptor.class).keySet())
            {
                final IMongoDBDescriptor descriptor = factory.getBean(name, IMongoDBDescriptor.class);

                descriptor.setName(name);

                logger.info("Found IMongoDBDescriptor(" + name + ") class " + descriptor.getClass().getName());

                m_descriptors.put(name, descriptor);
            }
        }
    }
}

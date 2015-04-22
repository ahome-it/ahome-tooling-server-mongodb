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

package com.ait.tooling.server.mongodb.support

import groovy.transform.Memoized

import com.ait.tooling.common.api.java.util.StringOps
import com.ait.tooling.server.core.support.CoreGroovySupport
import com.ait.tooling.server.mongodb.IMongoDBContext
import com.ait.tooling.server.mongodb.IMongoDBProvider
import com.ait.tooling.server.mongodb.MongoDB
import com.ait.tooling.server.mongodb.MongoDBContext
import com.ait.tooling.server.mongodb.MongoDB.MCollection
import com.ait.tooling.server.mongodb.MongoDB.MDatabase

public class MongoDBGroovySupport extends CoreGroovySupport
{
    @Memoized
    public IMongoDBContext getMongoDBContext()
    {
        MongoDBContext.get()
    }

    @Memoized
    public IMongoDBProvider getMongoDBProvider()
    {
        getMongoDBContext().getMongoDBProvider()
    }

    @Memoized
    public MCollection collection(String name) throws Exception
    {
        db().collection(StringOps.requireTrimOrNull(name))
    }

    @Memoized
    public MDatabase db(String name) throws Exception
    {
        getMongoDB().db(StringOps.requireTrimOrNull(name))
    }

    @Memoized
    public MDatabase db() throws Exception
    {
        getMongoDB().db()
    }

    @Memoized
    public MongoDB getMongoDB()
    {
        getMongoDB('DefaultMongoDB')
    }

    @Memoized
    public MongoDB getMongoDB(String name)
    {
        getMongoDBProvider().getMongoDBDescriptor(StringOps.requireTrimOrNull(name)).getMongoDB()
    }

    public Map IN(Map object, String name)
    {
        IN(object[name])
    }

    public Map IN(def args)
    {
        def list = []

        if (args)
        {
            if (args instanceof List)
            {
                list = args
            }
            else
            {
                list << args
            }
        }
        ['$in': list]
    }
}

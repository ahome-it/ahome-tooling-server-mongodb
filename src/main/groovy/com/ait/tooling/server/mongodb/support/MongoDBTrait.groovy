/*
 * Copyright (c) 2017 Ahome' Innovation Technologies. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ait.tooling.server.mongodb.support

import groovy.transform.CompileStatic
import groovy.transform.Memoized

import java.util.List;
import java.util.Map;

import com.ait.tooling.common.api.java.util.StringOps
import com.ait.tooling.server.core.json.JSONObject
import com.ait.tooling.server.mongodb.MongoDB
import com.ait.tooling.server.mongodb.MongoDB.IMCursor
import com.ait.tooling.server.mongodb.MongoDB.MAggregationGroup
import com.ait.tooling.server.mongodb.MongoDB.MAggregationMatch
import com.ait.tooling.server.mongodb.MongoDB.MCollection
import com.ait.tooling.server.mongodb.MongoDB.MCollectionPreferences
import com.ait.tooling.server.mongodb.MongoDB.MDatabase
import com.ait.tooling.server.mongodb.MongoDB.MProjection
import com.ait.tooling.server.mongodb.MongoDB.MQuery
import com.ait.tooling.server.mongodb.MongoDB.MSort
import com.ait.tooling.server.mongodb.support.spring.IMongoDBContext
import com.ait.tooling.server.mongodb.support.spring.IMongoDBProvider
import com.ait.tooling.server.mongodb.support.spring.MongoDBContextInstance

@CompileStatic
public trait MongoDBTrait
{
    @Memoized
    public IMongoDBContext getMongoDBContext()
    {
        MongoDBContextInstance.getMongoDBContextInstance()
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

    public MCollection collection(String name, MCollectionPreferences opts) throws Exception
    {
        db().collection(StringOps.requireTrimOrNull(name), opts)
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
        getMongoDB(getMongoDBDefaultDescriptorName())
    }

    @Memoized
    public MongoDB getMongoDB(String name)
    {
        getMongoDBProvider().getMongoDBDescriptor(StringOps.requireTrimOrNull(name)).getMongoDB()
    }

    @Memoized
    public String getMongoDBDefaultDescriptorName()
    {
        getMongoDBProvider().getMongoDBDefaultDescriptorName()
    }

    public JSONObject json(IMCursor cursor)
    {
        List list = []

        for (Map map: cursor)
        {
            list << map
        }
        new JSONObject(list)
    }

    public Map INC(Map args)
    {
        ['$inc': args]
    }

    public Map MUL(Map args)
    {
        ['$mul': args]
    }

    public Map RENAME(Map args)
    {
        ['$rename': args]
    }

    public Map SET(Map args)
    {
        ['$set': args]
    }

    public Map UNSET(Map args)
    {
        ['$unset': args]
    }

    public Map MIN(Map args)
    {
        ['$min': args]
    }

    public Map MAX(Map args)
    {
        ['$max': args]
    }

    public MSort SORTS(Map map)
    {
        new MSort(map)
    }

    public MSort ASCENDING(String... fields)
    {
        MSort.ASCENDING(fields)
    }

    public MSort ASCENDING(List<String> fields)
    {
        MSort.ASCENDING(fields)
    }

    public MSort DESCENDING(String... fields)
    {
        MSort.DESCENDING(fields)
    }

    public MSort DESCENDING(List<String> fields)
    {
        MSort.DESCENDING(fields)
    }

    public MSort ORDER_BY(MSort... sorts)
    {
        MSort.ORDER_BY(sorts)
    }

    public MSort ORDER_BY(List<MSort> sorts)
    {
        MSort.ORDER_BY(sorts)
    }

    public MProjection INCLUDE(String... fields)
    {
        MProjection.INCLUDE(fields)
    }

    public MProjection INCLUDE(List<String> fields)
    {
        MProjection.INCLUDE(fields)
    }

    public MProjection EXCLUDE(String... fields)
    {
        MProjection.EXCLUDE(fields)
    }

    public MProjection EXCLUDE(List<String> fields)
    {
        MProjection.EXCLUDE(fields)
    }

    @Memoized
    public MProjection NO_ID()
    {
        MProjection.NO_ID()
    }

    public MProjection FIELDS(MProjection... projections)
    {
        MProjection.FIELDS(projections)
    }

    public MProjection FIELDS(List<MProjection> projections)
    {
        MProjection.FIELDS(projections)
    }

    public MQuery QUERY(Map map)
    {
        MQuery.QUERY(map)
    }

    public MQuery QUERY(MQuery... queries)
    {
        MQuery.AND(queries)
    }

    public MQuery QUERY(List<MQuery> queries)
    {
        MQuery.AND(queries)
    }

    public <T> MQuery EQ(String name, T value)
    {
        MQuery.EQ(name, value)
    }

    public <T> MQuery NE(String name, T value)
    {
        MQuery.NE(name, value)
    }

    public <T> MQuery GT(String name, T value)
    {
        MQuery.GT(name, value)
    }

    public <T> MQuery LT(String name, T value)
    {
        MQuery.LT(name, value)
    }

    public <T> MQuery GTE(String name, T value)
    {
        MQuery.GTE(name, value)
    }

    public <T> MQuery LTE(String name, T value)
    {
        MQuery.LTE(name, value)
    }

    @SuppressWarnings("unchecked")
    public <T> MQuery IN(String name, T... list)
    {
        MQuery.IN(name, list)
    }

    public <T> MQuery IN(String name, List<T> list)
    {
        MQuery.IN(name, list)
    }

    @SuppressWarnings("unchecked")
    public <T> MQuery NIN(String name, T... list)
    {
        MQuery.NIN(name, list)
    }

    public <T> MQuery NIN(String name, List<T> list)
    {
        MQuery.NIN(name, list)
    }

    public MQuery NOT(MQuery query)
    {
        MQuery.NOT(query)
    }

    public MQuery AND(MQuery... queries)
    {
        MQuery.AND(queries)
    }

    public MQuery AND(List<MQuery> queries)
    {
        MQuery.AND(queries)
    }

    public MQuery OR(MQuery... queries)
    {
        MQuery.OR(queries)
    }

    public MQuery OR(List<MQuery> queries)
    {
        MQuery.OR(queries)
    }

    public MQuery EXISTS(String name, boolean exists)
    {
        MQuery.EXISTS(name, exists)
    }

    public MQuery EXISTS(String name)
    {
        MQuery.EXISTS(name)
    }

    public MAggregationMatch MATCH(Map map)
    {
        new MAggregationMatch(map)
    }

    public MAggregationGroup GROUP(Map map)
    {
        new MAggregationGroup(map)
    }
}

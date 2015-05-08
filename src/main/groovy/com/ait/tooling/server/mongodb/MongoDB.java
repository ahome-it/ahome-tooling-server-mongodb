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

package com.ait.tooling.server.mongodb;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.exists;
import static com.mongodb.client.model.Filters.gt;
import static com.mongodb.client.model.Filters.gte;
import static com.mongodb.client.model.Filters.in;
import static com.mongodb.client.model.Filters.lt;
import static com.mongodb.client.model.Filters.lte;
import static com.mongodb.client.model.Filters.ne;
import static com.mongodb.client.model.Filters.nin;
import static com.mongodb.client.model.Filters.not;
import static com.mongodb.client.model.Filters.or;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.bson.BSON;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.Transformer;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import com.ait.tooling.json.JSONUtils;
import com.google.common.base.Strings;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.UpdateOptions;

public final class MongoDB
{
    private static final Logger logger = Logger.getLogger(MongoDB.class);

    private final MongoClient   m_mongo;

    private final String        m_usedb;

    public MongoDB(final MongoClientOptions options, final String host, final int port, final String usedb)
    {
        m_usedb = usedb;

        BSON.addEncodingHook(BigDecimal.class, new Transformer()
        {
            @Override
            public Object transform(Object object)
            {
                return JSONUtils.asDouble(object);
            }
        });
        BSON.addEncodingHook(BigInteger.class, new Transformer()
        {
            @Override
            public Object transform(Object object)
            {
                Long lval = JSONUtils.asLong(object);

                if (null != lval)
                {
                    return lval;
                }
                return JSONUtils.asInteger(object);
            }
        });
        m_mongo = new MongoClient(host, options);
    }

    public void close()
    {
        if (null != m_mongo)
        {
            m_mongo.close();
        }
    }

    public List<String> getDatabaseNames()
    {
        return m_mongo.listDatabaseNames().into(new ArrayList<String>());
    }

    public final MDatabase db(String name) throws Exception
    {
        return db(name, false);
    }

    public final MDatabase db() throws Exception
    {
        return db(m_usedb, false);
    }

    public final MDatabase db(String name, boolean auth) throws Exception
    {
        return new MDatabase(m_mongo.getDatabase(name), auth);
    }

    public static final class MDatabase
    {
        private final MongoDatabase m_db;

        protected MDatabase(MongoDatabase db, boolean auth) throws Exception
        {
            m_db = db;
        }

        public final String getName()
        {
            return m_db.getName();
        }

        public final void drop()
        {
            m_db.drop();
        }

        public final boolean isCollection(String name)
        {
            for (String coll : m_db.listCollectionNames())
            {
                if (coll.equals(name))
                {
                    return true;
                }
            }
            return false;
        }

        public final List<String> getCollectionNames()
        {
            return m_db.listCollectionNames().into(new ArrayList<String>());
        }

        public final MCollection collection(String name) throws Exception
        {
            return new MCollection(m_db.getCollection(name));
        }
    }

    public static final class MCollection
    {
        private final MongoCollection<Document> m_collection;

        protected MCollection(MongoCollection<Document> collection)
        {
            m_collection = collection;
        }

        public final String getName()
        {
            return m_collection.getNamespace().getCollectionName();
        }

        public final MCollection createIndex(Map<String, ?> keys)
        {
            m_collection.createIndex(new MDocument(keys));

            return this;
        }

        public final MCollection createIndex(Map<String, ?> arg0, Map<String, ?> arg1)
        {
            IndexOptions opts = new IndexOptions();

            m_collection.createIndex(new MDocument(arg0), opts);

            return this;
        }

        public final void drop()
        {
            m_collection.drop();
        }

        public final MCollection deleteMany(Map<String, ?> query)
        {
            return deleteMany(new MQuery(query));
        }

        public final MCollection deleteMany(MQuery query)
        {
            m_collection.deleteMany(query);

            return this;
        }
        
        public final MCollection deleteOne(Map<String, ?> query)
        {
            return deleteOne(new MQuery(query));
        }
        
        public final MCollection deleteOne(MQuery query)
        {
            m_collection.deleteOne(query);

            return this;
        }

        @SuppressWarnings("unchecked")
        private final Map<String, ?> ensureid(Map<String, ?> update)
        {
            Object id = update.get("id");

            if ((null == id) || (Strings.isNullOrEmpty(id.toString())))
            {
                ((Map<String, Object>) update).put("id", (new ObjectId()).toString());
            }
            return update;
        }

        public final MCollection insert(Map<String, ?> record)
        {
            m_collection.insertOne(new MDocument(ensureid(record)));

            return this;
        }

        public final MCollection insert(List<Map<String, ?>> list)
        {
            ArrayList<MDocument> save = new ArrayList<MDocument>(list.size());

            for (Map<String, ?> lmap : list)
            {
                save.add(new MDocument(ensureid(lmap)));
            }
            m_collection.insertMany(save);

            return this;
        }

        public final long count()
        {
            return m_collection.count();
        }

        public final long count(Map<String, ?> query)
        {
            return count(new MQuery(query));
        }

        public final long count(MQuery query)
        {
            return m_collection.count(query);
        }

        public final MCursor find() throws Exception
        {
            return find(false);
        }

        final String getNameSpace()
        {
            return m_collection.getNamespace().toString();
        }

        public final MCursor find(boolean with_id) throws Exception
        {
            if (with_id)
            {
                return new MCursor(m_collection.find());
            }
            else
            {
                return new MCursor(m_collection.find().projection(MProjection.NO_ID()));
            }
        }

        public final MCursor find(Map<String, ?> query) throws Exception
        {
            return find(new MQuery(query), false);
        }

        public final MCursor find(MQuery query) throws Exception
        {
            return find(query, false);
        }

        public final MCursor find(Map<String, ?> query, boolean with_id) throws Exception
        {
            return find(new MQuery(query), with_id);
        }

        public final MCursor find(MQuery query, boolean with_id) throws Exception
        {
            if (with_id)
            {
                return new MCursor(m_collection.find(query));
            }
            else
            {
                return new MCursor(m_collection.find(query).projection(MProjection.NO_ID()));
            }
        }

        public final MCursor find(Map<String, ?> query, Map<String, ?> fields) throws Exception
        {
            return find(new MQuery(query), new MProjection(fields));
        }

        public final MCursor find(MQuery query, Map<String, ?> fields) throws Exception
        {
            return find(query, new MProjection(fields));
        }

        public final MCursor find(Map<String, ?> query, MProjection fields) throws Exception
        {
            return find(new MQuery(query), fields, false);
        }

        public final MCursor find(MQuery query, MProjection fields) throws Exception
        {
            return find(query, fields, false);
        }

        public final MCursor find(Map<String, ?> query, Map<String, ?> fields, boolean with_id) throws Exception
        {
            return find(new MQuery(query), new MProjection(fields), with_id);
        }

        public final MCursor find(Map<String, ?> query, MProjection fields, boolean with_id) throws Exception
        {
            return find(new MQuery(query), fields, with_id);
        }

        public final MCursor find(MQuery query, MProjection fields, boolean with_id) throws Exception
        {
            if (with_id)
            {
                return new MCursor(m_collection.find(query).projection(fields));
            }
            else
            {
                return new MCursor(m_collection.find(query).projection(MProjection.FIELDS(fields, MProjection.NO_ID())));
            }
        }

        public final Map<String, ?> findAndModify(Map<String, ?> query, Map<String, ?> update)
        {
            return update(new MQuery(query), update, false, true);
        }

        public final Map<String, ?> findAndModify(MQuery query, Map<String, ?> update)
        {
            return update(query, update, false, true);
        }

        public final Map<String, ?> upsert(Map<String, ?> query, Map<String, ?> update)
        {
            return upsert(new MQuery(query), update);
        }

        public final Map<String, ?> upsert(MQuery query, Map<String, ?> update)
        {
            return update(query, update, true, true);
        }

        public final Map<String, ?> create(Map<String, ?> update)
        {
            insert(update);

            return update;
        }

        public final Map<String, ?> update(Map<String, ?> query, Map<String, ?> update, boolean upsert, boolean multi)
        {
            return update(new MQuery(query), update, upsert, multi);
        }

        public final Map<String, ?> update(MQuery query, Map<String, ?> update, boolean upsert, boolean multi)
        {
            if (multi)
            {
                m_collection.updateMany(query, new MDocument(update), new UpdateOptions().upsert(upsert));
            }
            else
            {
                m_collection.updateOne(query, new MDocument(update), new UpdateOptions().upsert(upsert));
            }
            return update;
        }

        public final Map<String, ?> findOne(Map<String, ?> query)
        {
            return findOne(new MQuery(query));
        }

        public final Map<String, ?> findOne(MQuery query)
        {
            FindIterable<Document> iter = m_collection.find(query).limit(1).projection(MProjection.NO_ID());

            if (null != iter)
            {
                return iter.first();
            }
            return null;
        }

        public final boolean updateOne(Map<String, ?> query, Map<String, ?> update)
        {
            return updateOne(new MQuery(query), update);
        }

        public final boolean updateOne(MQuery query, Map<String, ?> update)
        {
            return (m_collection.updateOne(query, new MDocument(update)).getModifiedCount() == 1L);
        }

        public final long updateMany(Map<String, ?> query, Map<String, ?> update)
        {
            return updateMany(new MQuery(query), update);
        }

        public final long updateMany(MQuery query, Map<String, ?> update)
        {
            return m_collection.updateMany(query, new MDocument(update), new UpdateOptions().upsert(false)).getModifiedCount();
        }

        public final List<?> distinct(String field)
        {
            return m_collection.distinct(field, Document.class).into(new ArrayList<Document>());
        }

        public final List<?> distinct(String field, Map<String, ?> query)
        {
            return m_collection.distinct(field, Document.class).filter(new MDocument(query)).into(new ArrayList<Document>());
        }
    }

    public static final class MCursor implements Iterable<Map<String, ?>>, Iterator<Map<String, ?>>
    {
        private final FindIterable<Document> m_finder;

        private final MongoCursor<Document>  m_cursor;

        private boolean                      m_closed    = false;

        private boolean                      m_autoclose = true;

        protected MCursor(FindIterable<Document> finder)
        {
            m_finder = finder;

            m_cursor = m_finder.iterator();
        }

        public void close() throws Exception
        {
            if (false == m_closed)
            {
                m_cursor.close();

                m_closed = true;
            }
        }

        public MCursor setAutoClose(boolean autoclose)
        {
            m_autoclose = autoclose;

            return this;
        }

        @Override
        public Iterator<Map<String, ?>> iterator()
        {
            return this;
        }

        @Override
        public boolean hasNext()
        {
            boolean next = ((m_closed == false) && (m_cursor.hasNext()));

            if ((false == next) && (false == m_closed) && (m_autoclose))
            {
                try
                {
                    close();
                }
                catch (Exception e)
                {
                    logger.error("Error in MCursor.close() ", e);
                }
            }
            return next;
        }

        @Override
        public Map<String, ?> next()
        {
            return m_cursor.next();
        }

        @Override
        public void remove()
        {
            m_cursor.remove();
        }

        public MCursor skip(final int skip)
        {
            return new MCursor(m_finder.skip(skip));
        }

        public MCursor limit(final int limit)
        {
            return new MCursor(m_finder.limit(limit));
        }

        public MCursor sort(final Map<String, ?> sort)
        {
            return sort(new MSort(sort));
        }

        public MCursor sort(final MSort sort)
        {
            return new MCursor(m_finder.sort(sort));
        }
    }

    public static final class MDocument extends Document
    {
        private static final long serialVersionUID = -3704882524991565448L;

        public MDocument()
        {
        }

        @SuppressWarnings("unchecked")
        public MDocument(final Map<String, ?> map)
        {
            super((Map<String, Object>) map);
        }
    }

    @SuppressWarnings("serial")
    public static final class MSort extends Document
    {
        private static final BsonInt32 ORDER_A = new BsonInt32(0 + 1);

        private static final BsonInt32 ORDER_D = new BsonInt32(0 - 1);

        private MSort()
        {
        }

        @SuppressWarnings("unchecked")
        public MSort(final Map<String, ?> map)
        {
            super((Map<String, Object>) map);
        }

        public static final MSort ASCENDING(final String... fields)
        {
            return ASCENDING(Arrays.asList(fields));
        }

        public static final MSort ASCENDING(final List<String> fields)
        {
            return ORDER_BY(fields, ORDER_A);
        }

        public static final MSort DESCENDING(final String... fields)
        {
            return DESCENDING(Arrays.asList(fields));
        }

        public static final MSort DESCENDING(final List<String> fields)
        {
            return ORDER_BY(fields, ORDER_D);
        }

        public static final MSort ORDER_BY(final MSort... sorts)
        {
            return ORDER_BY(Arrays.asList(sorts));
        }

        public static final MSort ORDER_BY(final List<MSort> sorts)
        {
            final MSort sort = new MSort();

            for (MSort s : sorts)
            {
                for (String k : s.keySet())
                {
                    sort.remove(k);

                    sort.append(k, s.get(k));
                }
            }
            return sort;
        }

        private static final MSort ORDER_BY(final List<String> fields, final BsonInt32 value)
        {
            final MSort sort = new MSort();

            for (String name : fields)
            {
                sort.remove(name);

                sort.append(name, value);
            }
            return sort;
        }
    }

    @SuppressWarnings("serial")
    public static final class MProjection extends Document
    {
        private static final BsonInt32 INCLUDE_N = new BsonInt32(0);

        private static final BsonInt32 INCLUDE_Y = new BsonInt32(1);

        private MProjection()
        {
        }

        @SuppressWarnings("unchecked")
        public MProjection(final Map<String, ?> map)
        {
            super((Map<String, Object>) map);
        }

        private MProjection(final String name, final BsonValue value)
        {
            super(name, value);
        }

        public static final MProjection INCLUDE(final String... fields)
        {
            return INCLUDE(Arrays.asList(fields));
        }

        public static final MProjection INCLUDE(final List<String> fields)
        {
            return COMBINE(fields, INCLUDE_Y);
        }

        public static final MProjection EXCLUDE(final String... fields)
        {
            return EXCLUDE(Arrays.asList(fields));
        }

        public static final MProjection EXCLUDE(final List<String> fields)
        {
            return COMBINE(fields, INCLUDE_N);
        }

        public static final MProjection NO_ID()
        {
            return new MProjection("_id", INCLUDE_N);
        }

        public static final MProjection FIELDS(final MProjection... projections)
        {
            return FIELDS(Arrays.asList(projections));
        }

        public static final MProjection FIELDS(final List<MProjection> projections)
        {
            final MProjection projection = new MProjection();

            for (MProjection p : projections)
            {
                for (String k : p.keySet())
                {
                    projection.remove(k);

                    projection.append(k, p.get(k));
                }
            }
            return projection;
        }

        private static final MProjection COMBINE(final List<String> fields, final BsonInt32 value)
        {
            final MProjection projection = new MProjection();

            for (String name : fields)
            {
                projection.remove(name);

                projection.append(name, value);
            }
            return projection;
        }
    }

    @SuppressWarnings("serial")
    public static class MQuery extends Document
    {
        private MQuery()
        {
        }

        @SuppressWarnings("unchecked")
        public MQuery(final Map<String, ?> map)
        {
            super((Map<String, Object>) map);
        }

        public static final <T> MQuery EQ(String name, T value)
        {
            return convert(eq(name, value));
        }

        public static final <T> MQuery NE(String name, T value)
        {
            return convert(ne(name, value));
        }

        public static final <T> MQuery GT(String name, T value)
        {
            return convert(gt(name, value));
        }

        public static final <T> MQuery LT(String name, T value)
        {
            return convert(lt(name, value));
        }

        public static final <T> MQuery GTE(String name, T value)
        {
            return convert(gte(name, value));
        }

        public static final <T> MQuery LTE(String name, T value)
        {
            return convert(lte(name, value));
        }

        @SafeVarargs
        public static final <T> MQuery IN(String name, T... list)
        {
            return IN(name, Arrays.asList(list));
        }

        public static final <T> MQuery IN(String name, List<T> list)
        {
            return convert(in(name, list));
        }

        @SafeVarargs
        public static final <T> MQuery NIN(String name, T... list)
        {
            return NIN(name, Arrays.asList(list));
        }

        public static final <T> MQuery NIN(String name, List<T> list)
        {
            return convert(nin(name, list));
        }

        public static final MQuery AND(MQuery... list)
        {
            return AND(Arrays.asList(list));
        }

        public static final MQuery AND(List<MQuery> list)
        {
            return convert(and(new ArrayList<Bson>(list)));
        }

        public static final MQuery OR(MQuery... list)
        {
            return OR(Arrays.asList(list));
        }

        public static final MQuery OR(List<MQuery> list)
        {
            return convert(or(new ArrayList<Bson>(list)));
        }

        public static final MQuery NOT(MQuery query)
        {
            return convert(not(query));
        }

        public static final MQuery EXISTS(String name, boolean exists)
        {
            return convert(exists(name, exists));
        }

        public static final MQuery EXISTS(String name)
        {
            return convert(exists(name, true));
        }

        private static final MQuery convert(final Bson b)
        {
            return new MQuery()
            {
                @Override
                public <TDocument> BsonDocument toBsonDocument(final Class<TDocument> documentClass, final CodecRegistry codecRegistry)
                {
                    return b.toBsonDocument(documentClass, codecRegistry);
                }
            };
        }
    }
}
/*
 * Copyright (c) 2014,2015 Ahome' Innovation Technologies. All rights reserved.
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

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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

import com.ait.tooling.common.api.java.util.StringOps;
import com.ait.tooling.json.JSONUtils;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.UpdateOptions;

public final class MongoDB implements Serializable
{
    private static final long   serialVersionUID = 2378917194666504499L;

    private static final Logger logger           = Logger.getLogger(MongoDB.class);

    private final MongoClient   m_mongo;

    private final String        m_usedb;

    private boolean             m_id             = false;

    public MongoDB(final MongoClientOptions options, final String host, final int port, final String usedb)
    {
        m_usedb = StringOps.requireTrimOrNull(usedb);

        BSON.addEncodingHook(BigDecimal.class, new Transformer()
        {
            @Override
            public Object transform(final Object object)
            {
                if (null == object)
                {
                    return null;
                }
                return JSONUtils.asDouble(object);
            }
        });
        BSON.addEncodingHook(BigInteger.class, new Transformer()
        {
            @Override
            public Object transform(Object object)
            {
                if (null == object)
                {
                    return null;
                }
                Long lval = JSONUtils.asLong(object);

                if (null != lval)
                {
                    return lval;
                }
                return JSONUtils.asInteger(object);
            }
        });
        m_mongo = new MongoClient(StringOps.requireTrimOrNull(host), Objects.requireNonNull(options));
    }

    public void setAddingID(final boolean id)
    {
        m_id = id;
    }

    public boolean isAddingID()
    {
        return m_id;
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

    public final MDatabase db(final String name) throws Exception
    {
        return db(StringOps.requireTrimOrNull(name), false, isAddingID());
    }

    public final MDatabase db() throws Exception
    {
        return db(m_usedb, false, isAddingID());
    }

    public final MDatabase db(final String name, final boolean auth, final boolean id) throws Exception
    {
        return new MDatabase(m_mongo.getDatabase(StringOps.requireTrimOrNull(name)), auth, id);
    }

    public static final class MDatabase
    {
        private final MongoDatabase m_db;

        private final boolean       m_id;

        protected MDatabase(final MongoDatabase db, final boolean auth, final boolean id) throws Exception
        {
            m_db = Objects.requireNonNull(db);

            m_id = id;
        }

        public boolean isAddingID()
        {
            return m_id;
        }

        public final String getName()
        {
            return m_db.getName();
        }

        public final void drop()
        {
            m_db.drop();
        }

        public final boolean isCollection(final String name)
        {
            return getCollectionNames().contains(StringOps.requireTrimOrNull(name));
        }

        public final List<String> getCollectionNames()
        {
            return m_db.listCollectionNames().into(new ArrayList<String>());
        }

        public final MCollection collection(final String name) throws Exception
        {
            return new MCollection(m_db.getCollection(StringOps.requireTrimOrNull(name)), isAddingID());
        }
    }

    public static final class MCollection
    {
        private final MongoCollection<Document> m_collection;

        private final boolean                   m_id;

        protected MCollection(final MongoCollection<Document> collection, final boolean id)
        {
            m_collection = Objects.requireNonNull(collection);

            m_id = id;
        }

        public boolean isAddingID()
        {
            return m_id;
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

        protected final MAggregateCursor aggregateRaw(final List<? extends Bson> list)
        {
            return new MAggregateCursor(m_collection.aggregate(Objects.requireNonNull(list)));
        }

        public final void drop()
        {
            m_collection.drop();
        }

        public final MCollection deleteMany(final Map<String, ?> query)
        {
            return deleteMany(new MQuery(Objects.requireNonNull(query)));
        }

        public final MCollection deleteMany(final MQuery query)
        {
            m_collection.deleteMany(Objects.requireNonNull(query));

            return this;
        }

        public final MCollection deleteOne(final Map<String, ?> query)
        {
            return deleteOne(new MQuery(Objects.requireNonNull(query)));
        }

        public final MCollection deleteOne(final MQuery query)
        {
            m_collection.deleteOne(Objects.requireNonNull(query));

            return this;
        }

        @SuppressWarnings("unchecked")
        public final Map<String, ?> ensureHasID(final Map<String, ?> update)
        {
            Objects.requireNonNull(update);

            final Object id = update.get("id");

            if ((false == (id instanceof String)) || (null == StringOps.toTrimOrNull(id.toString())))
            {
                ((Map<String, Object>) update).put("id", (new ObjectId()).toString());
            }
            return update;
        }

        public final Map<String, ?> insertOne(final Map<String, ?> record)
        {
            if (isAddingID())
            {
                final Map<String, ?> withid = ensureHasID(Objects.requireNonNull(record));

                m_collection.insertOne(new MDocument(Objects.requireNonNull(withid)));

                return withid;
            }
            else
            {
                m_collection.insertOne(new MDocument(Objects.requireNonNull(record)));

                return record;
            }
        }

        public final MCollection insertMany(final List<Map<String, ?>> list)
        {
            Objects.requireNonNull(list);

            if (list.isEmpty())
            {
                logger.warn("MCollection.insertMany(empty)");

                return this;
            }
            if (1 == list.size())
            {
                insertOne(Objects.requireNonNull(list.get(0))); // let this do checkID

                return this;
            }
            final ArrayList<MDocument> save = new ArrayList<MDocument>(list.size());

            if (isAddingID())
            {
                for (Map<String, ?> lmap : list)
                {
                    save.add(new MDocument(ensureHasID(Objects.requireNonNull(lmap))));
                }
            }
            else
            {
                for (Map<String, ?> lmap : list)
                {
                    save.add(new MDocument(Objects.requireNonNull(lmap)));
                }
            }
            m_collection.insertMany(save);

            return this;
        }

        public final long count()
        {
            return m_collection.count();
        }

        public final long count(final Map<String, ?> query)
        {
            return count(new MQuery(Objects.requireNonNull(query)));
        }

        public final long count(final MQuery query)
        {
            return m_collection.count(Objects.requireNonNull(query));
        }

        public final MCursor find() throws Exception
        {
            return find(false);
        }

        final String getNameSpace()
        {
            return m_collection.getNamespace().toString();
        }

        public final MCursor find(final boolean with_id) throws Exception
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

        public final MCursor find(final Map<String, ?> query) throws Exception
        {
            return find(new MQuery(Objects.requireNonNull(query)), false);
        }

        public final MCursor find(final MQuery query) throws Exception
        {
            return find(Objects.requireNonNull(query), false);
        }

        public final MCursor find(final Map<String, ?> query, final boolean with_id) throws Exception
        {
            return find(new MQuery(Objects.requireNonNull(query)), with_id);
        }

        public final MCursor find(final MQuery query, final boolean with_id) throws Exception
        {
            if (with_id)
            {
                return new MCursor(m_collection.find(Objects.requireNonNull(query)));
            }
            else
            {
                return new MCursor(m_collection.find(Objects.requireNonNull(query)).projection(MProjection.NO_ID()));
            }
        }

        public final MCursor find(final Map<String, ?> query, final Map<String, ?> fields) throws Exception
        {
            return find(new MQuery(Objects.requireNonNull(query)), new MProjection(Objects.requireNonNull(fields)));
        }

        public final MCursor find(final MQuery query, final Map<String, ?> fields) throws Exception
        {
            return find(Objects.requireNonNull(query), new MProjection(Objects.requireNonNull(fields)));
        }

        public final MCursor find(final Map<String, ?> query, final MProjection fields) throws Exception
        {
            return find(new MQuery(Objects.requireNonNull(query)), Objects.requireNonNull(fields), false);
        }

        public final MCursor find(final MQuery query, final MProjection fields) throws Exception
        {
            return find(Objects.requireNonNull(query), Objects.requireNonNull(fields), false);
        }

        public final MCursor find(final Map<String, ?> query, final Map<String, ?> fields, final boolean with_id) throws Exception
        {
            return find(new MQuery(Objects.requireNonNull(query)), new MProjection(Objects.requireNonNull(fields)), with_id);
        }

        public final MCursor find(final Map<String, ?> query, final MProjection fields, final boolean with_id) throws Exception
        {
            return find(new MQuery(Objects.requireNonNull(query)), Objects.requireNonNull(fields), with_id);
        }

        public final MCursor find(final MQuery query, final MProjection fields, final boolean with_id) throws Exception
        {
            if (with_id)
            {
                return new MCursor(m_collection.find(Objects.requireNonNull(query)).projection(Objects.requireNonNull(fields)));
            }
            else
            {
                return new MCursor(m_collection.find(Objects.requireNonNull(query)).projection(MProjection.FIELDS(Objects.requireNonNull(fields), MProjection.NO_ID())));
            }
        }

        public final Map<String, ?> findAndModify(final Map<String, ?> query, final Map<String, ?> update)
        {
            return update(new MQuery(Objects.requireNonNull(query)), Objects.requireNonNull(update), false, true);
        }

        public final Map<String, ?> findAndModify(final MQuery query, final Map<String, ?> update)
        {
            return update(Objects.requireNonNull(query), Objects.requireNonNull(update), false, true);
        }

        public final Map<String, ?> upsert(final Map<String, ?> query, final Map<String, ?> update)
        {
            return upsert(new MQuery(Objects.requireNonNull(query)), Objects.requireNonNull(update));
        }

        public final Map<String, ?> upsert(final MQuery query, final Map<String, ?> update)
        {
            return update(Objects.requireNonNull(query), Objects.requireNonNull(update), true, true);
        }

        public final Map<String, ?> create(final Map<String, ?> record)
        {
            return insertOne(Objects.requireNonNull(record));
        }

        public final Map<String, ?> update(final Map<String, ?> query, final Map<String, ?> update, final boolean upsert, final boolean multi)
        {
            return update(new MQuery(Objects.requireNonNull(query)), Objects.requireNonNull(update), upsert, multi);
        }

        public final Map<String, ?> update(final MQuery query, final Map<String, ?> update, final boolean upsert, final boolean multi)
        {
            if (multi)
            {
                m_collection.updateMany(Objects.requireNonNull(query), new MDocument(Objects.requireNonNull(update)), new UpdateOptions().upsert(upsert));
            }
            else
            {
                m_collection.updateOne(Objects.requireNonNull(query), new MDocument(Objects.requireNonNull(update)), new UpdateOptions().upsert(upsert));
            }
            return update;
        }

        public final Map<String, ?> findOne(final Map<String, ?> query)
        {
            return findOne(new MQuery(Objects.requireNonNull(query)));
        }

        public final Map<String, ?> findOne(final MQuery query)
        {
            FindIterable<Document> iter = m_collection.find(Objects.requireNonNull(query)).limit(1).projection(MProjection.NO_ID());

            if (null != iter)
            {
                return iter.first();
            }
            return null;
        }

        public final boolean updateOne(final Map<String, ?> query, final Map<String, ?> update)
        {
            return updateOne(new MQuery(Objects.requireNonNull(query)), Objects.requireNonNull(update));
        }

        public final boolean updateOne(final MQuery query, final Map<String, ?> update)
        {
            return (m_collection.updateOne(Objects.requireNonNull(query), new MDocument(Objects.requireNonNull(update))).getModifiedCount() == 1L);
        }

        public final long updateMany(final Map<String, ?> query, final Map<String, ?> update)
        {
            return updateMany(new MQuery(Objects.requireNonNull(query)), Objects.requireNonNull(update));
        }

        public final long updateMany(final MQuery query, final Map<String, ?> update)
        {
            return m_collection.updateMany(Objects.requireNonNull(query), new MDocument(Objects.requireNonNull(update)), new UpdateOptions().upsert(false)).getModifiedCount();
        }

        public final List<?> distinct(final String field)
        {
            return m_collection.distinct(StringOps.requireTrimOrNull(field), Document.class).into(new ArrayList<Document>());
        }

        public final List<?> distinct(final String field, final Map<String, ?> query)
        {
            Objects.requireNonNull(query);

            return m_collection.distinct(StringOps.requireTrimOrNull(field), Document.class).filter(new MDocument(query)).into(new ArrayList<Document>());
        }
    }

    public static interface IMCursor extends Iterable<Map<String, ?>>, Iterator<Map<String, ?>>, Closeable
    {
        public <A extends Collection<? super Map<String, ?>>> A into(A target);
    }

    protected static abstract class AbstractMCursor<T extends MongoIterable<Document>> implements IMCursor
    {
        private final T                     m_iterab;

        private final MongoCursor<Document> m_cursor;

        private boolean                     m_closed    = false;

        private boolean                     m_autoclose = true;

        protected AbstractMCursor(final T iter)
        {
            m_iterab = Objects.requireNonNull(iter);

            m_cursor = Objects.requireNonNull(m_iterab.iterator());
        }

        protected final T self()
        {
            return m_iterab;
        }

        @Override
        public <A extends Collection<? super Map<String, ?>>> A into(A target)
        {
            final A result = m_iterab.into(target);

            try
            {
                close();
            }
            catch (IOException e)
            {
                logger.error("Error in AbstractMCursor.into() ", e);
            }
            return result;
        }

        @Override
        public Iterator<Map<String, ?>> iterator()
        {
            return this;
        }

        @Override
        public boolean hasNext()
        {
            final boolean next = ((m_closed == false) && (m_cursor.hasNext()));

            if ((false == next) && (false == m_closed) && (m_autoclose))
            {
                try
                {
                    close();
                }
                catch (Exception e)
                {
                    logger.error("Error in AbstractMCursor.close() ", e);
                }
            }
            return next;
        }

        public void setAutoClose(final boolean autoclose)
        {
            m_autoclose = autoclose;
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

        @Override
        public void close() throws IOException
        {
            if (false == m_closed)
            {
                m_cursor.close();

                m_closed = true;
            }
        }
    }

    public static final class MAggregateCursor extends AbstractMCursor<AggregateIterable<Document>>
    {
        protected MAggregateCursor(final AggregateIterable<Document> aggreg)
        {
            super(aggreg);
        }
    }

    public static final class MCursor extends AbstractMCursor<FindIterable<Document>>
    {
        protected MCursor(final FindIterable<Document> finder)
        {
            super(finder);
        }

        public MCursor projection(final MProjection projection)
        {
            return new MCursor(self().projection(Objects.requireNonNull(projection)));
        }

        public MCursor skip(final int skip)
        {
            return new MCursor(self().skip(Math.max(0, skip)));
        }

        public MCursor limit(final int limit)
        {
            return new MCursor(self().limit(Math.max(0, limit)));
        }

        public MCursor sort(final Map<String, ?> sort)
        {
            return sort(new MSort(Objects.requireNonNull(sort)));
        }

        public MCursor sort(final MSort sort)
        {
            return new MCursor(self().sort(Objects.requireNonNull(sort)));
        }
    }

    @SuppressWarnings("serial")
    public static final class MDocument extends Document
    {
        public MDocument()
        {
        }

        @SuppressWarnings("unchecked")
        public MDocument(final Map<String, ?> map)
        {
            super(Objects.requireNonNull((Map<String, Object>) map));
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
            return ORDER_BY(Objects.requireNonNull(fields), ORDER_A);
        }

        public static final MSort DESCENDING(final String... fields)
        {
            return DESCENDING(Arrays.asList(fields));
        }

        public static final MSort DESCENDING(final List<String> fields)
        {
            return ORDER_BY(Objects.requireNonNull(fields), ORDER_D);
        }

        public static final MSort ORDER_BY(final MSort... sorts)
        {
            return ORDER_BY(Arrays.asList(sorts));
        }

        public static final MSort ORDER_BY(final List<MSort> sorts)
        {
            Objects.requireNonNull(sorts);

            final MSort sort = new MSort();

            for (MSort s : sorts)
            {
                if (null != s)
                {
                    for (String k : s.keySet())
                    {
                        if (null != StringOps.toTrimOrNull(k))
                        {
                            sort.remove(k);

                            sort.append(k, s.get(k));
                        }
                    }
                }
                else
                {
                    logger.warn("MSort.ORDER_BY(null)");
                }
            }
            return sort;
        }

        private static final MSort ORDER_BY(final List<String> fields, final BsonInt32 value)
        {
            Objects.requireNonNull(fields);

            final MSort sort = new MSort();

            for (String name : fields)
            {
                if (null != StringOps.toTrimOrNull(name))
                {
                    sort.remove(name);

                    sort.append(name, value);
                }
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
            super(Objects.requireNonNull((Map<String, Object>) map));
        }

        private MProjection(final String name, final BsonValue value)
        {
            super(StringOps.requireTrimOrNull(name), value);
        }

        public static final MProjection INCLUDE(final String... fields)
        {
            return INCLUDE(Arrays.asList(fields));
        }

        public static final MProjection INCLUDE(final List<String> fields)
        {
            return COMBINE(Objects.requireNonNull(fields), INCLUDE_Y);
        }

        public static final MProjection EXCLUDE(final String... fields)
        {
            return EXCLUDE(Arrays.asList(fields));
        }

        public static final MProjection EXCLUDE(final List<String> fields)
        {
            return COMBINE(Objects.requireNonNull(fields), INCLUDE_N);
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
                    if (null != (k = StringOps.toTrimOrNull(k)))
                    {
                        projection.remove(k);

                        projection.append(k, p.get(k));
                    }
                }
            }
            return projection;
        }

        private static final MProjection COMBINE(final List<String> fields, final BsonInt32 value)
        {
            final MProjection projection = new MProjection();

            for (String name : fields)
            {
                if (null != (name = StringOps.toTrimOrNull(name)))
                {
                    projection.remove(name);

                    projection.append(name, value);
                }
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
            super(Objects.requireNonNull((Map<String, Object>) map));
        }

        public static final <T> MQuery EQ(final String name, final T value)
        {
            return convert(eq(StringOps.requireTrimOrNull(name), value));
        }

        public static final <T> MQuery NE(final String name, final T value)
        {
            return convert(ne(StringOps.requireTrimOrNull(name), value));
        }

        public static final <T> MQuery GT(final String name, final T value)
        {
            return convert(gt(StringOps.requireTrimOrNull(name), value));
        }

        public static final <T> MQuery LT(final String name, final T value)
        {
            return convert(lt(StringOps.requireTrimOrNull(name), value));
        }

        public static final <T> MQuery GTE(final String name, final T value)
        {
            return convert(gte(StringOps.requireTrimOrNull(name), value));
        }

        public static final <T> MQuery LTE(final String name, final T value)
        {
            return convert(lte(StringOps.requireTrimOrNull(name), value));
        }

        @SafeVarargs
        public static final <T> MQuery IN(final String name, final T... list)
        {
            return IN(StringOps.requireTrimOrNull(name), Arrays.asList(list));
        }

        public static final <T> MQuery IN(final String name, final List<T> list)
        {
            return convert(in(StringOps.requireTrimOrNull(name), Objects.requireNonNull(list)));
        }

        @SafeVarargs
        public static final <T> MQuery NIN(String name, T... list)
        {
            return NIN(StringOps.requireTrimOrNull(name), Arrays.asList(list));
        }

        public static final <T> MQuery NIN(final String name, final List<T> list)
        {
            return convert(nin(StringOps.requireTrimOrNull(name), Objects.requireNonNull(list)));
        }

        public static final MQuery AND(final MQuery... list)
        {
            return AND(Arrays.asList(list));
        }

        public static final MQuery AND(final List<MQuery> list)
        {
            return convert(and(new ArrayList<Bson>(Objects.requireNonNull(list))));
        }

        public static final MQuery OR(final MQuery... list)
        {
            return OR(Arrays.asList(list));
        }

        public static final MQuery OR(final List<MQuery> list)
        {
            return convert(or(new ArrayList<Bson>(Objects.requireNonNull(list))));
        }

        public static final MQuery NOT(final MQuery query)
        {
            return convert(not(Objects.requireNonNull(query)));
        }

        public static final MQuery EXISTS(final String name, final boolean exists)
        {
            return convert(exists(StringOps.requireTrimOrNull(name), exists));
        }

        public static final MQuery EXISTS(final String name)
        {
            return convert(exists(StringOps.requireTrimOrNull(name), true));
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
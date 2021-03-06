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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
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
import com.ait.tooling.server.core.json.JSONUtils;
import com.ait.tooling.server.mongodb.support.spring.IMongoDBCollectionOptions;
import com.ait.tooling.server.mongodb.support.spring.IMongoDBOptions;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.ListIndexesIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.UpdateOptions;

public final class MongoDB
{
    private static final Logger                logger = Logger.getLogger(MongoDB.class);

    private final MongoClient                  m_mongo;

    private final String                       m_usedb;

    private final boolean                      m_useid;

    private final Map<String, IMongoDBOptions> m_dbops;

    @SuppressWarnings("unchecked")
    private static final Map<String, Object> CAST_MAP(Map<String, ?> map)
    {
        return (Map<String, Object>) Objects.requireNonNull(map);
    }

    public MongoDB(final List<ServerAddress> addr, final List<MongoCredential> auth, final MongoClientOptions opts, final boolean repl, final String usedb, final boolean useid, final Map<String, IMongoDBOptions> dbops)
    {
        m_useid = useid;

        m_dbops = Objects.requireNonNull(dbops);

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
        if (addr.isEmpty())
        {
            throw new IllegalArgumentException("no ServerAddress");
        }
        if ((addr.size() == 1) && (false == repl))
        {
            final ServerAddress main = addr.get(0);

            if (null == main)
            {
                throw new IllegalArgumentException("null ServerAddress");
            }
            if ((null == auth) || (auth.isEmpty()))
            {
                m_mongo = new MongoClient(main, Objects.requireNonNull(opts));
            }
            else
            {
                m_mongo = new MongoClient(main, auth, Objects.requireNonNull(opts));
            }
        }
        else
        {
            if ((null == auth) || (auth.isEmpty()))
            {
                m_mongo = new MongoClient(addr, Objects.requireNonNull(opts));
            }
            else
            {
                m_mongo = new MongoClient(addr, auth, Objects.requireNonNull(opts));
            }
        }
    }

    public boolean isAddingID()
    {
        return m_useid;
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
        return db(StringOps.requireTrimOrNull(name), isAddingID());
    }

    public final MDatabase db() throws Exception
    {
        return db(m_usedb, isAddingID());
    }

    public final MDatabase db(String name, boolean id) throws Exception
    {
        name = StringOps.requireTrimOrNull(name);

        IMongoDBOptions op = m_dbops.get(name);

        if (null != op)
        {
            id = op.isCreateID();
        }
        return new MDatabase(m_mongo.getDatabase(name), id, op);
    }

    public static final class MDatabase
    {
        private final MongoDatabase   m_db;

        private final IMongoDBOptions m_op;

        private final boolean         m_id;

        protected MDatabase(final MongoDatabase db, final boolean id, final IMongoDBOptions op) throws Exception
        {
            m_id = id;

            m_op = op;

            m_db = Objects.requireNonNull(db);
        }

        public boolean isCreateID()
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

        public final MCollection collection(String name) throws Exception
        {
            name = StringOps.requireTrimOrNull(name);

            if (null != m_op)
            {
                final IMongoDBCollectionOptions cops = m_op.getCollectionOptions(name);

                if (null != cops)
                {
                    return new MCollection(m_db.getCollection(name), cops.isCreateID());
                }
            }
            return new MCollection(m_db.getCollection(name), isCreateID());
        }

        public final MCollection collection(String name, final MCollectionPreferences opts) throws Exception
        {
            name = StringOps.requireTrimOrNull(name);

            boolean crid = isCreateID();

            if (null != m_op)
            {
                final IMongoDBCollectionOptions cops = m_op.getCollectionOptions(name);

                if (null != cops)
                {
                    crid = cops.isCreateID();
                }
                if ((null != opts) && (opts.isValid()))
                {
                    return opts.withCollectionOptions(m_db.getCollection(name), crid);
                }
            }
            return new MCollection(m_db.getCollection(name), crid);
        }
    }

    public static final class MCollectionPreferences
    {
        private final WriteConcern   m_write;

        private final ReadPreference m_prefs;

        private final CodecRegistry  m_codec;

        public MCollectionPreferences(final WriteConcern write, final ReadPreference prefs, final CodecRegistry codec)
        {
            m_write = write;

            m_prefs = prefs;

            m_codec = codec;
        }

        public MCollectionPreferences(final WriteConcern write)
        {
            this(write, null, null);
        }

        public MCollectionPreferences(final ReadPreference prefs)
        {
            this(null, prefs, null);
        }

        public MCollectionPreferences(final CodecRegistry codec)
        {
            this(null, null, codec);
        }

        public MCollectionPreferences(final WriteConcern write, final ReadPreference prefs)
        {
            this(write, prefs, null);
        }

        public MCollectionPreferences(final WriteConcern write, final CodecRegistry codec)
        {
            this(write, null, codec);
        }

        public MCollectionPreferences(final ReadPreference prefs, final CodecRegistry codec)
        {
            this(null, prefs, codec);
        }

        final boolean isValid()
        {
            return (false == ((null == m_write) && (null == m_prefs) && (null == m_codec)));
        }

        final MCollection withCollectionOptions(final MongoCollection<Document> collection, boolean id)
        {
            return new MCollection(withCodecRegistry(withReadPreference(withWriteConcern(collection, m_write), m_prefs), m_codec), id);
        }

        private final static MongoCollection<Document> withWriteConcern(final MongoCollection<Document> collection, final WriteConcern write)
        {
            if (null == write)
            {
                return collection;
            }
            return collection.withWriteConcern(write);
        }

        private final static MongoCollection<Document> withReadPreference(final MongoCollection<Document> collection, final ReadPreference prefs)
        {
            if (null == prefs)
            {
                return collection;
            }
            return collection.withReadPreference(prefs);
        }

        private final static MongoCollection<Document> withCodecRegistry(final MongoCollection<Document> collection, final CodecRegistry codec)
        {
            if (null == codec)
            {
                return collection;
            }
            return collection.withCodecRegistry(codec);
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

        public boolean isCreateID()
        {
            return m_id;
        }

        public final String getName()
        {
            return m_collection.getNamespace().getCollectionName();
        }

        public final String createIndex(final Map<String, ?> keys)
        {
            return m_collection.createIndex(new Document(CAST_MAP(keys)));
        }

        public final String createIndex(final Map<String, ?> keys, final String name)
        {
            return m_collection.createIndex(new Document(CAST_MAP(keys)), new IndexOptions().name(Objects.requireNonNull(name)));
        }

        public final String createIndex(final Map<String, ?> keys, final IndexOptions opts)
        {
            return m_collection.createIndex(new Document(CAST_MAP(keys)), Objects.requireNonNull(opts));
        }

        public final MCollection dropIndex(final String name)
        {
            m_collection.dropIndex(Objects.requireNonNull(name));

            return this;
        }

        public final MCollection dropIndexes()
        {
            m_collection.dropIndexes();

            return this;
        }

        public final MIndexCursor getIndexes()
        {
            return new MIndexCursor(m_collection.listIndexes());
        }

        @SafeVarargs
        public final <T extends Document> MAggregateCursor aggregate(final T... list)
        {
            return aggregate(new MAggregationPipeline(Objects.requireNonNull(list)));
        }

        public final <T extends Document> MAggregateCursor aggregate(final List<T> list)
        {
            return aggregate(new MAggregationPipeline(Objects.requireNonNull(list)));
        }

        public final MAggregateCursor aggregate(final MAggregationPipeline pipeline)
        {
            return new MAggregateCursor(m_collection.aggregate(Objects.requireNonNull(pipeline.list())));
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
            if (isCreateID())
            {
                final Map<String, ?> withid = ensureHasID(Objects.requireNonNull(record));

                m_collection.insertOne(new Document(CAST_MAP(withid)));

                return withid;
            }
            else
            {
                m_collection.insertOne(new Document(CAST_MAP(record)));

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
                insertOne(Objects.requireNonNull(list.get(0)));// let this do checkID

                return this;
            }
            final ArrayList<Document> save = new ArrayList<Document>(list.size());

            if (isCreateID())
            {
                for (Map<String, ?> lmap : list)
                {
                    save.add(new Document(CAST_MAP(ensureHasID(lmap))));
                }
            }
            else
            {
                for (Map<String, ?> lmap : list)
                {
                    save.add(new Document(CAST_MAP(ensureHasID(lmap))));
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
                m_collection.updateMany(Objects.requireNonNull(query), new Document(CAST_MAP(update)), new UpdateOptions().upsert(upsert));
            }
            else
            {
                m_collection.updateOne(Objects.requireNonNull(query), new Document(CAST_MAP(update)), new UpdateOptions().upsert(upsert));
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
            return (m_collection.updateOne(Objects.requireNonNull(query), new Document(CAST_MAP(update))).getModifiedCount() == 1L);
        }

        public final long updateMany(final Map<String, ?> query, final Map<String, ?> update)
        {
            return updateMany(new MQuery(Objects.requireNonNull(query)), Objects.requireNonNull(update));
        }

        public final long updateMany(final MQuery query, final Map<String, ?> update)
        {
            return m_collection.updateMany(Objects.requireNonNull(query), new Document(CAST_MAP(update)), new UpdateOptions().upsert(false)).getModifiedCount();
        }

        public final List<?> distinct(final String field)
        {
            return m_collection.distinct(StringOps.requireTrimOrNull(field), Document.class).into(new ArrayList<Document>());
        }

        public final List<?> distinct(final String field, final Map<String, ?> query)
        {
            return m_collection.distinct(StringOps.requireTrimOrNull(field), Document.class).filter(new Document(CAST_MAP(query))).into(new ArrayList<Document>());
        }
    }

    @SuppressWarnings("serial")
    private static class MAggregationOp extends Document
    {
        private MAggregationOp(final Document doc)
        {
            super(Objects.requireNonNull(doc));
        }

        protected static final MAggregationOp makeAggregationOp(final String op, final Map<String, ?> map)
        {
            final LinkedHashMap<String, Object> make = new LinkedHashMap<String, Object>(1);

            make.put(Objects.requireNonNull(op), Objects.requireNonNull(map));

            return new MAggregationOp(new Document(make));
        }

        protected static final MAggregationOp makeAggregationOp(final String op, final Document doc)
        {
            final LinkedHashMap<String, Object> make = new LinkedHashMap<String, Object>(1);

            make.put(Objects.requireNonNull(op), Objects.requireNonNull(doc));

            return new MAggregationOp(new Document(make));
        }

        public static final MAggregationMatch MATCH(final Map<String, ?> map)
        {
            return new MAggregationMatch(Objects.requireNonNull(map));
        }

        public static final MAggregationMatch MATCH(final Document doc)
        {
            return new MAggregationMatch(Objects.requireNonNull(doc));
        }

        public static final MAggregationGroup GROUP(final Map<String, ?> map)
        {
            return new MAggregationGroup(Objects.requireNonNull(map));
        }

        public static final MAggregationGroup GROUP(final Document doc)
        {
            return new MAggregationGroup(Objects.requireNonNull(doc));
        }
    }

    public static final class MAggregationGroup extends MAggregationOp
    {
        private static final long serialVersionUID = 3079372174680166319L;

        public MAggregationGroup(final Map<String, ?> map)
        {
            super(makeAggregationOp("$group", Objects.requireNonNull(map)));
        }

        public MAggregationGroup(final Document doc)
        {
            super(makeAggregationOp("$group", Objects.requireNonNull(doc)));
        }
    }

    public static final class MAggregationMatch extends MAggregationOp
    {
        private static final long serialVersionUID = -1138722876045817851L;

        public MAggregationMatch(final Map<String, ?> map)
        {
            super(makeAggregationOp("$match", Objects.requireNonNull(map)));
        }

        public MAggregationMatch(final Document doc)
        {
            super(makeAggregationOp("$match", Objects.requireNonNull(doc)));
        }
    }

    public static final class MAggregationPipeline
    {
        private final ArrayList<Document> m_pipeline = new ArrayList<Document>();

        public <T extends Document> MAggregationPipeline(final List<T> list)
        {
            m_pipeline.addAll(Objects.requireNonNull(list));
        }

        @SafeVarargs
        public <T extends Document> MAggregationPipeline(final T... list)
        {
            m_pipeline.addAll(Arrays.asList(Objects.requireNonNull(list)));
        }

        List<Document> list()
        {
            return m_pipeline;
        }
    }

    public static interface IMCursor extends Iterable<Map<String, ?>>, Iterator<Map<String, ?>>, Closeable
    {
        public <A extends Collection<? super Map<String, ?>>> A into(A target);
    }

    protected static abstract class AbstractMCursor<T extends MongoIterable<Document>>implements IMCursor
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

    public static final class MIndexCursor extends AbstractMCursor<ListIndexesIterable<Document>>
    {
        protected MIndexCursor(final ListIndexesIterable<Document> index)
        {
            super(index);
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

        public static final MQuery QUERY(final Map<String, ?> map)
        {
            return new MQuery(map);
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
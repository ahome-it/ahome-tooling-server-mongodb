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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.bson.BSON;
import org.bson.Document;
import org.bson.Transformer;
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
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UpdateOptions;

public final class MongoDB
{
    private static final Logger logger = Logger.getLogger(MongoDB.class);

    private final MongoClient   m_mongo;

    private final String        m_usedb;

    public MongoDB(MongoClientOptions options, String host, int port, String usedb)
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

        public final MCollection remove(Map<String, ?> query)
        {
            m_collection.deleteMany(new MDocument(query));

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
            return m_collection.count(new MDocument(query));
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
                return new MCursor(m_collection.find().projection(Projections.excludeId()));
            }
        }

        public final MCursor find(Map<String, ?> query) throws Exception
        {
            return find(query, false);
        }

        public final MCursor find(Map<String, ?> query, boolean with_id) throws Exception
        {
            if (with_id)
            {
                return new MCursor(m_collection.find(new MDocument(query)));
            }
            else
            {
                return new MCursor(m_collection.find(new MDocument(query)).projection(Projections.excludeId()));
            }
        }

        public final MCursor find(Map<String, ?> query, Map<String, ?> fields) throws Exception
        {
            return find(query, fields, false);
        }

        public final MCursor find(Map<String, ?> query, Map<String, ?> fields, boolean with_id) throws Exception
        {
            if (with_id)
            {
                return new MCursor(m_collection.find(new MDocument(query)).projection(new MDocument(fields)));
            }
            else
            {
                return new MCursor(m_collection.find(new MDocument(query)).projection(new MDocument(fields)).projection(Projections.excludeId()));
            }
        }

        public final MCursor query(Map<String, ?> query) throws Exception
        {
            return find(query, false);
        }

        public final Map<String, ?> findAndModify(Map<String, ?> query, Map<String, ?> update)
        {
            return update(query, update, false, true);
        }

        public final Map<String, ?> upsert(Map<String, ?> query, Map<String, ?> update)
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
            if (multi)
            {
                m_collection.updateMany(new MDocument(query), new MDocument(update), new UpdateOptions().upsert(upsert));
            }
            else
            {
                m_collection.updateOne(new MDocument(query), new MDocument(update), new UpdateOptions().upsert(upsert));
            }
            return update;
        }

        public final Map<String, ?> findOne(Map<String, ?> query)
        {
            FindIterable<Document> iter = m_collection.find(new MDocument(query)).limit(1).projection(Projections.excludeId());

            if (null != iter)
            {
                return iter.first();
            }
            return null;
        }

        public final Map<String, ?> update(Map<String, ?> query, Map<String, ?> update)
        {
            m_collection.updateOne(new MDocument(query), new MDocument(update));

            return update;
        }

        public final long update_n(Map<String, ?> query, Map<String, ?> update)
        {
            return m_collection.updateMany(new MDocument(query), new MDocument(update), new UpdateOptions().upsert(false)).getModifiedCount();
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

        public MCursor skip(int skip)
        {
            return new MCursor(m_finder.skip(skip));
        }

        public MCursor limit(int limit)
        {
            return new MCursor(m_finder.limit(limit));
        }

        public MCursor sort(Map<String, ?> sort)
        {
            return new MCursor(m_finder.sort(new MDocument(sort)));
        }
    }

    private static final class MDocument extends Document
    {
        private static final long serialVersionUID = -3704882524991565448L;

        @SuppressWarnings("unchecked")
        public MDocument(final Map<String, ?> map)
        {
            super((Map<String, Object>) map);
        }
    }
}
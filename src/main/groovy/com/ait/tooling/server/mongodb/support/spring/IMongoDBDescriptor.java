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

package com.ait.tooling.server.mongodb.support.spring;

import java.io.Closeable;
import java.io.Serializable;
import java.util.List;

import com.ait.tooling.common.api.types.IActivatable;
import com.ait.tooling.common.api.types.INamed;
import com.ait.tooling.server.mongodb.MongoDB;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;

public interface IMongoDBDescriptor extends Closeable, INamed, IActivatable, Serializable
{
    public MongoDB getMongoDB();

    public int getConnectionTimeout();

    public int getConnectionMultiplier();

    public int getConnectionPoolSize();

    public String getDefaultDB();

    public boolean isCreateID();

    public boolean isReplicas();

    public List<MongoCredential> getCredentials();

    public List<ServerAddress> getAddresses();
    
    public MongoClientOptions getClientOptions();
}

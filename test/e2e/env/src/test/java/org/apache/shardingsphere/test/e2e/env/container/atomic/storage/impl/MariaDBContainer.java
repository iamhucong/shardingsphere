/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.shardingsphere.test.e2e.env.container.atomic.storage.impl;

import com.google.common.base.Strings;
import org.apache.shardingsphere.infra.database.core.type.DatabaseType;
import org.apache.shardingsphere.infra.spi.type.typed.TypedSPILoader;
import org.apache.shardingsphere.test.e2e.env.container.atomic.storage.DockerStorageContainer;
import org.apache.shardingsphere.test.e2e.env.container.atomic.storage.config.StorageContainerConfiguration;

import java.util.Collection;
import java.util.Optional;

/**
 * MariaDB container.
 */
public final class MariaDBContainer extends DockerStorageContainer {
    
    public static final int EXPOSED_PORT = 3306;
    
    public static final String MARIADB_CONF_IN_CONTAINER = "/etc/mysql/mariadb.cnf";
    
    private final StorageContainerConfiguration storageContainerConfig;
    
    public MariaDBContainer(final String containerImage, final StorageContainerConfiguration storageContainerConfig, final Collection<String> databases) {
        super(TypedSPILoader.getService(DatabaseType.class, "MariaDB"), Strings.isNullOrEmpty(containerImage) ? "mariadb:11" : containerImage, databases);
        this.storageContainerConfig = storageContainerConfig;
    }
    
    @Override
    protected void configure() {
        setCommands(storageContainerConfig.getContainerCommand());
        addEnvs(storageContainerConfig.getContainerEnvironments());
        mapResources(storageContainerConfig.getMountedResources());
        super.configure();
    }
    
    @Override
    public int getExposedPort() {
        return EXPOSED_PORT;
    }
    
    @Override
    public int getMappedPort() {
        return getMappedPort(EXPOSED_PORT);
    }
    
    @Override
    protected Optional<String> getDefaultDatabaseName() {
        return Optional.empty();
    }
}

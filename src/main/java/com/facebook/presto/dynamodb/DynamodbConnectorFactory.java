/*
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
package com.facebook.presto.dynamodb;

import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorFactory;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class DynamodbConnectorFactory
        implements ConnectorFactory
{
    private final Map<String, String> optionalConfig;

    public DynamodbConnectorFactory(Map<String, String> optionalConfig)
    {
        this.optionalConfig = ImmutableMap.copyOf(requireNonNull(optionalConfig, "optionalConfig is null"));
    }

    @Override
    public String getName()
    {
        return "rakam-dynamodb";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new DynamodbHandleResolver();
    }

    @Override
    public Connector create(final String connectorId, Map<String, String> requiredConfig)
    {
        requireNonNull(requiredConfig, "requiredConfig is null");
        try {
            // A plugin is not required to use Guice; it is just very convenient
            Bootstrap app = new Bootstrap(
                    new DynamodbModule(connectorId));

        Injector injector = app
                    .strictConfig()
                    .doNotInitializeLogging()
                    .setRequiredConfigurationProperties(requiredConfig)
                    .setOptionalConfigurationProperties(optionalConfig)
                    .initialize();

            return injector.getInstance(DynamodbConnector.class);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
}

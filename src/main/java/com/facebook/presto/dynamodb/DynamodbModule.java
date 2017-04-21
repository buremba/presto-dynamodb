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

import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import javax.inject.Inject;

import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

public class DynamodbModule
        implements Module
{
    private final String connectorId;

    public DynamodbModule(String connectorId)
    {
        this.connectorId = requireNonNull(connectorId, "connector id is null");
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(DynamodbConnector.class).in(Scopes.SINGLETON);
        binder.bind(DynamodbConnectorId.class).toInstance(new DynamodbConnectorId(connectorId));
        binder.bind(DynamodbMetadata.class).in(Scopes.SINGLETON);
        binder.bind(DynamodbClient.class).in(Scopes.SINGLETON);
        binder.bind(DynamodbSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(DynamodbRecordSetProvider.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(DynamodbConfig.class);
    }
}

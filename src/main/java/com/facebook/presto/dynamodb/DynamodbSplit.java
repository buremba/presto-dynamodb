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

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class DynamodbSplit
        implements ConnectorSplit
{
    private final String connectorId;
    private final String tableName;
    private final Optional<Map.Entry<String, AttributeValue>> hashKeyAttributeKey;
    private final Optional<Map.Entry<String, Condition>> rangeKeyCondition;

    @JsonCreator
    public DynamodbSplit(
            @JsonProperty("connectorId") String connectorId,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("hashKeyAttributeKey") Optional<Map.Entry<String, AttributeValue>> hashKeyAttributeKey,
            @JsonProperty("rangeKeyCondition") Optional<Map.Entry<String, Condition>> rangeKeyCondition)
    {
        this.connectorId = requireNonNull(connectorId, "connector id is null");
        this.tableName = requireNonNull(tableName, "table name is null");
        this.hashKeyAttributeKey = requireNonNull(hashKeyAttributeKey, "hashKeyAttributeKey is null");
        this.rangeKeyCondition = requireNonNull(rangeKeyCondition, "rangeKeyCondition is null");
    }

    @JsonProperty
    public String getConnectorId()
    {
        return connectorId;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public Optional<Map.Entry<String, AttributeValue>> getHashKeyAttributeKey()
    {
        return hashKeyAttributeKey;
    }

    @JsonProperty
    public Optional<Map.Entry<String, Condition>> getRangeKeyCondition()
    {
        return rangeKeyCondition;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        try {
            return ImmutableList.of(HostAddress.fromUri(new URI("http://dynamodb.aws.com")));
        }
        catch (URISyntaxException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public Object getInfo()
    {
        return this;
    }
}

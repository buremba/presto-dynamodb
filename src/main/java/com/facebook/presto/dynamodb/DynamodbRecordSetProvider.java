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
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.connector.ConnectorRecordSetProvider;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.AbstractMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.dynamodb.Types.checkType;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class DynamodbRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final String connectorId;
    private final DynamodbClient dynamodbClient;

    @Inject
    public DynamodbRecordSetProvider(DynamodbConnectorId connectorId, DynamodbClient dynamodbClient)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.dynamodbClient = requireNonNull(dynamodbClient, "dynamodbClient is null");
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transactionHandle, ConnectorSession session, ConnectorSplit split, List<? extends ColumnHandle> columns)
    {
        requireNonNull(split, "partitionChunk is null");
        DynamodbSplit exampleSplit = checkType(split, DynamodbSplit.class, "split");
        checkArgument(exampleSplit.getConnectorId().equals(connectorId), "split is not for this connector");

        ImmutableList.Builder<DynamodbColumnHandle> handles = ImmutableList.builder();
        for (ColumnHandle handle : columns) {
            handles.add(checkType(handle, DynamodbColumnHandle.class, "handle"));
        }

        Iterator<List<Map<String, AttributeValue>>> tableData = dynamodbClient.getTableData(
                exampleSplit.getTableName(),
                exampleSplit.getHashKeyAttributeKey(),
                exampleSplit.getRangeKeyCondition());

        return new DynamodbRecordSet(exampleSplit, handles.build(), tableData);
    }
}

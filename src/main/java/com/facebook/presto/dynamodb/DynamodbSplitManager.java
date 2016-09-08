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
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.predicate.AllOrNone;
import com.facebook.presto.spi.predicate.DiscreteValues;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Ranges;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.predicate.ValueSet;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.VarcharType;
import io.airlift.slice.Slice;

import javax.inject.Inject;

import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import static com.facebook.presto.dynamodb.Types.checkType;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class DynamodbSplitManager
        implements ConnectorSplitManager
{
    private final String connectorId;
    private final DynamodbClient exampleClient;

    @Inject
    public DynamodbSplitManager(DynamodbConnectorId connectorId, DynamodbClient exampleClient)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.exampleClient = requireNonNull(exampleClient, "client is null");
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle handle, ConnectorSession session, ConnectorTableLayoutHandle layout)
    {
        DynamodbTableLayoutHandle layoutHandle = checkType(layout, DynamodbTableLayoutHandle.class, "layout");
        DynamodbTableHandle tableHandle = layoutHandle.getTable();
        DynamodbTable table = exampleClient.getTable(tableHandle.getTableName());
        // this can happen if table is removed during a query
        checkState(table != null, "Table %s.%s no longer exists", tableHandle.getSchemaName(), tableHandle.getTableName());

        Optional<AttributeValue> hashKeyCondition;
        Optional<Condition> rangeKeyCondition;
        List<ConnectorSplit> splits = new ArrayList<>();
        Optional<List<TupleDomain.ColumnDomain<ColumnHandle>>> domains = layoutHandle.getPromisedPredicate().getColumnDomains();
        if (domains.isPresent()) {
            hashKeyCondition = table.getHashKey().flatMap(key -> domains.get().stream()
                    .filter(column -> checkType(column.getColumn(), DynamodbColumnHandle.class, "columnHandle")
                            .getColumnName().equals(key))
                    .findAny()).map(e -> e.getDomain()).flatMap(e -> getAttributeValue(e));

            rangeKeyCondition = table.getRangeKey().flatMap(key -> domains.get().stream()
                    .filter(column -> checkType(column.getColumn(), DynamodbColumnHandle.class, "columnHandle")
                            .getColumnName().equals(key))
                    .findAny()).map(e -> e.getDomain()).flatMap(e -> {
                Optional<AttributeValue> attributeValue = getAttributeValue(e);
                if (attributeValue.isPresent()) {
                    return Optional.of(new Condition().withAttributeValueList(attributeValue.get())
                            .withComparisonOperator(ComparisonOperator.EQ));
                }

                Condition condition = new Condition();
//                if (e.getValues().getDiscreteValues().isWhiteList()) {
                return Optional.empty();
//                }
            });
        }
        else {
            hashKeyCondition = Optional.empty();
            rangeKeyCondition = Optional.empty();
        }

        splits.add(new DynamodbSplit(connectorId, table.getName(),
                hashKeyCondition.map(e -> new AbstractMap.SimpleImmutableEntry<>(table.getHashKey().get(), e)),
                rangeKeyCondition.map(e -> new AbstractMap.SimpleImmutableEntry<>(table.getHashKey().get(), e))
                ));
        Collections.shuffle(splits);

        return new FixedSplitSource(splits);
    }

    private Optional<AttributeValue> getAttributeValue(Domain domain)
    {
        if (domain.isNullableSingleValue()) {
            Object singleValue = domain.getSingleValue();
            AttributeValue value;
            if (domain.getType() instanceof VarcharType) {
                value = new AttributeValue().withS(((Slice) singleValue).toStringUtf8());
            }
            else if (singleValue instanceof Number || singleValue instanceof Boolean) {
                value = new AttributeValue().withN(singleValue.toString());
            }
            else {
                throw new IllegalStateException();
            }

            return Optional.of(value);
        }

        return Optional.empty();
    }
}

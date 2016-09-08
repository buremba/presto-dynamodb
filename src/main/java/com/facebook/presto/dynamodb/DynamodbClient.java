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

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import javax.inject.Inject;

import java.io.IOException;
import java.lang.ref.Reference;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.amazonaws.services.dynamodbv2.model.ComparisonOperator.EQ;
import static java.util.Objects.requireNonNull;

public class DynamodbClient
{
    private final AmazonDynamoDBClient dynamoDBClient;

    @Inject
    public DynamodbClient(DynamodbConfig config)
            throws IOException
    {
        requireNonNull(config, "config is null");
        dynamoDBClient = new AmazonDynamoDBClient(config.getCredentials());
        dynamoDBClient.setRegion(config.getAWSRegion());
        if (config.getDynamodbEndpoint() != null) {
            dynamoDBClient.setEndpoint(config.getDynamodbEndpoint());
        }
    }

    public Set<String> getSchemaNames()
    {
        return ImmutableSet.of("default");
    }

    public Set<String> getTableNames(String schema)
    {
        if (!"default".equals(schema)) {
            throw new PrestoException(StandardErrorCode.GENERIC_USER_ERROR, "Schema does not exist");
        }

        return ImmutableSet.copyOf(dynamoDBClient.listTables().getTableNames());
    }

    public DynamodbTable getTable(String tableName)
    {
        TableDescription table = dynamoDBClient.describeTable(tableName).getTable();
        List<DynamodbColumn> columns = table.getAttributeDefinitions().stream()
                .map(e -> new DynamodbColumn(e.getAttributeName(), getType(e.getAttributeType())))
                .collect(Collectors.toList());

        Optional<String> hashKey = table.getKeySchema().stream()
                .filter(e -> e.getKeyType().equals("HASH"))
                .map(e -> e.getAttributeName()).findAny();
        Optional<String> rangeKey = table.getKeySchema().stream()
                .filter(e -> e.getKeyType().equals("RANGE"))
                .map(e -> e.getAttributeName()).findAny();

        return new DynamodbTable(tableName, hashKey, rangeKey, columns);
    }

    private static Type getType(String dynamodbAttributeType)
    {
        switch (dynamodbAttributeType) {
            case "S":
                return VarcharType.VARCHAR;
            case "N":
                return DoubleType.DOUBLE;
            case "B":
                return BooleanType.BOOLEAN;
            case "NS":
                return VarcharType.VARCHAR;
        }
        throw new IllegalStateException("Illegal dynamodb attribute type: " + dynamodbAttributeType);
    }

    public Iterator<List<Map<String, AttributeValue>>> getTableData(String name, Optional<Entry<String, AttributeValue>> hashKeyCondition, Optional<Entry<String, Condition>> rangeKeyCondition)
    {
        AtomicReference<Map<String, AttributeValue>> lastKeyEvaluated = new AtomicReference<>();
        AtomicBoolean firstRun = new AtomicBoolean(true);

        return new Iterator<List<Map<String, AttributeValue>>>()
        {
            @Override
            public boolean hasNext()
            {
                return firstRun.get() && lastKeyEvaluated.get() != null;
            }

            @Override
            public List<Map<String, AttributeValue>> next()
            {
                firstRun.set(false);
                if (hashKeyCondition.isPresent()) {
                    ImmutableMap.Builder<String, Condition> builder = ImmutableMap.builder();
                    builder.put(hashKeyCondition.get().getKey(),
                            new Condition()
                                    .withAttributeValueList(hashKeyCondition.get().getValue())
                                    .withComparisonOperator(EQ));

                    if (rangeKeyCondition.isPresent()) {
                        Entry<String, Condition> rangeEntry = rangeKeyCondition.get();
                        if (rangeEntry.getValue().getComparisonOperator() == EQ.name() && rangeEntry.getValue().getAttributeValueList().size() == 1) {
                            GetItemResult item = dynamoDBClient.getItem(name, ImmutableMap.of(hashKeyCondition.get().getKey(), hashKeyCondition.get().getValue(),
                                    rangeEntry.getKey(), rangeEntry.getValue().getAttributeValueList().get(0)));
                            return ImmutableList.of(item.getItem());
                        }
                        else {
                            builder.put(rangeKeyCondition.get().getKey(), rangeKeyCondition.get().getValue());
                        }
                    }

                    QueryResult query = dynamoDBClient.query(new QueryRequest()
                            .withTableName(name)
                            .withExclusiveStartKey(lastKeyEvaluated.get())
                            .withKeyConditions(builder.build())
                            .withLimit(100000));

                    lastKeyEvaluated.set(query.getLastEvaluatedKey());

                    return query.getItems();
                }
                else {
                    ScanResult scan = dynamoDBClient.scan(new ScanRequest()
                            .withExclusiveStartKey(lastKeyEvaluated.get())
                            .withLimit(100000)
                            .withTableName(name));

                    lastKeyEvaluated.set(scan.getLastEvaluatedKey());
                    return scan.getItems();
                }
            }
        };
    }
}

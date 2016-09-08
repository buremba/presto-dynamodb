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

import io.airlift.testing.EquivalenceTester;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;

public class TestDynamodbColumnHandle
{
    @Test
    public void testEquivalence()
    {
        EquivalenceTester.equivalenceTester()
                .addEquivalentGroup(
                        new DynamodbColumnHandle("connectorId", "columnName", createUnboundedVarcharType(), 0),
                        new DynamodbColumnHandle("connectorId", "columnName", createUnboundedVarcharType(), 0),
                        new DynamodbColumnHandle("connectorId", "columnName", BIGINT, 0),
                        new DynamodbColumnHandle("connectorId", "columnName", createUnboundedVarcharType(), 1))
                .addEquivalentGroup(
                        new DynamodbColumnHandle("connectorIdX", "columnName", createUnboundedVarcharType(), 0),
                        new DynamodbColumnHandle("connectorIdX", "columnName", createUnboundedVarcharType(), 0),
                        new DynamodbColumnHandle("connectorIdX", "columnName", BIGINT, 0),
                        new DynamodbColumnHandle("connectorIdX", "columnName", createUnboundedVarcharType(), 1))
                .addEquivalentGroup(
                        new DynamodbColumnHandle("connectorId", "columnNameX", createUnboundedVarcharType(), 0),
                        new DynamodbColumnHandle("connectorId", "columnNameX", createUnboundedVarcharType(), 0),
                        new DynamodbColumnHandle("connectorId", "columnNameX", BIGINT, 0),
                        new DynamodbColumnHandle("connectorId", "columnNameX", createUnboundedVarcharType(), 1))
                .check();
    }
}

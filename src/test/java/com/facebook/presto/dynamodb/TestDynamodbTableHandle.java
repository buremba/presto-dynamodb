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

public class TestDynamodbTableHandle
{
    @Test
    public void testEquivalence()
    {
        EquivalenceTester.equivalenceTester()
                .addEquivalentGroup(new DynamodbTableHandle("connector", "schema", "table"), new DynamodbTableHandle("connector", "schema", "table"))
                .addEquivalentGroup(new DynamodbTableHandle("connectorX", "schema", "table"), new DynamodbTableHandle("connectorX", "schema", "table"))
                .addEquivalentGroup(new DynamodbTableHandle("connector", "schemaX", "table"), new DynamodbTableHandle("connector", "schemaX", "table"))
                .addEquivalentGroup(new DynamodbTableHandle("connector", "schema", "tableX"), new DynamodbTableHandle("connector", "schema", "tableX"))
                .check();
    }
}

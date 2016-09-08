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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class DynamodbTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final DynamodbTableHandle table;
    private final TupleDomain<ColumnHandle> promisedPredicate;

    @JsonCreator
    public DynamodbTableLayoutHandle(@JsonProperty("table") DynamodbTableHandle table, @JsonProperty("promisedPredicate") TupleDomain<ColumnHandle> promisedPredicate)
    {
        this.table = table;
        this.promisedPredicate = promisedPredicate;
    }

    @JsonProperty
    public DynamodbTableHandle getTable()
    {
        return table;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getPromisedPredicate()
    {
        return promisedPredicate;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        DynamodbTableLayoutHandle that = (DynamodbTableLayoutHandle) o;

        if (!table.equals(that.table)) {
            return false;
        }
        return promisedPredicate.equals(that.promisedPredicate);
    }

    @Override
    public int hashCode()
    {
        int result = table.hashCode();
        result = 31 * result + promisedPredicate.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        return table.toString();
    }
}

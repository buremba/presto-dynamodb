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

import com.facebook.presto.spi.ColumnMetadata;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Locale;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

public class DynamodbTable
{
    private final String name;
    private final List<DynamodbColumn> columns;
    private final List<ColumnMetadata> columnsMetadata;
    private final Optional<String> hashKey;
    private final Optional<String> rangeKey;

    @JsonCreator
    public DynamodbTable(
            @JsonProperty("name") String name,
            @JsonProperty("hashKey") Optional<String> hashKey,
            @JsonProperty("rangeKey") Optional<String> rangeKey,
            @JsonProperty("columns") List<DynamodbColumn> columns)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or is empty");
        this.hashKey = hashKey.map(e -> e.toLowerCase(Locale.ENGLISH));
        this.rangeKey = rangeKey.map(e -> e.toLowerCase(Locale.ENGLISH));
        this.name = requireNonNull(name, "name is null");
        this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));

        ImmutableList.Builder<ColumnMetadata> columnsMetadata = ImmutableList.builder();
        for (DynamodbColumn column : this.columns) {
            columnsMetadata.add(new ColumnMetadata(column.getName(), column.getType()));
        }
        this.columnsMetadata = columnsMetadata.build();
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public Optional<String> getHashKey()
    {
        return hashKey;
    }

    @JsonProperty
    public Optional<String> getRangeKey()
    {
        return rangeKey;
    }

    @JsonProperty
    public List<DynamodbColumn> getColumns()
    {
        return columns;
    }

    public List<ColumnMetadata> getColumnsMetadata()
    {
        return columnsMetadata;
    }
}

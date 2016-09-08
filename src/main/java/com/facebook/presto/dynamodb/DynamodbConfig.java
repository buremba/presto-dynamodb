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

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.internal.StaticCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import io.airlift.configuration.Config;

public class DynamodbConfig
{
    private String accessKey;
    private String secretAccessKey;
    private String region;
    private String dynamodbEndpoint;

    @Config("aws.access-key")
    public DynamodbConfig setAccessKey(String accessKey)
    {
        this.accessKey = accessKey;
        return this;
    }

    @Config("aws.region")
    public DynamodbConfig setRegion(String region)
    {
        this.region = region;
        return this;
    }

    public String getRegion()
    {
        return region;
    }

    public String getAccessKey()
    {
        return accessKey;
    }

    @Config("aws.secret-access-key")
    public DynamodbConfig setSecretAccessKey(String secretAccessKey)
    {
        this.secretAccessKey = secretAccessKey;
        return this;
    }

    public String getSecretAccessKey()
    {
        return secretAccessKey;
    }

    @Config("aws.dynamodb-endpoint")
    public DynamodbConfig setDynamodbEndpoint(String dynamodbEndpoint)
    {
        this.dynamodbEndpoint = dynamodbEndpoint;
        return this;
    }

    public String getDynamodbEndpoint()
    {
        return dynamodbEndpoint;
    }

    public AWSCredentialsProvider getCredentials()
    {
        if (accessKey == null && secretAccessKey == null) {
            return new InstanceProfileCredentialsProvider();
        }
        return new StaticCredentialsProvider(new BasicAWSCredentials(getAccessKey(), getSecretAccessKey()));
    }

    public Region getAWSRegion()
    {
        return Region.getRegion(region == null ? Regions.DEFAULT_REGION : Regions.fromName(region));
    }
}

# Dynamodb connector for Presto

You can set your AWS credentials specified in [config](https://github.com/buremba/presto-dynamodb/blob/master/src/main/java/com/facebook/presto/dynamodb/DynamodbConfig.java) class and connect to your Dynamodb tables. The connector automatically maps the schema of Dynamodb tables to Presto types.

By default, the only schema is *default*, when you reference the table *default.mydynamodbtable*, the connector tries to connect *mydynamodbtable* in specified region of your AWS account..


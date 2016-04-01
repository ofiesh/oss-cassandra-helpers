![Logo](https://www.clearcapital.com/wp-content/uploads/2015/02/Clear-Capital@2x.png)
--

[Home](README.md)

# Temporary Keyspaces and Tables

You guessed it: you can now create keyspaces and tables which will be closed upon exiting a code block:

```java
String tableName = CQLHelpers.getUniqueName("tmp_");
try( TemporaryTable temporaryTable = CassandraTableProcessor
  .tableBuilder(executor, cassandraResource.getClient(), DemoTable.class).setTableName(tableName)
  .buildTemp() ) {
  
  // do stuff with temporaryTable here...
  
} // temporaryTable.close() gets called, dropping the table.
```

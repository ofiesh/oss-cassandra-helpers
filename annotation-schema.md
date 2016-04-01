![Logo](https://www.clearcapital.com/wp-content/uploads/2015/02/Clear-Capital@2x.png)
--

[Home](README.md)

# Annotation-Based Schema Definition

The DataStax drivers do a great job of providing CQL statement
builders. The only problem is that, if you are working in an
environment where your schema is evolving, trying to create
appropriate ```ALTER TABLE``` statements can be tedious and
error-prone.  Consider, for example, what happens when two developers
decide to change the schema for a table in two separate releases,
but the first release is scrubbed for some reason or other.

Now you've got a situation where the second schema change depends on
the first, but the first hasn't happened. Yes, this can be handled by
better managing your releases. Or... you can use a declarative method
to describe your schema, rather than an imperative one, and allow a
tool to determine the necessary set of commands to modify your schema.

Our approach builds on the DataStax drivers' imperative methodology by
placing an annotation layer above it, starting with
```@CassandraTable```, and providing an annotation processor which
translates from annotations to DataStax' imperative statements.

To use it, start by having one java class per cassandra table:

```java
@CassandraTable
class DemoTable {
};
```

This tells ```CassandraTableProcessor``` that ```DemoTable```
provides a programmatic interface to a table in Cassandra, but doesn't (yet)
tell ```CassandraTableProcessor``` what its schema looks like. Let's start by setting the name of the
table from Cassandra's perspective, and how to find it with respect to
[MultiRing Management](multiring-management.md).

```java
@CassandraTable(
  tableName="demo_table", 
  multiRingGroup="groupA"
)
class DemoTable {
};
```

What about columns? This is the area where ```@CassandraTable```
becomes the most complex. There are a few reasons for this complexity:
Cassandra itself supports a wide array of data types, including
integral types, text, lists, sets, maps, and even user-defined types;
annotations do not support inheritance of annotations, so there's some
patterns which may seem a bit quirky; we support different methods of
encoding/decoding data between Cassandra and Java.

We have not implemented _all_ of Cassandra's data types, nor all of
Java's data types, though we've set up a framework which we believe
will allow us to expand to do so.

So let's start introducing this aspect by expanding our DemoTable to
be a simple mapping from an id to a JSON-encoded object:

```java
@CassandraTable(
  tableName="demo_table", 
  multiRingGroup="groupA",
  columns={
    @Column(cassandraName="id", reflectionColumnInfo = @ReflectionColumnInfo(
      javaPath={"id"},
      dataType=CassandraDataType.BIGINT,
      columnOption=ColumnOption.PARTITION_KEY
      )),
    @Column(cassandraName="json", jsonColumnInfo = @JsonColumnInfo(
      model = Object.class
      ))
  }
)
class DemoTable {
};
```

This tells ```CassandraTableProcessor``` that the schema looks
something like this:

```CQL
# CONNECT TO RING INDICATED BY "groupA"
# USE KEYSPACE INDICATED BY "groupA"
CREATE TABLE demo_table (
  id BIGINT,
  json TEXT,
  PRIMARY KEY(id)
)
```

It also tells ```CassandraTableProcessor``` some other interesting
information:

- the ```id``` column can be retrieved from a model object using
reflection to call ```getId()```.

- the ```json``` column encodes/decodes the entire model object as
JSON, using JsonSerializer from the com.clearcapital=>oss-java-helpers
project.

Maybe we'd like to retain version history on our objects by adding a
clustering column:

```java
@CassandraTable(
  tableName="demo_table", 
  multiRingGroup="groupA",
  columns={
    @Column(cassandraName="id", reflectionColumnInfo = @ReflectionColumnInfo(
      javaPath={"id"},
      dataType=CassandraDataType.BIGINT,
      columnOption=ColumnOption.PARTITION_KEY
      )),
    @Column(cassandraName="updateId", reflectionColumnInfo = @ReflectionColumnInfo(
      javaPath={"updateId"},
      dataType=CassandraDataType.BIGINT,
      columnOption=ColumnOption.CLUSTERING_KEY
      )),
    @Column(cassandraName="json", jsonColumnInfo = @JsonColumnInfo(
      model = Object.class
      ))
  }
)
class DemoTable {
};
```

This changes the schema to look more like this:

```CQL
# CONNECT TO RING INDICATED BY "groupA"
# USE KEYSPACE INDICATED BY "groupA"
CREATE TABLE demo_table (
  id BIGINT,
  updateId BIGINT,
  json TEXT,
  PRIMARY KEY(id, updateId)
)
```

Or perhaps we want a compound partition key:

```java
@CassandraTable(
  tableName="demo_table", 
  multiRingGroup="groupA",
  columns={
    @Column(cassandraName="id", reflectionColumnInfo = @ReflectionColumnInfo(
      javaPath={"id"},
      dataType=CassandraDataType.BIGINT,
      columnOption=ColumnOption.PARTITION_KEY
      )),
    @Column(cassandraName="updateId", reflectionColumnInfo = @ReflectionColumnInfo(
      javaPath={"updateId"},
      dataType=CassandraDataType.BIGINT,
      columnOption=ColumnOption.PARTITION_KEY
      )),
    @Column(cassandraName="json", jsonColumnInfo = @JsonColumnInfo(
      model = Object.class
      ))
  }
)
class DemoTable {
};
```

Now we get the extra set of parentheses, as you might expect:

```CQL
# CONNECT TO RING INDICATED BY "groupA"
# USE KEYSPACE INDICATED BY "groupA"
CREATE TABLE demo_table (
  id BIGINT,
  updateId BIGINT,
  json TEXT,
  PRIMARY KEY((id, updateId))
)
```

- Columns are created in the order they are found, and added to the
```PRIMARY KEY``` in that same order.

- Columns using jsonColumnInfo do not have a ```columnInfo``` member
  and therefore cannot be a part of the PRIMARY KEY.

What about clustering order? There's an annotation for that, too!

```java
@CassandraTable(
  tableName="demo_table", 
  multiRingGroup="groupA",
  columns={
    @Column(cassandraName="id", reflectionColumnInfo = @ReflectionColumnInfo(
      javaPath={"id"},
      dataType=CassandraDataType.BIGINT,
      columnOption=ColumnOption.PARTITION_KEY
      )),
    @Column(cassandraName="updateId", reflectionColumnInfo = @ReflectionColumnInfo(
      javaPath={"updateId"},
      dataType=CassandraDataType.BIGINT,
      columnOption=ColumnOption.CLUSTERING_KEY
      )),
    @Column(cassandraName="json", jsonColumnInfo = @JsonColumnInfo(
      model = Object.class
      ))
  },
  clusteringOrder={
    @ClusteringOrder(columnName="updateId",descending=true)
  }
)
class DemoTable {
};
```


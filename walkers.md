![Logo](https://www.clearcapital.com/wp-content/uploads/2015/02/Clear-Capital@2x.png)
--

[Home](README)

# Walkers

Walkers are iterable objects which will walk through a cassandra table, one row at a time,
and deserialize each row into your model objects, in turn.

The order of the walk is *token order*.

Walkers support calculating progress and eta, as well as obtaining the token for the current 
row.

```
CassandraTableWalker<ModelType> walker = new CassandraTableWalker<ModelType>(
    session, session.getLoggedKeyspace(), tableName, keyColumnNames, deserializer)
        .setStartToken(startToken)
        .setEndToken(endToken);
    
LocalDateTime start = LocalDateTime.now();
                    
for(ModelType model : walker) {
    log.debug("The walker found:" + model);
    log.debug("Token:" + walker.getToken());
    log.debug("Progress:" + walker.getProgress());
    log.debug("ETA:" + walker.getEta(start));
}
```

Walkers can form the basis of a "poor man's spark," when used on a cassandra ring with one of these characteristics:

- Isn't using vnodes
- Don't mind parsing nodetool info to determine token ranges

The basic approach is to place your application on the nodes in the cassandra ring, and tell each instance
the node's start and end tokens. This will cause each instance to traverse only those rows for which the
host node is the first replica.
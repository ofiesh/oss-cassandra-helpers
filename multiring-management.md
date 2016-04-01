![Logo](https://www.clearcapital.com/wp-content/uploads/2015/02/Clear-Capital@2x.png)
--

[Home](README.md)

# Multiring Management

In your application's configuration, include a MultiRingConfiguration
object. In this example, it's named ```multiRingConfiguration```, but
you could use any name, of course.

```yaml
# A MultiRingConfiguration Object:
multiRingConfiguration:
  rings:
    dse:
      hosts: [db]
      preferredKeyspace: dse
    nondse:
      hosts: [db]
      preferredKeyspace: nondse
  defaultRing: nondse
  groups:
    groupA: dse
    groupB: nondse
```

Somewhere during your initialization, you would then pass this object
off to a ```MultiRingClientManager```:

```java
MultiRingClientManager manager =
  new MultiRingClientManager(appConfiguration.getMultiRingConfiguration());
```

At this point, you have the ability to do things like connect to a
cluster based either on the ring names or the group names:

```java
RingClient dseClient = manager.getRingClientForRing("dse");
RingClient groupAClient = manager.getRingClientForGroup("groupA");
// dseClient == groupAClient.
```

From here, you may want to do things like connect to the preferredKeyspace:

```java
SessionHelper session = groupAClient.getPreferredKeyspace();
ResultSet results = session.execute("SELECT * FROM someTable");
```

## FAQ

- *Why not just have a single ring/keyspace configured?* This feature
  was born out of a desire to separate one monolithic Cassandra ring
  into a set of Cassandra rings, so that we could better manage our
  data needs. When you have only a handful of tables, a single ring
  makes a lot of sense. When you have dozens, not so much.

- *Why have the added complexity of groups?* There's effectively two
  ways to look at Cassandra rings. The first is more physical: I've
  got these hosts, listening on these ports, with this keyspace. The
  second is more logical: I've got these tables that all relate to
  each other somehow. It seems like it makes sense to be able to
  migrate tables from one ring to another by grouping them together
  on logical boundaries (e.g., "authentication" and "logs"), and then
  choosing which ring each group "belongs" to.



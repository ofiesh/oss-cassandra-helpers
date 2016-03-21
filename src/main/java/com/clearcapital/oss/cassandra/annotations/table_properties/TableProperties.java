package com.clearcapital.oss.cassandra.annotations.table_properties;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE })
public @interface TableProperties {

    public static final double BLOOM_FILTER_FP_CHANCE_DEFAULT = -Double.MAX_VALUE;
    public static final double DCLOCAL_READ_REPAIR_CHANCE_DEFAULT = -Double.MAX_VALUE;
    public static final int DEFAULT_TIME_TO_LIVE_DEFAULT = Integer.MIN_VALUE;
    public static final int GC_GRACE_SECONDS_DEFAULT = Integer.MIN_VALUE;
    public static final int MIN_INDEX_INTERVAL_DEFAULT = Integer.MIN_VALUE;
    public static final int MAX_INDEX_INTERVAL_DEFAULT = Integer.MIN_VALUE;
    public static final int MEMTABLE_FLUSH_PERIOD_IN_MS_DEFAULT = Integer.MIN_VALUE;
    public static final double READ_REPAIR_CHANCE_DEFAULT = -Double.MAX_VALUE;

    /**
     * Desired false-positive probability for SSTable Bloom filters.
     * 
     * <li><0 : do not specify/use the default
     * <li>0.0 : Enables the unmodified, effectively the largest possible, Bloom filter
     * <li>1.0 : Disables the Bloom Filter
     * 
     * <li>See Also: <a href='http://docs.datastax.com/en/cql/3.1/cql/cql_reference/tabProp.html'>DSE table property
     * docs</a>
     */
    double bloomFilterFpChance() default BLOOM_FILTER_FP_CHANCE_DEFAULT;

    /**
     * Sets the caching strategy.
     * 
     * <li>See Also: caching section
     * <a href='http://docs.datastax.com/en/cql/3.1/cql/cql_reference/tabProp.html#tabProp__moreCaching'>here</a>
     */
    Caching caching() default @Caching(useDefault = true);

    /**
     * Commentary.
     */
    String comment() default "";

    /**
     * Sets the compaction strategy.
     *
     * <li>See Also: compaction section
     * <a href='http://docs.datastax.com/en/cql/3.1/cql/cql_reference/tabProp.html#tabProp__moreCompaction'>here</a>
     */
    Compaction compaction() default @Compaction;

    /**
     * Sets the compression strategy.
     * 
     * <li>See Also: compression section
     * <a href='http://docs.datastax.com/en/cql/3.1/cql/cql_reference/tabProp.html#tabProp__moreCompression'>here</a>
     */
    Compression compression() default @Compression;

    /**
     * Probability of read repairs being invoked over all replicas in the current data center.
     *
     * <li>See Also: <a href='http://docs.datastax.com/en/cql/3.1/cql/cql_reference/tabProp.html'>DSE table property
     * docs</a>
     */
    double dclocalReadRepairChance() default DCLOCAL_READ_REPAIR_CHANCE_DEFAULT;

    /**
     * This is <b>not</b> the TTL for new records, despite the somewhat awkward naming.
     * 
     * <li>See Also: <a href='http://docs.datastax.com/en/cql/3.1/cql/cql_reference/tabProp.html'>DSE table property
     * docs</a>
     */
    int defaultTimeToLive() default DEFAULT_TIME_TO_LIVE_DEFAULT;

    /**
     * Time to wait before garbage collecting tombstones.
     * 
     * <li>See Also: <a href='http://docs.datastax.com/en/cql/3.1/cql/cql_reference/tabProp.html'>DSE table property
     * docs</a>
     */
    int gcGraceSeconds() default GC_GRACE_SECONDS_DEFAULT;

    /**
     * Control the sampling of entries from the partition index, along with {@link #maxIndexInterval()}
     * 
     * <li>See Also:
     * <a href='http://docs.datastax.com/en/cql/3.1/cql/cql_reference/tabProp.html#tabProp__moreIndexInterval'>DSE index
     * interval docs</a>
     */
    int minIndexInterval() default MIN_INDEX_INTERVAL_DEFAULT;

    /**
     * Control the sampling of entries from the partition index, along with {@link #minIndexInterval()}
     * 
     * <li>See Also:
     * <a href='http://docs.datastax.com/en/cql/3.1/cql/cql_reference/tabProp.html#tabProp__moreIndexInterval'>DSE index
     * interval docs</a>
     */
    int maxIndexInterval() default MAX_INDEX_INTERVAL_DEFAULT;

    /**
     * Forces flushing of the memtable (after specified milliseconds)
     * 
     * <li>See Also: <a href='http://docs.datastax.com/en/cql/3.1/cql/cql_reference/tabProp.html'>DSE table property
     * docs</a>
     */
    int memtableFlushPeriodInMs() default MEMTABLE_FLUSH_PERIOD_IN_MS_DEFAULT;

    /**
     * Adds newly flushed or compacted SSTables to the operating system page cache
     * 
     * <li>See Also: <a href='http://docs.datastax.com/en/cql/3.1/cql/cql_reference/tabProp.html'>DSE table property
     * docs</a>
     */
    TriBoolean populate_io_cache_on_flush() default TriBoolean.USE_DEFAULT;

    /**
     * Basis for invoking read repairs on reads in clusters. valid range: [0..1]
     * 
     * <li>See Also: <a href='http://docs.datastax.com/en/cql/3.1/cql/cql_reference/tabProp.html'>DSE table property
     * docs</a>
     */
    double readRepairChance() default READ_REPAIR_CHANCE_DEFAULT;

    SpeculativeRetry speculativeRetry() default @SpeculativeRetry(useDefault = true);
}

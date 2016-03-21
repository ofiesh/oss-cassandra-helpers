package com.clearcapital.oss.cassandra.annotations.table_properties.compaction;

import com.clearcapital.oss.cassandra.annotations.table_properties.TriBoolean;

public @interface SizeTiered {

    boolean selected() default true;

    /**
     * Size-tiered compaction considers SSTables to be within the same bucket if the SSTable size diverges by 50% or
     * less from the default {@link #bucketLow} and default {@link #bucketHigh} values
     */
    double bucketHigh() default AnnotationHelpers.DOUBLE_UNPSECIFIED;

    /**
     * Size-tiered compaction considers SSTables to be within the same bucket if the SSTable size diverges by 50% or
     * less from the default {@link #bucketLow} and default {@link #bucketHigh} values
     */
    double bucketLow() default AnnotationHelpers.DOUBLE_UNPSECIFIED;

    /**
     * Maximum percentage of reads/sec that ignored SSTables may account for.
     */
    double coldReadsRatioToOmit() default AnnotationHelpers.DOUBLE_UNPSECIFIED;

    /**
     * Enables background compaction.
     */
    TriBoolean backgroundEnabled() default TriBoolean.USE_DEFAULT;

    /**
     * maximum number of SSTables to allow in a minor compaction.
     */
    int maxThreshold() default AnnotationHelpers.INT_UNSPECIFIED;

    /**
     * If your SSTables are small, use min_sstable_size to define a size threshold (in bytes) below which all SSTables
     * belong to one unique bucket.
     */
    long minSSTableSizeInBytes() default AnnotationHelpers.LONG_UNSPECIFIED;

    /**
     * minimum number of SSTables to trigger a minor compaction.
     */
    int minThreshold() default AnnotationHelpers.INT_UNSPECIFIED;

    /**
     * Minimum time to wait after an SSTable creation time before considering the SSTable for tombstone compaction.
     */
    int tombstoneCompactionIntervalInDay() default AnnotationHelpers.INT_UNSPECIFIED;

    /**
     * Ratio of garbage-collectable tombstones to all contained columns.
     */
    double tombstoneThreshold() default AnnotationHelpers.DOUBLE_UNPSECIFIED;

    /**
     * Enables more aggressive than normal tombstone compactions.
     */
    TriBoolean uncheckedTombstoneCompaction() default TriBoolean.USE_DEFAULT;

    /**
     * only_purge_repaired_tombstones false In Cassandra 3.0 and later, to avoid users resurrected data if repair is not
     * run within gc_grace_seconds, this option allows purging only tombstones from repaired SSTables if set to "true".
     * If you do not run repair for a long time, all tombstones are kept and may cause problems.
     * 
     * TODO: support this once the released DSE driver supports it.
     */
}

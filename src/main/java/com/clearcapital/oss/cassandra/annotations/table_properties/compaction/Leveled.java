package com.clearcapital.oss.cassandra.annotations.table_properties.compaction;

import com.clearcapital.oss.cassandra.annotations.table_properties.TriBoolean;

public @interface Leveled {

    boolean selected() default true;

    /**
     * Enable background compaction. See cold_reads_to_omit below.
     */
    TriBoolean backgroundEnabled() default TriBoolean.USE_DEFAULT;

    /**
     * Set the target size for SSTables.
     */
    int ssTableSizeInMB() default AnnotationHelpers.INT_UNSPECIFIED;

    /**
     * Minimum time to wait after an SSTable creation time before considering the SSTable for tombstone compaction. *
     */
    int tombstoneCompactionIntervalInDay() default AnnotationHelpers.INT_UNSPECIFIED;

    /**
     * Ratio of garbage-collectable tombstones to all contained columns
     */
    double tombstoneThreshold() default AnnotationHelpers.DOUBLE_UNPSECIFIED;

    /**
     * enables more aggressive than normal tombstone compactions.
     */
    TriBoolean uncheckedTombstoneCompaction() default TriBoolean.USE_DEFAULT;

}

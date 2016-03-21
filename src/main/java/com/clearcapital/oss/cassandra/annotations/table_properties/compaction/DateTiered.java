package com.clearcapital.oss.cassandra.annotations.table_properties.compaction;

import com.clearcapital.oss.cassandra.annotations.table_properties.TriBoolean;

public @interface DateTiered {

    enum TimestampResolution {
        UNSPECIFIED,
        MICROSECONDS,
        MILLISECONDS
    }

    /**
     * Specifies that this is the compaction strategy which has been selected.
     */
    boolean selected() default true;

    /**
     * size of the first time window.
     */
    int baseTimeSeconds() default AnnotationHelpers.INT_UNSPECIFIED;
    
    /**
     * enable background compaction.
     */
    TriBoolean backgroundEnabled() default TriBoolean.USE_DEFAULT;
    
    /**
     * max_sstable_age_days 1000 Stop compacting SSTables only having data older than these specified days. Fractional
     * days can be set. This parameter is deprecated in Casandra3.2.
     */
    int maxSSTableAgeDays() default AnnotationHelpers.INT_UNSPECIFIED;
    
    /**
     * max_window_size_seconds 86,4000 Set the maximum window size in seconds. The default is 1 day.
     */
    int maxWindowSizeSeconds() default AnnotationHelpers.INT_UNSPECIFIED;
    
    /**
     * min_threshold 4 Set the minimum number of SSTables to trigger a minor compaction.
     */
    int minThreshold() default AnnotationHelpers.INT_UNSPECIFIED;

    /**
     * timestamp unit of the data you insert
     */
    TimestampResolution timestampResolution() default TimestampResolution.UNSPECIFIED;

    /**
     * Not documented by DS, but present in DSE driver.
     */
    int maxThreshold() default AnnotationHelpers.INT_UNSPECIFIED;

    /**
     * tombstone_compaction_interval 1 day Set to the minimum time to wait after an SSTable creation time before
     * considering the SSTable for tombstone compaction. Tombstone compaction is the compaction triggered if the SSTable
     * has more garbage-collectable tombstones than tombstone_threshold.
     */
    int tombstoneCompactionIntervalInDay() default AnnotationHelpers.INT_UNSPECIFIED;

    /**
     * tombstone_threshold 0.2 Set the ratio of garbage-collectable tombstones to all contained columns, which if
     * exceeded by the SSTable triggers compaction (with no other SSTables) for the purpose of purging the tombstones.
     */
    double tombstoneThreshold() default AnnotationHelpers.DOUBLE_UNPSECIFIED;

    /**
     * unchecked_tombstone_compaction false Set to True enables more aggressive than normal tombstone compactions. A
     * single SSTable tombstone compaction runs without checking the likelihood of success.
     */
    TriBoolean uncheckedTombstoneCompaction() default TriBoolean.USE_DEFAULT;
}

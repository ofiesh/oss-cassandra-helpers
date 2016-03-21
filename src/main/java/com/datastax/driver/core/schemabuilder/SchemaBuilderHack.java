package com.datastax.driver.core.schemabuilder;

import com.clearcapital.oss.cassandra.annotations.table_properties.compaction.DateTiered.TimestampResolution;
import com.datastax.driver.core.schemabuilder.TableOptions.CompactionOptions.DateTieredCompactionStrategyOptions;
import com.datastax.driver.core.schemabuilder.TableOptions.CompactionOptions.DateTieredCompactionStrategyOptions.TimeStampResolution;

/**
 * <p>
 * This is not ideal; {@link TimeStampResolution} ought to have been a public type.
 * </p>
 * 
 * <p>
 * Okay, let's be honest: this is an ugly hack.
 * </p>
 * 
 * <li>TODO: submit a driver patch which makes {@link TimeStampResolution} public
 * <li>TODO: get rid of this ugly hack.
 * 
 * @author eehlinger
 *
 */
public class SchemaBuilderHack {

    public static void encodeTimestampResolution(DateTieredCompactionStrategyOptions dateTieredStrategy,
            TimestampResolution timestampResolution) {
        switch (timestampResolution) {
            case UNSPECIFIED:
                return;
            case MICROSECONDS:
                dateTieredStrategy.timestampResolution(TimeStampResolution.MICROSECONDS);
                break;
            case MILLISECONDS:
                dateTieredStrategy.timestampResolution(TimeStampResolution.MILLISECONDS);
                break;
        }
    }
}

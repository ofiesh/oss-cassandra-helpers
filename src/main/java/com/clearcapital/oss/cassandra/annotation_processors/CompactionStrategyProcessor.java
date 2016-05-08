package com.clearcapital.oss.cassandra.annotation_processors;

import com.clearcapital.oss.cassandra.annotations.table_properties.Compaction;
import com.clearcapital.oss.cassandra.annotations.table_properties.TriBoolean;
import com.clearcapital.oss.cassandra.annotations.table_properties.compaction.DateTiered;
import com.clearcapital.oss.cassandra.annotations.table_properties.compaction.DateTiered.TimestampResolution;
import com.clearcapital.oss.cassandra.annotations.table_properties.compaction.Leveled;
import com.clearcapital.oss.cassandra.annotations.table_properties.compaction.SizeTiered;
import com.datastax.driver.core.schemabuilder.Create.Options;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.datastax.driver.core.schemabuilder.SchemaBuilderHack;
import com.datastax.driver.core.schemabuilder.TableOptions.CompactionOptions.DateTieredCompactionStrategyOptions;
import com.datastax.driver.core.schemabuilder.TableOptions.CompactionOptions.LeveledCompactionStrategyOptions;
import com.datastax.driver.core.schemabuilder.TableOptions.CompactionOptions.SizeTieredCompactionStrategyOptions;

public class CompactionStrategyProcessor {

    public static boolean encodeCompactionStrategy(Options output, Compaction compaction) {
        boolean result = false;
        
        if (compaction.compactStorage()) {
            result = true;
            output.compactStorage();
        }
        
        if (compaction.dateTiered().selected()) {
            encodeCompactionStrategy(output, compaction.dateTiered());
            return true;
        } else if (compaction.sizeTiered().selected()) {
            encodeCompactionStrategy(output, compaction.sizeTiered());
            return true;
        } else if (compaction.leveled().selected()) {
            encodeCompactionStrategy(output, compaction.leveled());
            return true;
        }
        return result;
    }

    static void encodeCompactionStrategy(Options result, DateTiered dateTiered) {
        // result.compactStorage();
        DateTieredCompactionStrategyOptions dateTieredStrategy = SchemaBuilder.dateTieredStrategy();
    
        if (dateTiered.baseTimeSeconds() >= 0) {
            dateTieredStrategy.baseTimeSeconds(dateTiered.baseTimeSeconds());
        }
    
        if (dateTiered.backgroundEnabled() != TriBoolean.USE_DEFAULT) {
            dateTieredStrategy.enabled(dateTiered.backgroundEnabled() == TriBoolean.TRUE);
        }
    
        if (dateTiered.maxSSTableAgeDays() >= 0) {
            dateTieredStrategy.maxSSTableAgeDays(dateTiered.maxSSTableAgeDays());
        }
    
        if (dateTiered.maxThreshold() >= 0) {
            dateTieredStrategy.maxThreshold(dateTiered.maxThreshold());
        }
    
        if (dateTiered.minThreshold() >= 0) {
            dateTieredStrategy.minThreshold(dateTiered.minThreshold());
        }
    
        if (dateTiered.timestampResolution() != TimestampResolution.UNSPECIFIED) {
            SchemaBuilderHack.encodeTimestampResolution(dateTieredStrategy, dateTiered.timestampResolution());
        }
    
        if (dateTiered.tombstoneCompactionIntervalInDay() >= 0) {
            dateTieredStrategy.tombstoneCompactionIntervalInDay(dateTiered.tombstoneCompactionIntervalInDay());
        }
    
        if (dateTiered.tombstoneThreshold() > 0) {
            dateTieredStrategy.tombstoneThreshold(dateTiered.tombstoneThreshold());
        }
    
        if (dateTiered.uncheckedTombstoneCompaction() != TriBoolean.USE_DEFAULT) {
            dateTieredStrategy
                    .uncheckedTombstoneCompaction(dateTiered.uncheckedTombstoneCompaction() == TriBoolean.TRUE);
        }
    
        result.compactionOptions(dateTieredStrategy);
    }

    static void encodeCompactionStrategy(Options result, SizeTiered sizeTiered) {
        // result.compactStorage();
        SizeTieredCompactionStrategyOptions sizedTieredStrategy = SchemaBuilder.sizedTieredStategy();
    
        if (sizeTiered.bucketHigh() >= 0) {
            sizedTieredStrategy.bucketHigh(sizeTiered.bucketHigh());
        }
    
        if (sizeTiered.bucketLow() >= 0) {
            sizedTieredStrategy.bucketLow(sizeTiered.bucketLow());
        }
    
        if (sizeTiered.coldReadsRatioToOmit() >= 0) {
            sizedTieredStrategy.coldReadsRatioToOmit(sizeTiered.coldReadsRatioToOmit());
        }
    
        if (sizeTiered.backgroundEnabled() != TriBoolean.USE_DEFAULT) {
            sizedTieredStrategy.enabled(sizeTiered.backgroundEnabled() == TriBoolean.TRUE);
        }
    
        if (sizeTiered.maxThreshold() >= 0) {
            sizedTieredStrategy.maxThreshold(sizeTiered.maxThreshold());
        }
    
        if (sizeTiered.minSSTableSizeInBytes() >= 0) {
            sizedTieredStrategy.minSSTableSizeInBytes(sizeTiered.minSSTableSizeInBytes());
        }
    
        if (sizeTiered.minThreshold() >= 0) {
            sizedTieredStrategy.minThreshold(sizeTiered.minThreshold());
        }
    
        if (sizeTiered.tombstoneCompactionIntervalInDay() >= 0) {
            sizedTieredStrategy.tombstoneCompactionIntervalInDay(sizeTiered.tombstoneCompactionIntervalInDay());
        }
    
        if (sizeTiered.tombstoneThreshold() >= 0) {
            sizedTieredStrategy.tombstoneThreshold(sizeTiered.tombstoneThreshold());
        }
    
        if (sizeTiered.uncheckedTombstoneCompaction() != TriBoolean.USE_DEFAULT) {
            sizedTieredStrategy
                    .uncheckedTombstoneCompaction(sizeTiered.uncheckedTombstoneCompaction() == TriBoolean.TRUE);
        }
    
        result.compactionOptions(sizedTieredStrategy);
    }

    static void encodeCompactionStrategy(Options result, Leveled leveled) {
        // result.compactStorage();
    
        LeveledCompactionStrategyOptions leveledStrategy = SchemaBuilder.leveledStrategy();
        if (leveled.backgroundEnabled() != TriBoolean.USE_DEFAULT) {
            leveledStrategy.enabled(leveled.backgroundEnabled() == TriBoolean.TRUE);
        }
    
        if (leveled.ssTableSizeInMB() >= 0) {
            leveledStrategy.ssTableSizeInMB(leveled.ssTableSizeInMB());
        }
    
        if (leveled.tombstoneCompactionIntervalInDay() >= 0) {
            leveledStrategy.tombstoneCompactionIntervalInDay(leveled.tombstoneCompactionIntervalInDay());
        }
    
        if (leveled.tombstoneThreshold() >= 0) {
            leveledStrategy.tombstoneThreshold(leveled.tombstoneThreshold());
        }
    
        if (leveled.uncheckedTombstoneCompaction() != TriBoolean.USE_DEFAULT) {
            leveledStrategy.uncheckedTombstoneCompaction(leveled.uncheckedTombstoneCompaction() == TriBoolean.TRUE);
        }
    
        result.compactionOptions(leveledStrategy);
    }


}

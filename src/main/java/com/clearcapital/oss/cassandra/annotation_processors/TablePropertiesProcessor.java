package com.clearcapital.oss.cassandra.annotation_processors;

import java.util.Objects;

import com.clearcapital.oss.cassandra.annotations.table_properties.Caching;
import com.clearcapital.oss.cassandra.annotations.table_properties.Compaction;
import com.clearcapital.oss.cassandra.annotations.table_properties.Compression;
import com.clearcapital.oss.cassandra.annotations.table_properties.CompressionMethod;
import com.clearcapital.oss.cassandra.annotations.table_properties.SpeculativeRetry;
import com.clearcapital.oss.cassandra.annotations.table_properties.TableProperties;
import com.clearcapital.oss.cassandra.annotations.table_properties.TriBoolean;
import com.datastax.driver.core.schemabuilder.Create.Options;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.datastax.driver.core.schemabuilder.TableOptions;
import com.datastax.driver.core.schemabuilder.TableOptions.CachingRowsPerPartition;
import com.datastax.driver.core.schemabuilder.TableOptions.SpeculativeRetryValue;

public class TablePropertiesProcessor {

    public static boolean encodeTableProperties(Options options, TableProperties properties) {
        boolean result = false;
        if (properties.bloomFilterFpChance() > 0.0) {
            options.bloomFilterFPChance(properties.bloomFilterFpChance());
            result = true;
        }

        if (!properties.comment().isEmpty()) {
            options.comment(properties.comment());
            result = true;
        }

        Caching caching = properties.caching();
        if (!caching.useDefault()) {
            options.caching(caching.keys(), encodeCachingRowsPerPartition(caching.rowsPerPartition()));
            result = true;
        }

        Compaction compaction = properties.compaction();
        result |= CompactionStrategyProcessor.encodeCompactionStrategy(options, compaction);

        Compression compression = properties.compression();
        if (compression.sstableCompression() != CompressionMethod.DEFAULT) {
            result |= encodeCompressionStrategy(options, compression);
        }

        if (properties.dclocalReadRepairChance() >= 0.0) {
            options.dcLocalReadRepairChance(properties.dclocalReadRepairChance());
            result = true;
        }

        if (properties.defaultTimeToLive() >= 0) {
            options.defaultTimeToLive(properties.defaultTimeToLive());
            result = true;
        }

        if (properties.gcGraceSeconds() >= 0) {
            options.gcGraceSeconds(properties.gcGraceSeconds());
            result = true;
        }

        if (properties.minIndexInterval() >= 0) {
            options.minIndexInterval(properties.minIndexInterval());
            result = true;
        }

        if (properties.maxIndexInterval() >= 0) {
            options.maxIndexInterval(properties.maxIndexInterval());
            result = true;
        }

        if (properties.memtableFlushPeriodInMs() >= 0) {
            options.memtableFlushPeriodInMillis(properties.memtableFlushPeriodInMs());
            result = true;
        }

        if (!Objects.equals(properties.populate_io_cache_on_flush(), TriBoolean.USE_DEFAULT)) {
            options.populateIOCacheOnFlush(properties.populate_io_cache_on_flush() == TriBoolean.TRUE);
            result = true;
        }

        if (properties.readRepairChance() >= 0) {
            options.readRepairChance(properties.readRepairChance());
            result = true;
        }

        SpeculativeRetry speculativeRetry = properties.speculativeRetry();
        if (!speculativeRetry.useDefault()) {
            SpeculativeRetryValue encoded = encodeSpeculativeRetry(speculativeRetry);
            options.speculativeRetry(encoded);
            result = true;
        }

        return result;
    }

    static boolean encodeCompressionStrategy(Options result, Compression compression) {
        TableOptions.CompressionOptions options = null;
        switch (compression.sstableCompression()) {
            case DEFAULT:
                return false;
            case NONE:
                result.compressionOptions(SchemaBuilder.noCompression());
                return true;
            case DEFLATE:
                options = SchemaBuilder.deflate();
                break;
            case LZ4:
                options = SchemaBuilder.lz4();
                break;
            case SNAPPY:
                options = SchemaBuilder.snappy();
                break;
        }

        if (compression.chunkLengthKB() != Compression.DEFAULT_CHUNK_LENGTH) {
            options.withChunkLengthInKb(compression.chunkLengthKB());
        }
        if (compression.crcCheckChance() != Compression.DEFAULT_CHECK_CHANCE) {
            options.withCRCCheckChance(compression.crcCheckChance());
        }
        result.compressionOptions(options);
        return true;
    }

    static CachingRowsPerPartition encodeCachingRowsPerPartition(int value) {
        switch (value) {
            case Caching.ROWS_UNSPECIFIED:
                return null;
            case Caching.ROWS_ALL:
                return SchemaBuilder.allRows();
            case Caching.ROWS_NONE:
                return SchemaBuilder.noRows();
            default:
                return SchemaBuilder.rows(value);
        }
    }

    static SpeculativeRetryValue encodeSpeculativeRetry(SpeculativeRetry speculativeRetry) {
        /**
         * {@link SchemaBuilder#noSpeculativeRetry()}, {@link SchemaBuilder#always()},
         * {@link SchemaBuilder#percentile(int)} or {@link SchemaBuilder#millisecs(int)}.
         */
        switch (speculativeRetry.when()) {
            case ALWAYS:
                return SchemaBuilder.always();
            case NONE:
                return SchemaBuilder.noSpeculativeRetry();
            case CHOOSE_PERCENTILE_THEN_MS:
                if (speculativeRetry.percentile() >= 0) {
                    return SchemaBuilder.percentile(speculativeRetry.percentile());
                } else {
                    return SchemaBuilder.millisecs(speculativeRetry.milliseconds());
                }
            default:
                throw new RuntimeException("Unexpected value for speculativeRetry.when()");
        }
    }

}

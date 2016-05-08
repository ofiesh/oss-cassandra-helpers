package com.clearcapital.oss.cassandra.annotations.table_properties;

import com.clearcapital.oss.cassandra.annotations.table_properties.compaction.DateTiered;
import com.clearcapital.oss.cassandra.annotations.table_properties.compaction.Leveled;
import com.clearcapital.oss.cassandra.annotations.table_properties.compaction.SizeTiered;

/**
 * See also: <a href='http://docs.datastax.com/en/cql/3.3/cql/cql_reference/compactSubprop.html'>DSE docs</a>
 */
public @interface Compaction {

    // Implementation notes: it may seem weird to have all of these properties populated with a "selected=false"
    // attribute, but there's no way to default to "null" for annotations.

    /**
     * Use the size tiered compaction strategy.
     */
    SizeTiered sizeTiered() default @SizeTiered(selected = false);

    /**
     * Use the date tiered compaction strategy.
     */
    DateTiered dateTiered() default @DateTiered(selected = false);

    /**
     * Use the level tiered compaction strategy.
     */
    Leveled leveled() default @Leveled(selected = false);

    /**
     * The table should have the WITH COMPACT STORAGE option. This is not implied, or even permissible, in all cases
     * where one of the compaction strategies ({@link #sizeTiered()}, {@link #dateTiered()}, {@link leveled}) are
     * applied.
     */
    boolean compactStorage() default false;
}

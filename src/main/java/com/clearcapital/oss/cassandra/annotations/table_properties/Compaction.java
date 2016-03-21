package com.clearcapital.oss.cassandra.annotations.table_properties;

import com.clearcapital.oss.cassandra.annotations.table_properties.compaction.DateTiered;
import com.clearcapital.oss.cassandra.annotations.table_properties.compaction.Leveled;
import com.clearcapital.oss.cassandra.annotations.table_properties.compaction.SizeTiered;

/**
 * 
 * See also: <a href='http://docs.datastax.com/en/cql/3.3/cql/cql_reference/compactSubprop.html'>DSE docs</a>
 * 
 * @author eehlinger
 */
public @interface Compaction {

    // Implementation notes: it may seem weird to have all of these properties populated with a "selected=false"
    // attribute, but there's no way to default to "null" for annotations.

    SizeTiered sizeTiered() default @SizeTiered(selected = false);

    DateTiered dateTiered() default @DateTiered(selected = false);

    Leveled leveled() default @Leveled(selected = false);
}

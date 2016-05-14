package com.clearcapital.oss.cassandra;

import org.apache.solr.client.solrj.SolrQuery;

public class SolrHelpers {

    public static String getGeolocationString(final Double latitude, final Double longitude) {
        if (latitude == null || longitude == null) {
            return null;
        }
        return "" + latitude + "," + longitude;
    }

    public static void addRangeQueryFilter(final SolrQuery query, final String fieldName, final Object[] range) {
        if (range != null && (range[0] != null || range[1] != null)) {
            String lowerBound = range[0] != null ? range[0].toString() : "*";
            String upperBound = range[1] != null ? range[1].toString() : "*";
            query.addFilterQuery(fieldName + ":[" + lowerBound + " TO " + upperBound + "]");
        }
    }

    public static void addRangeQueryFilter(final SolrQuery query, final String fieldName, final Object[] range,
            final boolean includeNulls) {
        if (includeNulls) {
            if (range != null && (range[0] != null || range[1] != null)) {
                String lowerBound = range[0] != null ? range[0].toString() : "*";
                String upperBound = range[1] != null ? range[1].toString() : "*";
                query.addFilterQuery("(-" + fieldName + ":[* TO *] AND *:*) OR " + fieldName + ":[" + lowerBound
                        + " TO " + upperBound + "]");
                // query.addFilterQuery("(-" + fieldName + ":[* TO *]) OR " + fieldName + ":[" + lowerBound + " TO "
                // + upperBound + "]");
            }
        } else {
            addRangeQueryFilter(query, fieldName, range);
        }
    }
}

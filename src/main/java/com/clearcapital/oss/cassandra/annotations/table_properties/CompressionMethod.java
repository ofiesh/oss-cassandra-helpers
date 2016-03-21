package com.clearcapital.oss.cassandra.annotations.table_properties;

public enum CompressionMethod {
    DEFAULT(null),
    NONE(""),
    LZ4("LZ4Compressor"),
    SNAPPY("SnappyCompressor"),
    DEFLATE("DeflateCompressor");

    private final String cqlName;

    public String getCqlName() {
        return cqlName;
    }

    CompressionMethod(String cqlName) {
        this.cqlName = cqlName;
    }
}

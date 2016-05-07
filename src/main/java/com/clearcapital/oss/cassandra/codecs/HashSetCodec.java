package com.clearcapital.oss.cassandra.codecs;

import java.util.HashSet;

public class HashSetCodec extends CollectionCodec {

    public HashSetCodec() {
        super(HashSet.class);
    }
}

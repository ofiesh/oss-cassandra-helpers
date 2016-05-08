package com.clearcapital.oss.cassandra.codecs;

import java.util.HashSet;

public class HashSetCodec extends CollectionCodec {

    @Override
    Class<?> getCollectionClass() {
        Class<?> result = HashSet.class;
        return result;
    }

}

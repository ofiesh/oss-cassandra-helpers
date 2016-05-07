package com.clearcapital.oss.cassandra.codecs;

import java.util.Collection;
import java.util.HashSet;

public class HashSetCodec extends CollectionCodec {

    @Override
    Class<? extends Collection<?>> getCollectionClass() {
        @SuppressWarnings("unchecked")
        Class<? extends Collection<?>> result = (Class<? extends Collection<?>>) HashSet.class;
        return result;
    }

}

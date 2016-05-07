package com.clearcapital.oss.cassandra.iterate;

import com.clearcapital.oss.cassandra.configuration.WithMultiRingConfiguration;
import com.clearcapital.oss.cassandra.multiring.MultiRingClientManager;

abstract public class RecordTransformerImpl<WithWalkerClass extends WalkerGenerator, ModelClass>
        implements RecordTransformer {

    abstract protected void transformRecord(ModelClass modelClass);

    abstract WalkerGenerator getWalkerGenerator();

    abstract CassandraRowDeserializer<ModelClass> getDeserializer();

    @Override
    public Long transformRecords(Long startToken, Long endToken) throws Exception {

        CassandraTableWalker<ModelClass> walker = getWalkerGenerator().getWalker(getDeserializer())
                .setStartToken(startToken).setEndToken(endToken).build();

        Long count = 0L;
        for (ModelClass modelClass : walker) {
            transformRecord(modelClass);
            count++;
        }
        return count;
    }

    @Override
    public void setConfiguration(WithMultiRingConfiguration configuration) throws Exception {

    }

    @Override
    public void setMultiRingClientManager(MultiRingClientManager clientManager) throws Exception {
        // TODO Auto-generated method stub

    }

}

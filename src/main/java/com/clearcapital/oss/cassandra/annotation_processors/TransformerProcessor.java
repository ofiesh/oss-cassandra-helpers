package com.clearcapital.oss.cassandra.annotation_processors;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.processing.SupportedAnnotationTypes;
import javax.annotation.processing.SupportedSourceVersion;
import javax.lang.model.SourceVersion;

import org.apache.cassandra.locator.EndpointSnitchInfoMBean;
import org.apache.cassandra.tools.NodeProbe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.clearcapital.oss.cassandra.annotations.Transformer;
import com.clearcapital.oss.cassandra.configuration.WithMultiRingConfiguration;
import com.clearcapital.oss.cassandra.exceptions.CassandraException;
import com.clearcapital.oss.cassandra.iterate.RecordTransformer;
import com.clearcapital.oss.cassandra.multiring.MultiRingClientManager;
import com.clearcapital.oss.java.AssertHelpers;
import com.clearcapital.oss.java.ReflectionHelpers;
import com.clearcapital.oss.java.exceptions.AssertException;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

@SupportedAnnotationTypes("com.clearcapital.dropwizard.cli.transformer.Transformer")
@SupportedSourceVersion(SourceVersion.RELEASE_7)
public class TransformerProcessor<T extends WithMultiRingConfiguration> {

    /**
     * Call processPredicate.apply() for every @{@link Transformer} -annotated class.
     * 
     * @param schema
     * @param processPredicate
     */
    public static Set<Class<?>> getTransformers(final Collection<String> packageNames) {
        Set<Class<?>> result = ReflectionHelpers.getTypesAnnotatedWith(packageNames, Transformer.class);
        return result;
    }

    public static Set<Class<?>> getTransformers(String packageName) {
        Set<Class<?>> result = ReflectionHelpers.getTypesAnnotatedWith(packageName, Transformer.class);
        return result;
    }

    public static boolean implementsRecordTransformer(Class<?> candidate) {
        if (candidate == null) {
            return false;
        }
        for (Class<?> transformerInterface : candidate.getInterfaces()) {
            if (transformerInterface == RecordTransformer.class) {
                return true;
            }
        }
        return false;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private static Logger log = LoggerFactory.getLogger(TransformerProcessor.Builder.class);

        Boolean listTransformers;
        String transformer;

        // VNode mode options:
        Boolean vnodesEnabled;
        String vnodeHost;
        String vnodeDC;
        Integer vnodeStart;

        // Non-Vnode mode options:
        Long nonVnodeStartToken;
        Long nonVnodeEndToken;

        WithMultiRingConfiguration configuration;
        MultiRingClientManager multiRingClientManager;

        public void execute() throws Exception {
            Set<Class<?>> transformers = TransformerProcessor.getTransformers("/");
            if (listTransformers) {
                this.executeListTransformers(transformers);
                return;
            }

            multiRingClientManager = new MultiRingClientManager(configuration.getMultiRingConfiguration());
            RecordTransformer recordTransformer = createTransformer(transformers);

            if (vnodesEnabled) {
                executeTransformerWithVnodes(recordTransformer);
            } else {
                executeTransformerWithoutVnodes(recordTransformer);
            }
        }

        private RecordTransformer createTransformer(Set<Class<?>> transformers)
                throws CassandraException, AssertException, InstantiationException, IllegalAccessException, Exception {
            if (transformer == null) {
                throw new CassandraException("transformer name is required");
            }
            log.info("=== Finding transformer: " + transformer);
            Class<?> transformerClass = Iterables.find(transformers, new Predicate<Class<?>>() {

                @Override
                public boolean apply(Class<?> input) {
                    return input.getName().equals(transformer) || input.getName().endsWith("." + transformer);
                }
            });

            AssertHelpers.isTrue(TransformerProcessor.implementsRecordTransformer(transformerClass),
                    "The provided class does not implement interface 'RecordTransformer'");

            RecordTransformer recordTransformer = (RecordTransformer) transformerClass.newInstance();

            recordTransformer.setConfiguration(configuration);
            recordTransformer.setMultiRingClientManager(multiRingClientManager);
            return recordTransformer;
        }

        private void executeListTransformers(Set<Class<?>> transformers) {
            log.info("=== Listing available transformers ===");
            for (Class<?> transformerClass : transformers) {
                log.info("* " + transformerClass.getName());
            }
        }

        private void executeTransformerWithVnodes(RecordTransformer recordTransformer) throws Exception {
            log.info("=== Executing transformer [vnode mode]: " + recordTransformer.getClass().getName());

            try (NodeProbe probe = new NodeProbe("127.0.0.1")) {
                Map<String, String> tokensToEndpoints = probe.getTokenToEndpointMap();
                log.debug("tokensToEndpoints:" + tokensToEndpoints);

                Map<String, String> tokensToEndpointsForDc = new LinkedHashMap<>();
                Long startToken = null;
                EndpointSnitchInfoMBean epSnitchInfo = probe.getEndpointSnitchInfoProxy();
                for (Entry<String, String> entry : tokensToEndpoints.entrySet()) {
                    String endpoint = entry.getValue();
                    String endpointDc = epSnitchInfo.getDatacenter(endpoint);
                    if (vnodeDC.equals(endpointDc)) {
                        tokensToEndpointsForDc.put(entry.getKey(), entry.getValue());
                        startToken = Long.parseLong(entry.getKey());
                    }
                }
                log.debug("tokensToEndpointsForDc:" + tokensToEndpointsForDc);

                // Finally - we've got the list of tokens on this DC.
                Long totalCounter = 0L;
                int vnodeIndex = 0;
                int totalVnodes = tokensToEndpointsForDc.entrySet().size();
                for (Entry<String, String> vnode : tokensToEndpointsForDc.entrySet()) {
                    Long endToken = Long.parseLong(vnode.getKey());
                    String endpoint = vnode.getValue();
                    Double percentage = 100.0 * vnodeIndex / totalVnodes;
                    String progressMessage = String.format("Token Range: %d => %d | Vnode %d/%d (%3.1f%%)", startToken,
                            endToken, vnodeIndex, totalVnodes, percentage);
                    if (vnodeIndex >= vnodeStart && endpoint.equals(vnodeHost)) {
                        while (true) {
                            try {
                                log.info(progressMessage);

                                Long recordsProcessed = recordTransformer.transformRecords(startToken, endToken);
                                totalCounter += recordsProcessed;

                            } catch (Exception e) {
                                log.debug(
                                        "Caught exception while processing vnode. Waiting 30 seconds and trying again.",
                                        e);
                                Thread.sleep(30000);
                            }
                        }
                    } else {
                        log.debug(progressMessage + " [skipped - owned by " + endpoint + "]");
                    }

                    startToken = endToken + 1L;
                    vnodeIndex++;
                }
                log.info(String.format("Total Records Processed: (total:%d)", totalCounter));
            }

        }

        private void executeTransformerWithoutVnodes(RecordTransformer recordTransformer) throws Exception {
            log.info("=== Executing transformer [non-vnode mode]: " + recordTransformer.getClass().getName());
            recordTransformer.transformRecords(nonVnodeStartToken, nonVnodeEndToken);
        }

        public Builder setListTransformers(Boolean value) {
            listTransformers = value;
            return this;
        }

        public Builder setTransformer(String value) {
            transformer = value;
            return this;
        }

        public Builder setVnodesEnabled(Boolean value) {
            vnodesEnabled = value;
            return this;
        }

        public Builder setVnodeDC(String value) {
            vnodeDC = value;
            return this;
        }

        public Builder setVnodeHost(String value) {
            vnodeHost = value;
            return this;
        }

        public Builder setVnodeStart(Integer value) {
            vnodeStart = value;
            return this;
        }

        public Builder setNonVnodeStartToken(Long value) {
            nonVnodeStartToken = value;
            return this;
        }

        public Builder setNonVnodeEndToken(Long value) {
            nonVnodeEndToken = value;
            return this;
        }

        public Builder setConfiguration(WithMultiRingConfiguration value) {
            this.configuration = value;
            return this;
        }
    }

}

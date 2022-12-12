package org.apache.pulsar.logstash.inputs;

import co.elastic.logstash.api.Configuration;
import co.elastic.logstash.api.Context;
import co.elastic.logstash.api.Input;
import co.elastic.logstash.api.LogstashPlugin;
import co.elastic.logstash.api.PluginConfigSpec;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.pulsar.client.api.BatchReceivePolicy;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.auth.AuthenticationKeyStoreTls;
import org.apache.pulsar.client.impl.auth.AuthenticationTls;

import java.lang.Math;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@LogstashPlugin(name = "pulsar")
public class Pulsar implements Input {

    private final static Logger logger = LogManager.getLogger(Pulsar.class);

    private final String id;
    private final CountDownLatch done = new CountDownLatch(1);
    private volatile boolean stopped;

    private PulsarClient client;
    private org.apache.pulsar.client.api.Consumer<byte[]> pulsarConsumer;

    private static final PluginConfigSpec<String> CONFIG_SERVICE_URL =
            PluginConfigSpec.stringSetting("serviceUrl", "pulsar://localhost:6650");

    // base config
    private Configuration config;

    // consumer config list

    // codec, plain, json
    private static final String CODEC_PLAIN = "plain";
    private static final String CODEC_JSON = "json";
    private static final PluginConfigSpec<String> CONFIG_CODEC =
            PluginConfigSpec.stringSetting("codec", CODEC_PLAIN);

    // topic Names, array
    private static final PluginConfigSpec<List<Object>> CONFIG_TOPICS =
            PluginConfigSpec.arraySetting("topics", null, false, true);

    // subscription name
    private static final PluginConfigSpec<String> CONFIG_SUBSCRIPTION_NAME =
            PluginConfigSpec.requiredStringSetting("subscriptionName");

    // consumer name
    private static final PluginConfigSpec<String> CONFIG_CONSUMER_NAME =
            PluginConfigSpec.stringSetting("consumerName");

    // subscription type: Exclusive,Failover,Shared,Key_shared
    private static final PluginConfigSpec<String> CONFIG_SUBSCRIPTION_TYPE =
            PluginConfigSpec.stringSetting("subscriptionType", "Shared");

    // subscription initial position: Latest,Earliest
    private static final PluginConfigSpec<String> CONFIG_SUBSCRIPTION_INITIAL_POSITION =
            PluginConfigSpec.stringSetting("subscriptionInitialPosition", "Latest");

    // TODO: support     decorate_events => true &     consumer_threads => 2 & metadata

    private static final PluginConfigSpec<Boolean> CONFIG_ENABLE_ASYNCHRONOUS_ACKNOWLEDGEMENTS =
            PluginConfigSpec.booleanSetting("enable_asynchronous_acknowledgements",false);

    private static final PluginConfigSpec<Boolean> CONFIG_ENABLE_NEGATIVE_ACKNOWLEDGEMENTS =
            PluginConfigSpec.booleanSetting("enable_negative_acknowledgements",true);

    private static final PluginConfigSpec<Boolean> CONFIG_ENABLE_BATCH_RECEIVE =
            PluginConfigSpec.booleanSetting("enable_batch_receive",false);

    private static final PluginConfigSpec<Long> CONFIG_BATCH_MAX_NUM_BYTES =
            PluginConfigSpec.numSetting("batch_max_num_bytes",1024*1024);

    private static final PluginConfigSpec<Long> CONFIG_BATCH_MAX_NUM_MESSAGES =
            PluginConfigSpec.numSetting("batch_max_num_messages",100);

    private static final PluginConfigSpec<Long> CONFIG_BATCH_TIMEOUT_MILLISECONDS =
            PluginConfigSpec.numSetting("batch_timeout_milliseconds",1000);

    private static final PluginConfigSpec<Boolean> CONFIG_BATCH_INDEX_ACKNOWLEDGEMENT_ENABLED =
            PluginConfigSpec.booleanSetting("batch_index_acknowledgement_enabled",false);

    private static final PluginConfigSpec<Long> CONFIG_RECEIVER_QUEUE_SIZE =
            PluginConfigSpec.numSetting("receiver_queue_size",1000);

    private static final PluginConfigSpec<Long> CONFIG_MAX_TOTAL_RECEIVER_QUEUE_SIZE_ACROSS_PARTITIONS =
            PluginConfigSpec.numSetting("max_total_receiver_queue_size_across_partitions",50000);

    private static final PluginConfigSpec<Boolean> CONFIG_POOL_MESSAGES =
            PluginConfigSpec.booleanSetting("pool_mesages",false);

    private static final PluginConfigSpec<Boolean> CONFIG_AUTO_UPDATE_PARTITIONS =
            PluginConfigSpec.booleanSetting("auto_update_partitions",true);

    private static final PluginConfigSpec<Long> CONFIG_AUTO_UPDATE_PARTITIONS_INTERVAL_SECONDS =
            PluginConfigSpec.numSetting("auto_update_partitions_interval_seconds",60);

    // TLS Config
    private static final String authPluginClassName = "org.apache.pulsar.client.impl.auth.AuthenticationKeyStoreTls";
    private static final List<String> protocols = Arrays.asList("TLSv1.2");
    private static final List<String> ciphers = Arrays.asList(
            "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256",
            "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384",
            "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256",
            "TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384"
    );

    private static final PluginConfigSpec<Boolean> CONFIG_ENABLE_TLS =
            PluginConfigSpec.booleanSetting("enable_tls",false);

    private static final PluginConfigSpec<Boolean> CONFIG_ALLOW_TLS_INSECURE_CONNECTION =
            PluginConfigSpec.booleanSetting("allow_tls_insecure_connection",false);

    private static final PluginConfigSpec<Boolean> CONFIG_ENABLE_TLS_HOSTNAME_VERIFICATION =
            PluginConfigSpec.booleanSetting("enable_tls_hostname_verification",true);

    private static final PluginConfigSpec<String> CONFIG_TLS_TRUST_STORE_PATH =
            PluginConfigSpec.stringSetting("tls_trust_store_path","");

    private static final PluginConfigSpec<String> CONFIG_TLS_TRUST_STORE_PASSWORD =
            PluginConfigSpec.stringSetting("tls_trust_store_password","");

    private static final PluginConfigSpec<String> CONFIG_TLS_CLIENT_CERT_FILE_PATH =
            PluginConfigSpec.stringSetting("tls_client_cert_file_path","");

    private static final PluginConfigSpec<String> CONFIG_TLS_CLIENT_KEY_FILE_PATH =
            PluginConfigSpec.stringSetting("tls_client_key_file_path","");

    private static final PluginConfigSpec<String> CONFIG_TLS_TRUST_CERTS_FILE_PATH =
            PluginConfigSpec.stringSetting("tls_trust_certs_file_path","");

    private static final PluginConfigSpec<String> CONFIG_AUTH_PLUGIN_CLASS_NAME =
            PluginConfigSpec.stringSetting("auth_plugin_class_name",authPluginClassName);

    private static final PluginConfigSpec<List<Object>> CONFIG_CIPHERS =
            PluginConfigSpec.arraySetting("ciphers", Collections.singletonList(ciphers), false, false);

    private static final PluginConfigSpec<List<Object>> CONFIG_PROTOCOLS =
            PluginConfigSpec.arraySetting("protocols", Collections.singletonList(protocols), false, false);

    public Pulsar(String id, Configuration config, Context context) {
        // constructors should validate configuration options
        this.id = id;
        this.config = config;
    }

    private void createConsumer() throws PulsarClientException {
        try {
            String serviceUrl = config.get(CONFIG_SERVICE_URL);

            String codec = config.get(CONFIG_CODEC);
            List<String> topics = config.get(CONFIG_TOPICS).stream().map(Object::toString).collect(Collectors.toList());
            String subscriptionName = config.get(CONFIG_SUBSCRIPTION_NAME);
            String consumerName = config.get(CONFIG_CONSUMER_NAME);
            String subscriptionType = config.get(CONFIG_SUBSCRIPTION_TYPE);
            String subscriptionInitialPosition = config.get(CONFIG_SUBSCRIPTION_INITIAL_POSITION);
            boolean enableTls = config.get(CONFIG_ENABLE_TLS);
            if (enableTls) {
                // pulsar TLS
                Boolean allowTlsInsecureConnection = config.get(CONFIG_ALLOW_TLS_INSECURE_CONNECTION);
                Boolean enableTlsHostnameVerification = config.get(CONFIG_ENABLE_TLS_HOSTNAME_VERIFICATION);
                Map<String, String> authMap = new HashMap<>();

                Set<String> cipherSet = new HashSet<>();
                Optional.ofNullable(config.get(CONFIG_CIPHERS)).ifPresent(
                        cipherList -> cipherList.forEach(cipher -> cipherSet.add(String.valueOf(cipher))));

                Set<String> protocolSet = new HashSet<>();
                 Optional.ofNullable(config.get(CONFIG_PROTOCOLS)).ifPresent(
                         protocolList -> protocolList.forEach(protocol -> protocolSet.add(String.valueOf(protocol))));

                // Since tls with trust store was supported previously check if client cert path is supplied in the
                // configuration. If a client cert not was supplied then proceed with JKS, otherwise look for file
                // locations
                String tlsTrustStorePath = config.get(CONFIG_TLS_TRUST_STORE_PATH);
                if("".equals(config.get(CONFIG_TLS_CLIENT_CERT_FILE_PATH))) {
                    // This code assumes the CA certs and the client cert / private key are going to be in the same
                    // JKS file. This is technically allowed in Java but is generally regarded as a bad security
                    // practice. The truststore is supposed to only contain public certs of the CAs to be trusted and
                    // the keystore is supposed to contain the client's identity: including the certificate (public)
                    // and the key (private)
                    authMap.put(AuthenticationKeyStoreTls.KEYSTORE_TYPE, "JKS");
                    authMap.put(AuthenticationKeyStoreTls.KEYSTORE_PATH, tlsTrustStorePath);
                    authMap.put(AuthenticationKeyStoreTls.KEYSTORE_PW, config.get(CONFIG_TLS_TRUST_STORE_PASSWORD));


                    logger.info("Attempting to create TLS Pulsar client to {} using protocols: [{}], ciphers: [{}]," +
                                "allowTlsInsecureConnection={}, enableTlsHostnameVerification={}, the trust store " +
                                "located at: {}",
                                serviceUrl, String.join(", ", protocolSet), String.join(", ",cipherSet),
                                allowTlsInsecureConnection, enableTlsHostnameVerification, tlsTrustStorePath);
                    client = PulsarClient.builder()
                            .serviceUrl(serviceUrl)
                            .tlsCiphers(cipherSet)
                            .tlsProtocols(protocolSet)
                            .allowTlsInsecureConnection(allowTlsInsecureConnection)
                            .enableTlsHostnameVerification(enableTlsHostnameVerification)
                            .tlsTrustStorePath(tlsTrustStorePath)
                            .tlsTrustStorePassword(config.get(CONFIG_TLS_TRUST_STORE_PASSWORD))
                            .authentication(config.get(CONFIG_AUTH_PLUGIN_CLASS_NAME),authMap)
                            .build();
                    logger.info("TLS Pulsar client successfully created with JKS-based keystore");
                } else {
                    String tlsClientCertFilePath = config.get(CONFIG_TLS_CLIENT_CERT_FILE_PATH);
                    String tlsClientKeyFilePath = config.get(CONFIG_TLS_CLIENT_KEY_FILE_PATH);
                    String tlsTrustCertsFilePath = config.get(CONFIG_TLS_TRUST_CERTS_FILE_PATH);

                    logger.info("Attempting to create TLS Pulsar client to {} using protocols: [{}], ciphers: [{}]," +
                                "allowTlsInsecureConnection={}, enableTlsHostnameVerification={}, the trust store " +
                                "located at: {}, client certificate located at: {} and the private key located at: {}",
                                 serviceUrl, String.join(", ", protocolSet), String.join(", ",cipherSet),
                                 allowTlsInsecureConnection, enableTlsHostnameVerification, tlsTrustStorePath,
                                 tlsClientCertFilePath, tlsClientKeyFilePath);
                    client = PulsarClient.builder()
                             .serviceUrl(serviceUrl)
                             //.tlsCiphers(cipherSet)
                             //.tlsProtocols(protocolSet)
                             .allowTlsInsecureConnection(allowTlsInsecureConnection)
                             .enableTlsHostnameVerification(enableTlsHostnameVerification)
                             .tlsTrustCertsFilePath(tlsTrustCertsFilePath)
                             .authentication(new AuthenticationTls(tlsClientCertFilePath,tlsClientKeyFilePath))
                             .build();
                    logger.info("TLS Pulsar client successfully created with file-system cert/key pair");
                }
            } else {
                client = PulsarClient.builder()
                        .serviceUrl(serviceUrl)
                        .build();
            }

            // Create a consumer
            ConsumerBuilder<byte[]> consumerBuilder = client.newConsumer()
                    .topics(topics)
                    .subscriptionName(subscriptionName)
                    .subscriptionType(getSubscriptionType())
                    .subscriptionInitialPosition(getSubscriptionInitialPosition());
            if (consumerName != null) {
                consumerBuilder.consumerName(consumerName);
            }

            if(config.get(CONFIG_ENABLE_BATCH_RECEIVE)) {
                int maxNumBytes = Math.toIntExact((Long)config.get(CONFIG_BATCH_MAX_NUM_BYTES));
                int maxNumMessages = Math.toIntExact((Long)config.get(CONFIG_BATCH_MAX_NUM_MESSAGES));
                int batchTimeoutMilliseconds = Math.toIntExact((Long)config.get(CONFIG_BATCH_TIMEOUT_MILLISECONDS));
                BatchReceivePolicy batchReceivePolicy = BatchReceivePolicy.builder()
                    .maxNumBytes(maxNumBytes)
                    .maxNumMessages(maxNumMessages)
                    .timeout(batchTimeoutMilliseconds, TimeUnit.MILLISECONDS)
                    .build();
                consumerBuilder.batchReceivePolicy(batchReceivePolicy);
            }
            boolean batchIndexAcknowledgementEnabled = config.get(CONFIG_BATCH_INDEX_ACKNOWLEDGEMENT_ENABLED);
            consumerBuilder.enableBatchIndexAcknowledgment(batchIndexAcknowledgementEnabled);

            boolean autoUpdatePartitions = config.get(CONFIG_AUTO_UPDATE_PARTITIONS);
            consumerBuilder.autoUpdatePartitions(autoUpdatePartitions);
            if(autoUpdatePartitions) {
                int autoUpdatePartitionsIntervalSeconds = Math.toIntExact((Long)config.get(CONFIG_AUTO_UPDATE_PARTITIONS_INTERVAL_SECONDS));
                consumerBuilder.autoUpdatePartitionsInterval(autoUpdatePartitionsIntervalSeconds, TimeUnit.SECONDS);
            }

            int maxReceiverQueueSize = Math.toIntExact((Long)config.get(CONFIG_RECEIVER_QUEUE_SIZE));
            consumerBuilder.receiverQueueSize(maxReceiverQueueSize);

            int maxTotalReceiverQueueSizeAcrossPartitions = Math.toIntExact((Long)config.get(CONFIG_MAX_TOTAL_RECEIVER_QUEUE_SIZE_ACROSS_PARTITIONS));
            consumerBuilder.maxTotalReceiverQueueSizeAcrossPartitions(maxTotalReceiverQueueSizeAcrossPartitions);
            
            boolean poolMessages = config.get(CONFIG_POOL_MESSAGES);
            consumerBuilder.poolMessages(poolMessages);
            pulsarConsumer = consumerBuilder.subscribe();
            logger.info("Create subscription {} on topics {} with codec {}, consumer name is {},subscription Type is {},subscriptionInitialPosition is {}", subscriptionName, topics, codec , consumerName, subscriptionType, subscriptionInitialPosition);

        } catch (PulsarClientException e) {
            logger.error("pulsar client exception ", e);
            throw e;
        }
    }

    private SubscriptionInitialPosition getSubscriptionInitialPosition() {
        SubscriptionInitialPosition position;
        switch (config.get(CONFIG_SUBSCRIPTION_INITIAL_POSITION)) {
            case "Latest":
                position = SubscriptionInitialPosition.Latest;
                break;
            case "Earliest":
                position = SubscriptionInitialPosition.Earliest;
                break;
            default:
                position = SubscriptionInitialPosition.Latest;
                logger.warn("{} is not one known subscription initial position! 'Latest' will be used! [Latest,Earliest]", config.get(CONFIG_SUBSCRIPTION_INITIAL_POSITION));

        }
        return position;
    }

    private SubscriptionType getSubscriptionType() {
        SubscriptionType type;
        switch (config.get(CONFIG_SUBSCRIPTION_TYPE)) {
            case "Exclusive":
                type = SubscriptionType.Exclusive;
                break;
            case "Failover":
                type = SubscriptionType.Failover;
                break;
            case "Shared":
                type = SubscriptionType.Shared;
                break;
            case "Key_Shared":
                type = SubscriptionType.Key_Shared;
                break;
            default:
                type = SubscriptionType.Shared;
                logger.warn("{} is not one known subscription type! 'Shared' type will be used! [Exclusive,Failover,Shared,Key_Shared]",
                        config.get(CONFIG_SUBSCRIPTION_TYPE));
        }
        return type;
    }

    private void closePulsarConsumer() {
        try {
            pulsarConsumer.close();
            client.close();
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }
    }

    private void processIndividualMessage(final Consumer<Map<String, Object>> consumer, final Gson gson,
            final Type gsonType, final Message<byte[]> message) {
        String messageString = new String(message.getData());
        if (config.get(CONFIG_CODEC).equals(CODEC_JSON)) {
             try {
                 Map map = gson.fromJson(messageString, gsonType);
                 consumer.accept(map);
                 return;
             } catch (Exception e) {
                 logger.error("Exception ocurred while parsing JSON message with key: " + message.getKey() +
                     " enable debug to see message contents. Falling back to plain codec.", e);
                 logger.debug("Message content is {}", messageString);
             }
        }
        consumer.accept(Collections.singletonMap("message", messageString));
    }

    private void processBatchOfMessages(final Consumer<Map<String, Object>> consumer, final Gson gson,
            final Type gsonType, final Messages<byte[]> messages) {
        if(null != messages && messages.size() > 0) {
            messages.forEach(message -> processIndividualMessage(consumer, gson, gsonType, message));
        }
    }

    @Override
    public void start(Consumer<Map<String, Object>> consumer) {

        // The start method should push Map<String, Object> instances to the supplied QueueWriter
        // instance. Those will be converted to Event instances later in the Logstash event
        // processing pipeline.
        //
        // Inputs that operate on unbounded streams of data or that poll indefinitely for new
        // events should loop indefinitely until they receive a stop request. Inputs that produce
        // a finite sequence of events should loop until that sequence is exhausted or until they
        // receive a stop request, whichever comes first.

        boolean useMessageBatching = config.get(CONFIG_ENABLE_BATCH_RECEIVE);
        boolean useAsynchronousAcknowledgements = config.get(CONFIG_ENABLE_ASYNCHRONOUS_ACKNOWLEDGEMENTS);
        boolean useNegativeAcknowledgements = config.get(CONFIG_ENABLE_ASYNCHRONOUS_ACKNOWLEDGEMENTS);

        try {
            createConsumer();
            Gson gson = new Gson();
            Type gsonType = new TypeToken<Map>(){}.getType();
            
            while (!stopped) {
                if(useMessageBatching) {
                    Messages<byte[]> messages = null;
                    try {
                        messages = pulsarConsumer.batchReceive();
                        if(messages == null) {
                            continue;
                        }

                        processBatchOfMessages(consumer, gson, gsonType, messages);

                        if(useAsynchronousAcknowledgements) {
                            pulsarConsumer.acknowledgeAsync(messages);
                        } else {
                            pulsarConsumer.acknowledge(messages);
                        }
                    } catch(Exception e) {
                        if(useNegativeAcknowledgements && messages != null) {
                            pulsarConsumer.negativeAcknowledge(messages);
                        }
                    }
                } else {
                    Message<byte[]> message = null;
                    try {
                        message = pulsarConsumer.receive(1000, TimeUnit.MILLISECONDS);
                        if(message == null) {
                            continue;
                        }

                        processIndividualMessage(consumer, gson, gsonType, message);

                        if(useAsynchronousAcknowledgements) {
                            pulsarConsumer.acknowledgeAsync(message);
                        } else {
                            pulsarConsumer.acknowledge(message);
                        }
                    } catch(Exception e) {
                        if(useNegativeAcknowledgements && message != null) {
                            pulsarConsumer.negativeAcknowledge(message);
                        }
                    }
                }
            }
        } catch (PulsarClientException e) {
            logger.error("Error ocurred interrupting the receive loop!", e);
        } finally {
            stopped = true;
            done.countDown();
        }
    }

    @Override
    public void stop() {
        stopped = true; // set flag to request cooperative stop of input
        closePulsarConsumer();
    }

    @Override
    public void awaitStop() throws InterruptedException {
        done.await(); // blocks until input has stopped
    }

    @Override
    public Collection<PluginConfigSpec<?>> configSchema() {
        // should return a list of all configuration options for this plugin
        return Arrays.asList(
                CONFIG_SERVICE_URL,
                CONFIG_TOPICS,
                CONFIG_SUBSCRIPTION_NAME,
                CONFIG_SUBSCRIPTION_TYPE,
                CONFIG_SUBSCRIPTION_INITIAL_POSITION,
                CONFIG_CONSUMER_NAME,
                CONFIG_CODEC,
                CONFIG_ENABLE_BATCH_RECEIVE,
                CONFIG_ENABLE_ASYNCHRONOUS_ACKNOWLEDGEMENTS,
                CONFIG_ENABLE_NEGATIVE_ACKNOWLEDGEMENTS,
                CONFIG_BATCH_MAX_NUM_BYTES,
                CONFIG_BATCH_MAX_NUM_MESSAGES,
                CONFIG_BATCH_TIMEOUT_MILLISECONDS,
                CONFIG_BATCH_INDEX_ACKNOWLEDGEMENT_ENABLED,
                CONFIG_POOL_MESSAGES,
                CONFIG_AUTO_UPDATE_PARTITIONS,
                CONFIG_AUTO_UPDATE_PARTITIONS_INTERVAL_SECONDS,
                CONFIG_MAX_TOTAL_RECEIVER_QUEUE_SIZE_ACROSS_PARTITIONS,
                CONFIG_RECEIVER_QUEUE_SIZE,
                CONFIG_ENABLE_TLS,
                CONFIG_ALLOW_TLS_INSECURE_CONNECTION,
                CONFIG_ENABLE_TLS_HOSTNAME_VERIFICATION,
                CONFIG_TLS_TRUST_STORE_PATH,
                CONFIG_TLS_TRUST_STORE_PASSWORD,
                CONFIG_TLS_CLIENT_CERT_FILE_PATH,
                CONFIG_TLS_CLIENT_KEY_FILE_PATH,
                CONFIG_TLS_TRUST_CERTS_FILE_PATH,
                CONFIG_AUTH_PLUGIN_CLASS_NAME,
                CONFIG_CIPHERS,
                CONFIG_PROTOCOLS
        );
    }

    @Override
    public String getId() {
        return this.id;
    }


}

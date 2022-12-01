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
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.auth.AuthenticationKeyStoreTls;
import org.apache.pulsar.client.impl.auth.AuthenticationTls;

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

                // Since tls with trust store was supported previously check if trust store path is supplied in the
                // configuration. If a trust store was not supplied but enableTls is true then look for path to cert
                // and key in the properties.
                String tlsTrustStorePath = config.get(CONFIG_TLS_TRUST_STORE_PATH);
                if("".equals(tlsTrustStorePath)) {
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

                    logger.info("Attempting to create TLS Pulsar client to {} using protocols: [{}], ciphers: [{}]," +
                                "allowTlsInsecureConnection={}, enableTlsHostnameVerification={}, the trust store " +
                                "located at: {}, client certificate located at: {} and the private key located at: {}",
                                 serviceUrl, String.join(", ", protocolSet), String.join(", ",cipherSet),
                                 allowTlsInsecureConnection, enableTlsHostnameVerification, tlsTrustStorePath,
                                 tlsClientCertFilePath, tlsClientKeyFilePath);
                    client = PulsarClient.builder()
                             .serviceUrl(serviceUrl)
                             .tlsCiphers(cipherSet)
                             .tlsProtocols(protocolSet)
                             .allowTlsInsecureConnection(allowTlsInsecureConnection)
                             .enableTlsHostnameVerification(enableTlsHostnameVerification)
                             .tlsTrustStorePath(tlsTrustStorePath)
                             .tlsTrustStorePassword(config.get(CONFIG_TLS_TRUST_STORE_PASSWORD))
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

        try {
            createConsumer();
            Message<byte[]> message = null;
            String msgString = null;
            Gson gson = new Gson();
            Type gsonType = new TypeToken<Map>(){}.getType();
            while (!stopped) {
                try {
                    // Block and wait until a single message is available
                    message = pulsarConsumer.receive(1000, TimeUnit.MILLISECONDS);
                    if(message == null){
                        continue;
                    }
                    msgString = new String(message.getData());

                    if (config.get(CONFIG_CODEC).equals(CODEC_JSON)) {
                        try {
                            Map map = gson.fromJson(msgString, gsonType);
                            consumer.accept(map);
                        } catch (Exception e) {
                            // json parse exception
                            // treat it as codec plain
                            logger.error("json parse exception ", e);
                            logger.error("message key is {}, set logging level to debug if you'd like to see message", message.getKey());
                            logger.debug("message content is {}", msgString);
                            logger.error("default codec plain will be used ");

                            consumer.accept(Collections.singletonMap("message", msgString));
                        }
                    } else {
                        // default codec: plain
                        consumer.accept(Collections.singletonMap("message", msgString));
                    }


                    // Acknowledge the message so that it can be
                    // deleted by the message broker
                    pulsarConsumer.acknowledge(message);
                } catch (Exception e) {

                    // Message failed to process, redeliver later
                    logger.error("consume exception ", e);
                    if (message != null) {
                        pulsarConsumer.negativeAcknowledge(message);
                        logger.error("message is {}:{}", message.getKey(), msgString);
                    }
                }

            }
        } catch (PulsarClientException e) {
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
                CONFIG_CODEC
        );
    }

    @Override
    public String getId() {
        return this.id;
    }


}

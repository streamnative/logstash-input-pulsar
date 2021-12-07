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

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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

    private final String serviceUrl;
    private static final PluginConfigSpec<String> CONFIG_SERVICE_URL =
            PluginConfigSpec.stringSetting("serviceUrl", "pulsar://localhost:6650");

    // consumer config list

    // codec, plain, json
    private final String codec;
    private static final String CODEC_PLAIN = "plain";
    private static final String CODEC_JSON = "json";
    private static final PluginConfigSpec<String> CONFIG_CODEC =
            PluginConfigSpec.stringSetting("codec", CODEC_PLAIN);

    // topic Names, array
    private final List<String> topics;
    private static final PluginConfigSpec<List<Object>> CONFIG_TOPICS =
            PluginConfigSpec.arraySetting("topics", null, false, true);

    // subscription name
    private final String subscriptionName;
    private static final PluginConfigSpec<String> CONFIG_SUBSCRIPTION_NAME =
            PluginConfigSpec.requiredStringSetting("subscriptionName");

    // consumer name
    private final String consumerName;
    private static final PluginConfigSpec<String> CONFIG_CONSUMER_NAME =
            PluginConfigSpec.stringSetting("consumerName");

    // subscription type: Exclusive,Failover,Shared,Key_shared
    private final String subscriptionType;
    private static final PluginConfigSpec<String> CONFIG_SUBSCRIPTION_TYPE =
            PluginConfigSpec.stringSetting("subscriptionType", "Shared");

    // subscription initial position: Latest,Earliest
    private final String subscriptionInitialPosition;
    private static final PluginConfigSpec<String> CONFIG_SUBSCRIPTION_INITIAL_POSITION =
            PluginConfigSpec.stringSetting("subscriptionInitialPosition", "Latest");

    // TODO: support     decorate_events => true &     consumer_threads => 2 & metadata

    public Pulsar(String id, Configuration config, Context context) {
        // constructors should validate configuration options
        this.id = id;

        serviceUrl = config.get(CONFIG_SERVICE_URL);

        codec = config.get(CONFIG_CODEC);
        topics = config.get(CONFIG_TOPICS).stream().map(Object::toString).collect(Collectors.toList());
        subscriptionName = config.get(CONFIG_SUBSCRIPTION_NAME);
        consumerName = config.get(CONFIG_CONSUMER_NAME);
        subscriptionType = config.get(CONFIG_SUBSCRIPTION_TYPE);
        subscriptionInitialPosition = config.get(CONFIG_SUBSCRIPTION_INITIAL_POSITION);
    }

    private void createConsumer() throws PulsarClientException {
        try {

            client = PulsarClient.builder()
                    .serviceUrl(serviceUrl)
                    .build();

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
        switch (subscriptionInitialPosition) {
            case "Latest":
                position = SubscriptionInitialPosition.Latest;
                break;
            case "Earliest":
                position = SubscriptionInitialPosition.Earliest;
                break;
            default:
                position = SubscriptionInitialPosition.Latest;
                logger.warn("{} is not one known subscription initial position! 'Latest' will be used! [Latest,Earliest]", subscriptionInitialPosition);

        }
        return position;
    }

    private SubscriptionType getSubscriptionType() {
        SubscriptionType type;
        switch (subscriptionType) {
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
                logger.warn("{} is not one known subscription type! 'Shared' type will be used! [Exclusive,Failover,Shared,Key_Shared]", subscriptionType);
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

                    if (codec.equals(CODEC_JSON)) {
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

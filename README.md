# Logstash Input Pulsar Plugin

This is a Java plugin for [Logstash](https://github.com/elastic/logstash).

It is fully free and fully open source. The license is Apache 2.0, meaning you are free to use it however you want.

This input will read events from an [Apache Pulsar](https://pulsar.apache.org/) topic and feed them into a Logstash pipeline.

This plugin uses Pulsar Client 4.0.9. For broker compatibility, see the official [Pulsar client compatibility matrix](https://pulsar.apache.org/docs/next/client-libraries/). If you require features not yet available in this plugin (including client version upgrades), please file an issue with details about what you need.

## Configuration Options

| Setting                          | Input type | Required | Default                  |
|----------------------------------|:----------:|:--------:|--------------------------|
| serviceUrl                       | string     | No       | `pulsar://localhost:6650`|
| topics                           | array      | Yes      |                          |
| subscriptionName                 | string     | Yes      |                          |
| consumerName                     | string     | No       |                          |
| subscriptionType                 | string     | No       | `Shared`                 |
| subscriptionInitialPosition      | string     | No       | `Latest`                 |
| codec                            | string     | No       | `plain`                  |
| enable_tls                       | boolean    | No       | `false`                  |
| tls_trust_store_path             | string     | No       | `""`                     |
| tls_trust_store_password         | string     | No       | `""`                     |
| enable_tls_hostname_verification | boolean    | No       | `true`                   |
| allow_tls_insecure_connection    | boolean    | No       | `false`                  |
| protocols                        | array      | No       | `["TLSv1.2"]`           |
| ciphers                          | array      | No       | *(see below)*            |
| auth_plugin_class_name           | string     | No       | `""`                     |
| auth_params                      | string     | No       | `""`                     |

### Setting Details

#### `serviceUrl`
The Pulsar service URL. Use `pulsar://` for plaintext or `pulsar+ssl://` for TLS connections.

#### `topics`
A list of Pulsar topic names to consume from.

#### `subscriptionName`
The subscription name for the consumer. **Required.**

#### `consumerName`
An optional name for the consumer instance.

#### `subscriptionType`
One of: `Exclusive`, `Failover`, `Shared`, `Key_Shared`. Default: `Shared`.

#### `subscriptionInitialPosition`
Where to start reading if no existing subscription is found. One of: `Latest`, `Earliest`. Default: `Latest`.

#### `codec`
How to decode messages. `plain` treats the message as a raw string placed in a `message` field. `json` parses the message body as JSON and maps its fields directly into the Logstash event (falls back to `plain` on parse errors). Default: `plain`.

#### `enable_tls`
Set to `true` to enable TLS authentication using a JKS keystore.

#### `tls_trust_store_path`
Path to the JKS trust store file.

#### `tls_trust_store_password`
Password for the JKS trust store.

#### `enable_tls_hostname_verification`
Enable TLS hostname verification. Default: `true`.

#### `allow_tls_insecure_connection`
Allow insecure TLS connections. Default: `false`.

#### `protocols`
List of TLS protocol versions. Default: `["TLSv1.2"]`.

#### `ciphers`
List of TLS cipher suites. Default includes `TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256`, `TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384`, `TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256`, `TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384`.

#### `auth_plugin_class_name`
The fully qualified class name of the Pulsar authentication plugin (e.g., `org.apache.pulsar.client.impl.auth.AuthenticationToken`). Used when `enable_tls` is `false`.

#### `auth_params`
Authentication parameters as a string (e.g., `token:<your-token>`). Used with `auth_plugin_class_name`.

## Examples

### Basic (no authentication)

```ruby
input {
  pulsar {
    serviceUrl => "pulsar://127.0.0.1:6650"
    codec => "json"
    topics => [
      "persistent://public/default/topic1",
      "persistent://public/default/topic2"
    ]
    subscriptionName => "my_subscription"
    subscriptionType => "Shared"
    subscriptionInitialPosition => "Earliest"
  }
}
```

### With token authentication

```ruby
input {
  pulsar {
    serviceUrl => "pulsar://127.0.0.1:6650"
    codec => "plain"
    topics => [
      "persistent://public/default/topic1",
      "persistent://public/default/topic2"
    ]
    subscriptionName => "my_subscription"
    subscriptionType => "Shared"
    subscriptionInitialPosition => "Earliest"
    auth_plugin_class_name => "org.apache.pulsar.client.impl.auth.AuthenticationToken"
    auth_params => "token:eyJhbGciOiJIUzI1NiJ9..."
  }
}
```

### With TLS (JKS keystore)

```ruby
input {
  pulsar {
    serviceUrl => "pulsar+ssl://pulsar.example.com:6651"
    topics => ["persistent://tenant/namespace/my-topic"]
    subscriptionName => "my_subscription"
    enable_tls => true
    tls_trust_store_path => "/path/to/truststore.jks"
    tls_trust_store_password => "changeit"
    enable_tls_hostname_verification => true
  }
}
```

## Installation

1. Get the latest gem or zip file from the [releases page](https://github.com/streamnative/logstash-input-pulsar/releases).

2. Install the plugin:

```sh
bin/logstash-plugin install --no-verify --local logstash-input-pulsar-3.0.0.0.gem
```

## Building from Source

You need a local Logstash source checkout. Set the `LOGSTASH_CORE_PATH` environment variable or pass it as a Gradle property:

```sh
# Clone Logstash and build the core JAR
git clone https://github.com/elastic/logstash.git
cd logstash
git checkout tags/v7.17.6
./gradlew assemble

# In root of this repo:
./gradlew gem -PLOGSTASH_CORE_PATH=../logstash/logstash-core
```

This will produce a `.gem` file in the project root that can be installed with `logstash-plugin install`.

## License

Apache License 2.0

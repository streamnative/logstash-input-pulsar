# Logstash Input Pulsar Plugin

This is a Java plugin for [Logstash](https://github.com/elastic/logstash).

It is fully free and fully open source. The license is Apache 2.0, meaning you are free to use it however you want.

This input will read events from a Pulsar topic.

This plugin uses Pulsar Client 2.9.0. For broker compatibility, see the official Pulsar compatibility reference. If the compatibility wiki is not up-to-date, please contact Pulsar support/community to confirm compatibility.

If you require features not yet available in this plugin (including client version upgrades), please file an issue with details about what you need.

# Pulsar Input Configuration Options
This plugin supports these configuration options. 

| Settings                         |                          Input type                          | Required |
|----------------------------------|:------------------------------------------------------------:|---------:|
| codec                            |               string, one of ["plain","json"]                |       No |
| topics                           |                            array                             |      Yes |
| subscriptionName                 |                            string                            |      Yes |
| consumerName                     |                            string                            |       No |
| subscriptionType                 | string, one of["Shared","Exclusive","Failover","Key_shared"] |       No |
| subscriptionInitialPosition      |             string, one of["Latest","Earliest"]              |       No |
| enable_tls                       |       boolean, one of [true, false]. default is false        |       No |
| tls_trust_store_path             |        string, required if enable_tls is set to true         |       No |
| tls_trust_store_password         |                   string, default is empty                   |       No |
| tls_key_store_path               |                   string, default is empty                   |       No |
| tls_key_store_password           |                   string, default is empty                   |       No |
| enable_tls_hostname_verification |       boolean, one of [true, false]. default is false        |       No |
| protocols                        |           array, ciphers list. default is TLSv1.2            |       No |
| allow_tls_insecure_connection    |        boolean, one of [true, false].default is false        |       No |
| auth_plugin_class_name           |                            string                            |       No |
| ciphers                          |                     array, ciphers list                      |       No |


# Example

```
input{
  pulsar{
    serviceUrl => "pulsar://127.0.0.1:6650"
    codec => "json"
    topics => [ 
        "persistent://public/default/topic1", 
        "persistent://public/default/topic2"
    ]
    subscriptionName => "my_consumer"
    subscriptionType => "Shared"
    subscriptionInitialPosition => "Earliest"
  }
}
```


# Installation

1. Get the latest zip file from release page.
https://github.com/streamnative/logstash-input-pulsar/releases

2. Install this plugin using logstash preoffline command.

```
bin/logstash-plugin install file://{PATH_TO}/logstash-input-pulsar-2.10.0.0.zip
```

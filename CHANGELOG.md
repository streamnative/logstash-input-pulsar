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

    private static final PluginConfigSpec<String> CONFIG_AUTH_PLUGIN_CLASS_NAME =
            PluginConfigSpec.stringSetting("auth_plugin_class_name",authPluginClassName);

    private static final PluginConfigSpec<List<Object>> CONFIG_CIPHERS =
            PluginConfigSpec.arraySetting("ciphers", Collections.singletonList(ciphers), false, false);

    private static final PluginConfigSpec<List<Object>> CONFIG_PROTOCOLS =
            PluginConfigSpec.arraySetting("protocols", Collections.singletonList(protocols), false, false);

## 2.7.1
- First release based on Pulsar Client 2.7.1
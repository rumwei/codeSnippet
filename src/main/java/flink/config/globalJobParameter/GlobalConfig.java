package flink.config.globalJobParameter;

import org.apache.flink.api.common.ExecutionConfig;

import java.util.HashMap;
import java.util.Map;

public class GlobalConfig extends ExecutionConfig.GlobalJobParameters {

    private static final int ONLINE = 1;

    private static final int TEST = 0;

    private final Map<String, Object> configs;

    private int profile;

    public GlobalConfig(int profile) {
        super();
        this.profile = profile;
        this.configs = new HashMap<>();
        if (profile == TEST) {
            configs.put("env", "test");
        } else if (profile == ONLINE) {
            configs.put("env", "online");
        }
    }

    public String getString(String key) {
        return (String) configs.getOrDefault(key, null);
    }
}

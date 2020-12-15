package com.fanpan26.akfak.common.protocol;

/**
 * @author fanyuepan
 */
public enum  ApiKeys {

    PRODUCE(0, "Produce"),
    METADATA(3, "Metadata");

    public final short id;

    public final String name;

    private static final ApiKeys[] ID_TO_TYPE;
    private static final int MIN_API_KEY = 0;
    public static final int MAX_API_KEY;

    static {
        int maxKey = -1;
        for (ApiKeys key : ApiKeys.values()) {
            maxKey = Math.max(maxKey, key.id);
        }
        ApiKeys[] idToType = new ApiKeys[maxKey + 1];
        for (ApiKeys key : ApiKeys.values()) {
            idToType[key.id] = key;
        }
        ID_TO_TYPE = idToType;
        MAX_API_KEY = maxKey;
    }

    ApiKeys(int id, String name) {
        this.id = (short) id;
        this.name = name;
    }

    public static ApiKeys forId(int id) {
        if (id < MIN_API_KEY || id > MAX_API_KEY) {
            throw new IllegalArgumentException(String.format("Unexpected ApiKeys id `%s`, it should be between `%s` " +
                    "and `%s` (inclusive)", id, MIN_API_KEY, MAX_API_KEY));
        }
        return ID_TO_TYPE[id];
    }
}

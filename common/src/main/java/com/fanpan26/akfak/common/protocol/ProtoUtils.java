package com.fanpan26.akfak.common.protocol;

import com.fanpan26.akfak.common.protocol.types.Schema;
import com.fanpan26.akfak.common.protocol.types.Struct;

import java.nio.ByteBuffer;

/**
 * @author fanyuepan
 */
public class ProtoUtils {

    private static Schema schemaFor(Schema[][] schemas,int apiKey,int version){
        if (apiKey < 0 || apiKey > schemas.length) {
            throw new IllegalArgumentException("Invalid api key: " + apiKey);
        }
        Schema[] versions = schemas[apiKey];
        if (version < 0 || version > latestVersion(apiKey)) {
            throw new IllegalArgumentException("Invalid version for API key " + apiKey + ": " + version);
        }
        if (versions[version] == null) {
            throw new IllegalArgumentException("Unsupported version for API key " + apiKey + ": " + version);
        }
        return versions[version];
    }

    public static short latestVersion(int apiKey) {
        if (apiKey < 0 || apiKey >= Protocol.CURR_VERSION.length) {
            throw new IllegalArgumentException("Invalid api key: " + apiKey);
        }
        return Protocol.CURR_VERSION[apiKey];
    }

    public static Schema requestSchema(int apiKey, int version) {
        return schemaFor(Protocol.REQUESTS, apiKey, version);
    }

    public static Schema currentRequestSchema(int apiKey) {
        return requestSchema(apiKey, latestVersion(apiKey));
    }

    public static Schema responseSchema(int apiKey, int version) {
        return schemaFor(Protocol.RESPONSES, apiKey, version);
    }

    public static Schema currentResponseSchema(int apiKey) {
        return schemaFor(Protocol.RESPONSES, apiKey, latestVersion(apiKey));
    }

    public static Struct parseRequest(int apiKey, int version, ByteBuffer buffer) {
        return requestSchema(apiKey, version).read(buffer);
    }

    public static Struct parseResponse(int apiKey, ByteBuffer buffer) {
        return currentResponseSchema(apiKey).read(buffer);
    }

    public static Struct parseResponse(int apiKey, int version, ByteBuffer buffer) {
        return responseSchema(apiKey, version).read(buffer);
    }
}

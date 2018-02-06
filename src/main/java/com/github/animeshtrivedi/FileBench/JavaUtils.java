package com.github.animeshtrivedi.FileBench;

import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;

/**
 * Created by atr on 31.01.18.
 */
public class JavaUtils {

    static public int projection = 100;
    static public int selection = 100;
    static public boolean enableProjection = false;
    static public boolean enableSelection = false;

    static public MessageType makeProjectionSchema(MessageType originalSchema, int askProjection){
        Types.MessageTypeBuilder builder = Types.buildMessage();
        for (int i = 0; i < askProjection; i++) {
            builder.addFields(originalSchema.getType("int"+i));
        }
        return builder.named("fileBenchSchema");
    }
}

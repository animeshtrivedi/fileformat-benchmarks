package com.github.animeshtrivedi.FileBench;

import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;

/**
 * Created by atr on 25.01.18.
 */
public class DumpConverterX extends PrimitiveConverter {
    public DumpConverterX() {
    }

    final public GroupConverter asGroupConverter() {
        return new DumpGroupConverterX();
    }
}

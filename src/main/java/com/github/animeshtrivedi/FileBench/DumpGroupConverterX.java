package com.github.animeshtrivedi.FileBench;

import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;

/**
 * Created by atr on 25.01.18.
 */
public class DumpGroupConverterX extends GroupConverter {

    public DumpGroupConverterX(){

    }

    public Converter getConverter(int fieldIndex) {
        return new DumpConverterX();
    }

    public void start(){

    }

    public void end(){

    }
}

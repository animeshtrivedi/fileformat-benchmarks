package com.github.animeshtrivedi.FileBench;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by atr on 16.11.17.
 */
public class HdfsByteBufferReader {
    private FSDataInputStream istream = null;
    private FileStatus fileStatus = null;
    private ByteBuffer bb = null;
    private ByteBuffer tempBB = ByteBuffer.allocate(8);
    private boolean reachedEnd = false;

    public HdfsByteBufferReader(FSDataInputStream istream, FileStatus fileStatus){
        this.istream = istream;
        this.fileStatus = fileStatus;
        this.bb = ByteBuffer.allocateDirect(1024*1024);
        this.bb.flip();
    }

    private void readInBuffer() throws IOException {
        bb.clear();
        // we read it here
        if(-1 == this.istream.read(bb)){
            reachedEnd = true;
        } else {
            bb.flip();
        }
    }

    public int read(byte[] arr, int offset, int length) {
        if(bb.remaining() == 0)
            try {
                readInBuffer();
            } catch (IOException e) {
                e.printStackTrace();
            }
        if(reachedEnd)
            return -1;
        else {
            int toCopy = Math.min(length, bb.remaining());
            bb.get(arr, offset, toCopy);
            return  toCopy;
        }
    }

    private int readSlowInt() {
        tempBB.clear();
        tempBB.limit(Integer.BYTES);
        tempBB.put(bb);
        try {
            readInBuffer();
        } catch (IOException e) {
            e.printStackTrace();
        }
        if(reachedEnd)
            return -1;
        else {
            /* fresh read */
            int leftBytes = tempBB.remaining();
            for (int i = 0; i < leftBytes; i++){
                tempBB.put(bb.get());
            }
            tempBB.flip();
            return tempBB.getInt();
        }
    }

    public int readInt() {
        int retInt;
        if(bb.remaining() < Integer.BYTES) {
            retInt = readSlowInt();
        } else {
            retInt = bb.getInt();
        }
        return retInt;
    }

    public float readFloat() {
        return 0;
    }

    public double readDouble() {
        return 0;
    }

    public long capacity() {
        return this.fileStatus.getLen();
    }

    public String fileName() {
        return this.fileStatus.getPath().toString();
    }

    public void close() {
        try {
            this.istream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}


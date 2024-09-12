package org.e4s.service.parquet;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;

public class ParquetBufferReader implements InputFile {

    private final byte[] data;

    public ParquetBufferReader(byte[] data) {
        this.data = data;
    }

    @Override
    public long getLength() throws IOException {
        return this.data.length;
    }

    @Override
    public SeekableInputStream newStream() throws IOException {
        return new DelegatingSeekableInputStream(new SeekableByteArrayInputStream(this.data)) {
            @Override
            public void seek(long newPos) throws IOException {
                ((SeekableByteArrayInputStream) this.getStream()).setPos((int) newPos);
            }

            @Override
            public long getPos() throws IOException {
                return ((SeekableByteArrayInputStream) this.getStream()).getPos();
            }
        };
    }


    private static class SeekableByteArrayInputStream extends ByteArrayInputStream {

        public SeekableByteArrayInputStream(byte[] buf) {
            super(buf);
        }

        public void setPos(int pos) {
            this.pos = pos;
        }

        public int getPos() {
            return this.pos;
        }
    }
}

package org.e4s.service.parquet;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;

public class ParquetBufferWriter implements OutputFile {

    private final ByteArrayOutputStream out;

    public ParquetBufferWriter(ByteArrayOutputStream out) {
        this.out = out;
    }

    @Override
    public PositionOutputStream create(long blockSizeHint) throws IOException {
        return new ByteArrayPositionOutputStream(out);
    }

    @Override
    public PositionOutputStream createOrOverwrite(long blockSizeHint) throws IOException {
        return new ByteArrayPositionOutputStream(out);
    }

    @Override
    public boolean supportsBlockSize() {
        return false;
    }

    @Override
    public long defaultBlockSize() {
        return -1;
    }


    private class ByteArrayPositionOutputStream extends PositionOutputStream {

        private final ByteArrayOutputStream stream;

        private long pos = 0;

        public ByteArrayPositionOutputStream(ByteArrayOutputStream stream) throws IOException {
            this.stream = stream;
        }

        public long getPos() {
            return this.pos;
        }

        public void write(int data) throws IOException {
            pos++;
            stream.write(data);
        }

        @Override
        public void write(byte[] data) throws IOException {
            pos += data.length;
            stream.write(data);
        }

        @Override
        public void write(byte[] data, int off, int len) throws IOException {
            pos += len;
            stream.write(data, off, len);
        }

        @Override
        public void flush() throws IOException {
            stream.flush();
        }

        @Override
        public void close() throws IOException {
            try (OutputStream os = this.stream) {
                os.flush();
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
    }


}

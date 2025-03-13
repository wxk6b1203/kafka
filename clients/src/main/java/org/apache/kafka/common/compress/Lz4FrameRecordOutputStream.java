package org.apache.kafka.common.compress;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4FrameOutputStream;
import net.jpountz.xxhash.XXHashFactory;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.utils.ByteUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

public class Lz4FrameRecordOutputStream extends OutputStream {
    private LZ4FrameOutputStream out;
    private byte[] buffer;
    private final ByteBuffer sizeBuffer;
    private boolean finished;

    private int remains;

    public Lz4FrameRecordOutputStream(OutputStream out) throws IOException {
        this(out, Lz4BlockOutputStream.BLOCKSIZE_64KB, CompressionType.LZ4.defaultLevel());
    }

    /**
     * Create a new LZ4 output stream which split lz4 frame by single record.
     * @param out the output stream to write the compressed data to
     * @param blockSize the block size to use, between 4 and 7
     * @param level the compression level to use, between 1 and 17
     * @throws IOException if the output stream could not be created
     */
    public Lz4FrameRecordOutputStream(OutputStream out, int blockSize, int level) throws IOException {
        LZ4Compressor compressor = level == CompressionType.LZ4.defaultLevel() ? LZ4Factory.fastestInstance().fastCompressor() : LZ4Factory.fastestInstance().highCompressor(level);
        if (blockSize < 4 || blockSize > 7) {
            throw new RuntimeException("Block size value must be between 4 and 7");
        }
        this.out = new LZ4FrameOutputStream(out,
                LZ4FrameOutputStream.BLOCKSIZE.valueOf(blockSize),
                -1,
                compressor,
                XXHashFactory.fastestInstance().hash32(),
                LZ4FrameOutputStream.FLG.Bits.BLOCK_INDEPENDENCE);
        sizeBuffer = ByteBuffer.allocate(5);
        remains = 0;
    }


    @Override
    public void write(int b) throws IOException {
        write(new byte[]{(byte) b}, 0, 1);
    }

    private void writeBlock(byte[] buf) {

    }

    // TODO: check if this is the correct implementation
    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        ensureNotFinished();
        if (b == null || len == 0) {
            return;
        }

        int pos = off;
        int remaining = len;
        while (remaining > 0) {
            int next = writeRecord(b, pos, remaining);
            if (next == pos) {
                break;
            }
            int processed = next - pos;
            remaining -= processed;
            pos = next;
        }
    }

    private int writeRecord(byte[] b, int off, int len) throws IOException {
        if (b == null || len == 0) {
            return off;
        }

        int pos = off;
        if (remains <= 0) {
            // Read the varint size prefix
            while (pos < off + len && sizeBuffer.position() < 5) {
                byte currentByte = b[pos++];
                sizeBuffer.put(currentByte);
                // If this is the last byte of the varint (has MSB unset)
                if (currentByte >= 0) {
                    break;
                }
            }

            // If we haven't collected a complete varint yet, wait for more data
            if (sizeBuffer.position() > 0 && sizeBuffer.get(sizeBuffer.position() - 1) < 0) {
                return pos;
            }

            // Parse the varint to get the record size
            sizeBuffer.flip();
            int recordSize = ByteUtils.readVarint(sizeBuffer);
            int varintSize = sizeBuffer.position();

            // Allocate buffer for the complete record (varint + data)
            buffer = new byte[recordSize + varintSize];

            // Copy the varint bytes to the beginning of our buffer
            System.arraycopy(sizeBuffer.array(), 0, buffer, 0, varintSize);
            sizeBuffer.clear();

            // Set remains to the number of bytes we still need to read after the varint
            remains = recordSize;

            // Copy additional available data (if any) from the current chunk
            int availableDataSize = Math.min(remains, len - (pos - off));
            if (availableDataSize > 0) {
                System.arraycopy(b, pos, buffer, varintSize, availableDataSize);
                remains -= availableDataSize;
                pos += availableDataSize;
            }
        } else if (buffer != null) {
            // Continue filling the existing buffer
            int currentPosition = buffer.length - remains;
            int copySize = Math.min(remains, len);
            System.arraycopy(b, pos, buffer, currentPosition, copySize);
            remains -= copySize;
            pos += copySize;
        }

        // Write the complete record when we have it all
        if (remains == 0 && buffer != null) {
            out.write(buffer);
            buffer = null;
        }
        return pos;
    }

    @Override
    public void flush() throws IOException {
        super.flush();
    }

    @Override
    public void close() throws IOException {
        try {
            if (!finished) {
                finished = true;
            }
        } finally {
            try {
                if (out != null) {
                    try (OutputStream outStream = out) {
                        outStream.flush();
                    }
                }
            } finally {
                out = null;
                buffer = null;
                finished = true;
            }
        }
    }

    private void ensureNotFinished() {
        if (finished) {
            throw new IllegalStateException(Lz4BlockOutputStream.CLOSED_STREAM);
        }
    }
}

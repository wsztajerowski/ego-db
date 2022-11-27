/*
 * Copyright © 2022 Symentis.pl (jaroslaw.palka@symentis.pl)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package egodb.fs;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;

/**
 * Block oriented file implementation. You can only read and write in blocks (a constant size). There is however a special block, the first one, aka header
 * block, that has some metadata. The size of header is 512-bytes, with padding to a size of block, so the smallest block size can be 512-bytes.
 */
public class BlockFile implements AutoCloseable, Iterable<ByteBuffer> {
    private static final String MAGIC_BYTES = "EGO";
    private static final int VERSION = 1;
    private static final int HEADER_SIZE = 512;
    private final FileChannel fileChannel;
    private final Header header;

    public BlockFile(FileChannel fileChannel, Header header) {

        this.fileChannel = fileChannel;
        this.header = header;
    }

    public static BlockFile create(Path path, int blockSize, long numberOfBlocks) throws IOException {
        if (blockSize <= 0 || numberOfBlocks <= 0) {
            throw new IllegalArgumentException();
        }

        try (var randomAccessFile = new RandomAccessFile(requireNonNull(path.toFile()), "rw")) {
            randomAccessFile.setLength((numberOfBlocks + 1) * blockSize);
            // allocate first block for header
            try (var arrayOutputStream = new ByteArrayOutputStream(HEADER_SIZE)) {
                try (DataOutputStream outputStream = new DataOutputStream(arrayOutputStream)) {
                    Header.write(blockSize, numberOfBlocks, outputStream);
                }
                var headerBytes = new byte[blockSize];
                var bytes = arrayOutputStream.toByteArray();
                System.arraycopy(bytes, 0, headerBytes, 0, bytes.length);
                randomAccessFile.write(headerBytes);
            }
        }
        return open(path);
    }

    public static BlockFile open(Path path) throws IOException {
        var fileChannel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
        var header = Header.read(fileChannel);
        return new BlockFile(fileChannel, header);
    }

    public BlockFile.Header header() {
        return header;
    }

    public void write(ByteBuffer buffer, long block) throws IOException {
        ensureBufferRemainingBytes(buffer);
        var bytes = fileChannel.write(buffer, (block + 1) * header().blockSize());
        ensureBytesTransferred(bytes);
    }

    public void read(ByteBuffer buffer, long block) throws IOException {
        ensureBufferRemainingBytes(buffer);
        var bytes = fileChannel.read(buffer, (block + 1) * header.blockSize());
        // make sure we read enough bytes from channel
        ensureBytesTransferred(bytes);
    }

    private void ensureBytesTransferred(int bytes) {
        if (bytes != header().blockSize()) {
            throw new IllegalStateException(
                    format("transferred only %d bytes, expected block size is %d", bytes, header().blockSize()));
        }
    }

    private void ensureBufferRemainingBytes(ByteBuffer buffer) {
        if (buffer.remaining() < header.blockSize()) {
            throw new IllegalArgumentException(format(
                    "buffer doesn't have enough bytes (%d), remaining %d bytes",
                    header().blockSize(), buffer.remaining()));
        }
    }

    @Override
    public Iterator<ByteBuffer> iterator() {
        return null;
    }

    @Override
    public void close() throws Exception {
        fileChannel.close();
    }

    public record Header(int version, int blockSize, long numberOfBlocks, Flag... flags) {

        private static void write(int blockSize, long numberOfBlocks, DataOutputStream outputStream)
                throws IOException {
            // magic bytes
            outputStream.writeUTF(MAGIC_BYTES);
            // version
            outputStream.writeInt(VERSION);
            // blockSize
            outputStream.writeInt(blockSize);
            // number of blocks
            outputStream.writeLong(numberOfBlocks);
        }

        private static Header read(FileChannel fileChannel) throws IOException {
            var buffer = ByteBuffer.allocate(HEADER_SIZE);
            fileChannel.read(buffer, 0);
            buffer.flip();
            var array = buffer.array();
            try (var byteArrayInputStream = new ByteArrayInputStream(array)) {
                try (var inputStream = new DataInputStream(byteArrayInputStream)) {
                    var headerMagicBytes = inputStream.readUTF();
                    if (!MAGIC_BYTES.equals(headerMagicBytes)) {
                        throw new IllegalStateException("missing magic bytes " + headerMagicBytes);
                    }
                    var headerVersion = inputStream.readInt();
                    var headerBlockSize = inputStream.readInt();
                    var headerNumberOfBlocks = inputStream.readLong();
                    return new Header(headerVersion, headerBlockSize, headerNumberOfBlocks);
                }
            }
        }

        public enum Flag {
            CLOSED((byte) 0x01);

            private final byte value;

            Flag(byte value) {
                this.value = value;
            }
        }
    }
}

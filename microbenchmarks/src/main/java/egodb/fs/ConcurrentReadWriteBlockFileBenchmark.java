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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.ThreadParams;

/**
 * IMPLEMENT:
 *
 * The task is to implement asymmetric read write benchmark for {@code BlockFile}.
 *
 * REQUIREMENTS:
 * <uL>
 * <li> after warmup, whole block file should fit into OS page cache</li>
 * <li> it should be able to configure any combination of number of read and write threads</li>
 * <li> read threads should not compete with other read threads, it means not reading same block at the same time</li>
 * <li> write threads should not compete with other write threads</li>
 * <li> it is ok for read threads to compete with write threads for access to blocks</li>
 * </uL>
 */
@State(Scope.Benchmark)
public class ConcurrentReadWriteBlockFileBenchmark {
    private static final int BLOCK_SEQUENCE_SIZE = 256;

    @Param({"4096"})
    public static int blockSize;

    @Param({"SEQUENTIAL"})
    public String discAccess;

    @Param({"ON_HEAP"})
    public static String bufferAllocation;

    Path testFile;
    BlockFile blockFile;

    private long[] blockSequence;

    @Setup
    public void setup(BenchmarkParams params) throws IOException {
        int[] threadGroups = params.getThreadGroups();
        int maxNumberOfThreads = Math.max(threadGroups[0], threadGroups[1]);
        testFile = Files.createTempFile("block", ".dat");
        int numberOfBlocks = maxNumberOfThreads * BLOCK_SEQUENCE_SIZE;
        blockFile = BlockFile.create(testFile, blockSize, numberOfBlocks);

        blockSequence = BlockSequenceFactory.withSize(numberOfBlocks)
                .withDiskAccessStrategy(discAccess)
                .generateSequence();
    }

    @TearDown
    public void tearDown() throws Exception {
        blockFile.close();
        Files.deleteIfExists(testFile);
    }

    @State(Scope.Thread)
    public static class SingleThreadParams {
        private ByteBuffer buffer;
        private int operatingBlockNumber;
        private int executionCounter;

        @Setup
        public void setup(ThreadParams params) {
            buffer = ByteBufferFactory.withSize(blockSize)
                    .withAllocationStrategy(bufferAllocation)
                    .createBuffer();
            operatingBlockNumber = params.getSubgroupThreadIndex() * BLOCK_SEQUENCE_SIZE;
        }

        public int getBlockIndex() {
            return operatingBlockNumber + (executionCounter++) % BLOCK_SEQUENCE_SIZE;
        }

        public ByteBuffer getBuffer() {
            return buffer;
        }
    }

    @Benchmark
    @Group("blockFileGroup")
    @GroupThreads(4)
    public ByteBuffer readBenchmark(SingleThreadParams singleThreadParams) throws IOException {
        ByteBuffer buffer = singleThreadParams.getBuffer();
        buffer.rewind();
        long blockNumber = blockSequence[singleThreadParams.getBlockIndex()];
        blockFile.read(buffer, blockNumber);
        return buffer;
    }

    @Benchmark
    @Group("blockFileGroup")
    @GroupThreads(2)
    public ByteBuffer writeBenchmark(SingleThreadParams singleThreadParams) throws IOException {
        ByteBuffer buffer = singleThreadParams.getBuffer();
        buffer.rewind();
        long blockNumber = blockSequence[singleThreadParams.getBlockIndex()];
        blockFile.write(buffer, blockNumber);
        return buffer;
    }
}

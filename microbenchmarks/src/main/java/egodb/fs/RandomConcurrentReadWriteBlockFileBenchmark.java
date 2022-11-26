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
import java.util.ArrayList;
import java.util.stream.LongStream;
import org.apache.commons.lang3.ArrayUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.infra.ThreadParams;

@State(Scope.Benchmark)
public class RandomConcurrentReadWriteBlockFileBenchmark {

    public static final int BLOCK_SEQUENCE_SIZE = 256;
    private static final int BLOCK_SIZE = 4096;
    private BlockFile blockFile;
    private ArrayList<long[]> blocksSequences;

    @Setup(Level.Iteration)
    public void setUp(BenchmarkParams benchmarkParams) throws IOException {
        var threadGroup = benchmarkParams.getThreadGroups()[0];
        var numberOfBlocks = threadGroup * BLOCK_SEQUENCE_SIZE;

        var blocksSequence = LongStream.range(0, numberOfBlocks).toArray();

        ArrayUtils.shuffle(blocksSequence);

        // partition block sequence, for every reading thread
        blocksSequences = new ArrayList<>(threadGroup);
        for (int i = 0; i < threadGroup; i++) {
            var subarray = ArrayUtils.subarray(blocksSequence, i * BLOCK_SEQUENCE_SIZE, (i + 1) * BLOCK_SEQUENCE_SIZE);
            blocksSequences.add(i, subarray);
        }

        var file = Files.createTempFile("block", ".dat");

        blockFile = BlockFile.create(file, BLOCK_SIZE, numberOfBlocks);
    }

    @Benchmark
    @OperationsPerInvocation(BLOCK_SEQUENCE_SIZE)
    @Group("concurrent_block_read_write")
    public void read(ThreadParams threadParams, Blackhole bh, ThreadLocalByteBuffer localByteBuffer)
            throws IOException {
        // pick block sequence
        var groupThreadIndex = threadParams.getSubgroupThreadIndex();
        var blockSequence = blocksSequences.get(groupThreadIndex);
        // iterate over sequence of blocks
        for (int i = 0; i < blockSequence.length; i++) {
            var block = blockSequence[i];
            blockFile.read(localByteBuffer.byteBuffer, block);
            localByteBuffer.byteBuffer.rewind();
            bh.consume(localByteBuffer.byteBuffer);
        }
    }

    @Benchmark
    @OperationsPerInvocation(BLOCK_SEQUENCE_SIZE)
    @Group("concurrent_block_read_write")
    public void write(ThreadParams threadParams, Blackhole bh, ThreadLocalByteBuffer localByteBuffer)
            throws IOException {
        // pick block sequence
        var groupThreadIndex = threadParams.getSubgroupThreadIndex();
        var blockSequence = blocksSequences.get(groupThreadIndex);
        // iterate over sequence of blocks
        for (int i = 0; i < blockSequence.length; i++) {
            var block = blockSequence[i];
            localByteBuffer.byteBuffer.rewind();
            blockFile.write(localByteBuffer.byteBuffer, block);
            bh.consume(localByteBuffer.byteBuffer);
        }
    }

    @State(Scope.Thread)
    public static class ThreadLocalByteBuffer {
        private ByteBuffer byteBuffer;

        @Setup(Level.Iteration)
        public void setUp() {
            byteBuffer = ByteBuffer.allocate(BLOCK_SIZE);
        }
    }
}

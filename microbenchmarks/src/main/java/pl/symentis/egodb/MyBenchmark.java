/*
 * Copyright (c) 2014, Oracle America, Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *
 *  * Neither the name of Oracle nor the names of its contributors may be used
 *    to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 */

package pl.symentis.egodb;

import egodb.fs.BlockFile;
import org.apache.commons.lang3.ArrayUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Group;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.infra.ThreadParams;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.stream.LongStream;

@State( Scope.Benchmark )
public class MyBenchmark
{

    public static final int BLOCK_SEQUENCE_SIZE = 256;
    private BlockFile blockFile;
    private ArrayList<long[]> blocksSequences;

    @Setup( Level.Iteration )
    public void setUp( BenchmarkParams benchmarkParams ) throws IOException
    {
        System.out.println( benchmarkParams.getThreadGroupLabels() );
        var threadGroup = benchmarkParams.getThreadGroups()[0];
        var numberOfBlocks = threadGroup * BLOCK_SEQUENCE_SIZE;

        var blocksSequence = LongStream.range( 0, numberOfBlocks ).toArray();

        ArrayUtils.shuffle( blocksSequence );

        // partition block sequence, for every reading thread
        blocksSequences = new ArrayList<>( threadGroup );
        for ( int i = 0; i < threadGroup; i++ )
        {
            var subarray = ArrayUtils.subarray( blocksSequence,
                                                i * BLOCK_SEQUENCE_SIZE,
                                                (i + 1) * BLOCK_SEQUENCE_SIZE );
            blocksSequences.add( i, subarray );
        }

        blockFile = BlockFile.create( Paths.get( "file.dat" ), 4098, numberOfBlocks );
    }

    @Benchmark
    @OutputTimeUnit( TimeUnit.MILLISECONDS )
    @OperationsPerInvocation( BLOCK_SEQUENCE_SIZE )
    @Group( "concurrent_block_read_write" )
    public void read( ThreadParams threadParams, Blackhole bh, ThreadLocalByteBuffer localByteBuffer ) throws IOException
    {
        // pick block sequence
        var groupThreadIndex = threadParams.getSubgroupThreadIndex();
        var blockSequence = blocksSequences.get( groupThreadIndex );
        // iterate over sequence of blocks
        for ( int i = 0; i < blockSequence.length; i++ )
        {
            var block = blockSequence[i];
            blockFile.read( localByteBuffer.byteBuffer, block );
            localByteBuffer.byteBuffer.rewind();
            bh.consume( localByteBuffer.byteBuffer );
        }
    }

    @Benchmark
    @OutputTimeUnit( TimeUnit.MILLISECONDS )
    @OperationsPerInvocation( BLOCK_SEQUENCE_SIZE )
    @Group( "concurrent_block_read_write" )
    public void write( ThreadParams threadParams, Blackhole bh, ThreadLocalByteBuffer localByteBuffer ) throws IOException
    {
        // pick block sequence
        var groupThreadIndex = threadParams.getSubgroupThreadIndex();
        var blockSequence = blocksSequences.get( groupThreadIndex );
        // iterate over sequence of blocks
        for ( int i = 0; i < blockSequence.length; i++ )
        {
            var block = blockSequence[i];
            localByteBuffer.byteBuffer.rewind();
            blockFile.write( localByteBuffer.byteBuffer, block );
            bh.consume( localByteBuffer.byteBuffer );
        }
    }

    @State( Scope.Thread )
    public static class ThreadLocalByteBuffer
    {
        private ByteBuffer byteBuffer;

        @Setup( Level.Iteration )
        public void setUp()
        {
            byteBuffer = ByteBuffer.allocate( 4096 );
        }
    }
}

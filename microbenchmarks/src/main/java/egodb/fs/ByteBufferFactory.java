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

import java.nio.ByteBuffer;

public class ByteBufferFactory {
    private final int blockSize;
    private BufferAllocationStrategy bufferAllocationStrategy;

    public ByteBufferFactory(int blockSize) {
        this.blockSize = blockSize;
    }

    public static ByteBufferFactory withSize(int blockSize) {
        return new ByteBufferFactory(blockSize);
    }

    public ByteBufferFactory withAllocationStrategy(String bufferAllocation) {
        bufferAllocationStrategy = BufferAllocationStrategy.valueOf(bufferAllocation);
        return this;
    }

    public ByteBuffer createBuffer() {
        return switch (bufferAllocationStrategy) {
            case ON_HEAP -> ByteBuffer.allocate(blockSize);
            case OFF_HEAP -> ByteBuffer.allocateDirect(blockSize);
        };
    }
}

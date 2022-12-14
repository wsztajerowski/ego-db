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

import java.util.stream.LongStream;
import org.apache.commons.lang3.ArrayUtils;

public class BlockSequenceFactory {
    private final int numberOfBlocks;
    private AccessStrategy accessStrategy;

    public BlockSequenceFactory(int numberOfBlocks) {
        this.numberOfBlocks = numberOfBlocks;
    }

    public static BlockSequenceFactory withSize(int numberOfBlocks) {
        return new BlockSequenceFactory(numberOfBlocks);
    }

    public BlockSequenceFactory withStrategy(String accessStrategy) {
        this.accessStrategy = AccessStrategy.valueOf(accessStrategy);
        return this;
    }

    public long[] generateSequence() {
        return switch (accessStrategy) {
            case SEQUENTIAL -> LongStream.range(0, numberOfBlocks).toArray();
            case RANDOM -> {
                long[] array = LongStream.range(0, numberOfBlocks).toArray();
                ArrayUtils.shuffle(array);
                yield array;
            }
        };
    }
}

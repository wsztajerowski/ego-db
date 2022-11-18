package egodb.fs;

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
public class ConcurrentReadWriteBlockFileBenchmark
{
}

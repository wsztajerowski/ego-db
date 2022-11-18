package egodb.fs;

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
public class BlockFile implements AutoCloseable, Iterable<ByteBuffer>
{
    private static final String MAGIC_BYTES = "EGO";
    private static final int VERSION = 1;
    private static final int HEADER_SIZE = 512;
    private final FileChannel fileChannel;
    private final Header header;

    public BlockFile( FileChannel fileChannel, Header header )
    {

        this.fileChannel = fileChannel;
        this.header = header;
    }

    public static BlockFile create( Path path, int blockSize, long numberOfBlocks ) throws IOException
    {
        try ( var randomAccessFile = new RandomAccessFile( path.toFile(), "rw" ) )
        {
            randomAccessFile.setLength( (numberOfBlocks + 1) * blockSize );
            // allocate first block for header
            try ( var arrayOutputStream = new ByteArrayOutputStream( HEADER_SIZE ) )
            {
                try ( DataOutputStream outputStream = new DataOutputStream( arrayOutputStream ) )
                {
                    // magic bytes
                    outputStream.writeUTF( MAGIC_BYTES );
                    // version
                    outputStream.writeInt( VERSION );
                    // blockSize
                    outputStream.writeInt( blockSize );
                    // number of blocks
                    outputStream.writeLong( numberOfBlocks );
                }
                var headerBytes = new byte[blockSize];
                var bytes = arrayOutputStream.toByteArray();
                System.arraycopy( bytes, 0, headerBytes, 0, bytes.length );
                randomAccessFile.write( headerBytes );
            }
        }
        return open( path );
    }

    public static BlockFile open( Path path ) throws IOException
    {
        var fileChannel = FileChannel.open( path, StandardOpenOption.READ, StandardOpenOption.WRITE );
        var header = header( fileChannel );
        return new BlockFile( fileChannel, header );
    }

    private static Header header( FileChannel fileChannel ) throws IOException
    {
        var buffer = ByteBuffer.allocate( HEADER_SIZE );
        fileChannel.read( buffer, 0 );
        buffer.flip();
        var array = buffer.array();
        try ( var byteArrayInputStream = new ByteArrayInputStream( array ) )
        {
            try ( var inputStream = new DataInputStream( byteArrayInputStream ) )
            {
                var headerMagicBytes = inputStream.readUTF();
                if ( !MAGIC_BYTES.equals( headerMagicBytes ) )
                {
                    throw new IllegalStateException( "missing magic bytes " + headerMagicBytes );
                }
                var headerVersion = inputStream.readInt();
                var headerBlockSize = inputStream.readInt();
                var headerNumberOfBlocks = inputStream.readLong();
                return new Header( headerVersion, headerBlockSize, headerNumberOfBlocks );
            }
        }
    }

    public BlockFile.Header header()
    {
        return header;
    }

    public void write( ByteBuffer buffer, long block ) throws IOException
    {
        fileChannel.write( buffer, (block + 1) * header().blockSize() );
    }

    public void read( ByteBuffer buffer, long block ) throws IOException
    {
        fileChannel.read( buffer, (block + 1) * header.blockSize() );
    }

    @Override
    public Iterator<ByteBuffer> iterator()
    {
        return null;
    }

    @Override
    public void close() throws Exception
    {
        fileChannel.close();
    }

    public record Header(int version, int blockSize, long numberOfBlocks)
    {

    }
}

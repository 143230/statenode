import org.junit.Test;
import org.statenode.protocols.FileSystemProtocol;

/**
 * Created by root on 8/2/16.
 */
public class TestProto {
    @Test
    public void testProto(){
        FileSystemProtocol.Node.Builder builder = FileSystemProtocol.Node.newBuilder();
        FileSystemProtocol.IsDir isDir = FileSystemProtocol.IsDir.DIRECTORY;
        builder.setIsdir(isDir);
        builder.setPath("hdfs");
        FileSystemProtocol.Node file = builder.build();
        byte[] buf = file.toByteArray();
        System.out.println(new String(buf));
    }
}

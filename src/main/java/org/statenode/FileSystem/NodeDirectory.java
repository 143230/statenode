package org.statenode.FileSystem;

import org.hamcrest.core.Is;
import org.statenode.protocols.FileSystemProtocol;

/**
 * Created by root on 8/2/16.
 */
public class NodeDirectory implements NodePath {
    private String path;
    public NodeDirectory(String path){
        this.path = path;
    }
    @Override
    public boolean isDir() {
        return true;
    }

    @Override
    public String getPath() {
        return path;
    }

    @Override
    public byte[] toByteArray() {
        /*FileSystemProtocol.Node.Builder builder = FileSystemProtocol.Node.newBuilder();
        FileSystemProtocol.IsDir isDir = FileSystemProtocol.IsDir.DIRECTORY;
        builder.setIsdir(isDir);
        builder.setPath(path);
        FileSystemProtocol.Node file = builder.build();
        byte[] buf = file.toByteArray();*/

        FileSystemProtocol.NodeList.Builder builder = FileSystemProtocol.NodeList.newBuilder();
        FileSystemProtocol.Node.Builder nodeBuilder = FileSystemProtocol.Node.newBuilder();
        FileSystemProtocol.IsDir isDir = FileSystemProtocol.IsDir.DIRECTORY;
        nodeBuilder.setIsdir(isDir);
        nodeBuilder.setPath(path);
        FileSystemProtocol.Node file = nodeBuilder.build();
        builder.addPaths(file);
        FileSystemProtocol.NodeList list = builder.build();
        byte[] buf = list.toByteArray();
        return buf;
    }
}

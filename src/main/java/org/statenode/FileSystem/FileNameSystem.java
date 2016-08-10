package org.statenode.FileSystem;

import org.hamcrest.core.Is;
import org.rocksdb.*;
import org.rocksdb.util.SizeUnit;
import org.statenode.protocols.FileSystemProtocol;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by root on 8/2/16.
 */
public class FileNameSystem {
    private static final String db_path = "/tmp/rocks.db";
    private Options options;
    private RocksDB db;
    public FileNameSystem() throws RocksDBException {
        options = new Options();
        options.setCreateIfMissing(true)
                .createStatistics()
                .setWriteBufferSize(8 * SizeUnit.KB)
                .setMaxWriteBufferNumber(3)
                .setMaxBackgroundCompactions(10)
                .setCompressionType(CompressionType.SNAPPY_COMPRESSION)
                .setCompactionStyle(CompactionStyle.UNIVERSAL);
        db = RocksDB.open(options, db_path);
    }
    public boolean createFile(String src) throws Exception {
        return createPath(src,false);
    }
    private boolean createPath(String src,boolean isDir) throws Exception{
        if(src==null || !src.startsWith("/")){
            throw new Exception("src must be not null and start with '/'.");
        }
        src = resolvePath(src);
        String parentDir = src.substring(0,src.lastIndexOf("/")+1);
        StringBuffer buf = new StringBuffer();
        if(!db.keyMayExist(parentDir.getBytes(),buf)) {
            throw new IOException("Parent Directory \""+parentDir+"\" not exist.");
        }
        if(db.keyMayExist(src.getBytes(),buf)){
            throw new FileAlreadyExistsException("\""+src+"\"");
        }
        if(db.keyMayExist((src+"/").getBytes(),buf)){
            throw new FileAlreadyExistsException("\""+src+"/\"");
        }
        if(isDir){
            src = src + "/";
        }
        byte[] valdata = db.get(parentDir.getBytes());
        FileSystemProtocol.NodeList list = FileSystemProtocol.NodeList.parseFrom(valdata);
        FileSystemProtocol.NodeList.Builder listBuilder = list.toBuilder();
        FileSystemProtocol.Node.Builder nodeBuilder = FileSystemProtocol.Node.newBuilder();
        nodeBuilder.setIsdir(FileSystemProtocol.IsDir.DIRECTORY);
        nodeBuilder.setPath(src);
        listBuilder.addPaths(nodeBuilder.build());
        FileSystemProtocol.NodeList nodeList = listBuilder.build();
        WriteBatch batch = new WriteBatch();
        WriteOptions options = new WriteOptions();
        batch.remove(parentDir.getBytes());
        batch.put(parentDir.getBytes(),nodeList.toByteArray());
        NodePath path = new NodeDirectory(src);
        batch.put(src.getBytes(),path.toByteArray());
        db.write(options,batch);
        System.out.println(String.format("Create Directory \"%s\" Success.",src));
        return true;
    }
    private String resolvePath(String src) throws Exception {
        if(src==null || !src.startsWith("/")){
            throw new Exception("src must be not null and start with '/'.");
        }
        int i=src.length()-1;
        while(i>=0 && src.charAt(i)=='/')i--;
        return src.substring(0,i+1);
    }
    public boolean createDirectory(String src) throws Exception {
        return createPath(src,true);
    }
    public boolean deletePath(String src,boolean isDir) throws Exception {
        if(src==null || !src.startsWith("/")){
            throw new Exception("src must be not null and start with '/'.");
        }
        src = resolvePath(src);
        String parentDir = src.substring(0,src.lastIndexOf("/")+1);
        StringBuffer buf = new StringBuffer();
        if(!db.keyMayExist(parentDir.getBytes(),buf)) {
            throw new IOException("Parent Directory \""+parentDir+"\" not exist.");
        }
        if(isDir){
            src = src+"/";
        }
        if(!db.keyMayExist(src.getBytes(),buf)) {
            throw new IOException("File \""+src+"\" not exist.");
        }
        byte[] valdata = db.get(parentDir.getBytes());
        FileSystemProtocol.NodeList list = FileSystemProtocol.NodeList.parseFrom(valdata);
        FileSystemProtocol.NodeList.Builder listBuilder = list.toBuilder();
        FileSystemProtocol.NodeList.Builder newListBuilder = FileSystemProtocol.NodeList.newBuilder();
        List<FileSystemProtocol.Node> lists = listBuilder.getPathsList();
        for(int i=0;i<lists.size();i++){
            FileSystemProtocol.Node node = lists.get(i);
            if(!node.getPath().equals(src)){
                newListBuilder.addPaths(node);
            }
        }

        FileSystemProtocol.NodeList nodeList = newListBuilder.build();
        WriteBatch batch = new WriteBatch();
        WriteOptions options = new WriteOptions();
        batch.remove(parentDir.getBytes());
        batch.put(parentDir.getBytes(),nodeList.toByteArray());
        if(isDir){
            RocksIterator iterator = db.newIterator();
            for(iterator.seek(src.getBytes());iterator.isValid();iterator.next()){
                String key = new String(iterator.key());
                if(key!=null && key.startsWith(src)){
                    batch.remove(iterator.key());
                }else break;
            }
        }
        db.write(options,batch);
        db.remove(src.getBytes());
        return true;
    }
    public boolean deleteFile(String src) throws Exception{
        return deletePath(src,false);
    }
    public boolean deleteDirectory(String src) throws Exception {
        return deletePath(src,true);
    }
    public boolean modifyPath(String src,String dst,boolean isDir) throws Exception{
        if(src==null || !src.startsWith("/")){
            throw new Exception("src must be not null and start with '/'.");
        }
        src = resolvePath(src);
        dst = resolvePath(dst);

        String parentDir = src.substring(0,src.lastIndexOf("/")+1);
        if(isDir){
            src = src+"/";
            dst = dst+"/";
        }
        StringBuffer buf = new StringBuffer();
        if(!db.keyMayExist(parentDir.getBytes(),buf)) {
            throw new IOException("Parent Directory \""+parentDir+"\" not exist.");
        }
        if(!db.keyMayExist(src.getBytes(),buf)) {
            throw new IOException("Path \""+src+"\" not exist.");
        }
        if(db.keyMayExist(dst.getBytes(),buf)) {
            throw new IOException("Path \""+dst+"\" already exist.");
        }

        byte[] valdata = db.get(parentDir.getBytes());
        FileSystemProtocol.NodeList list = FileSystemProtocol.NodeList.parseFrom(valdata);
        FileSystemProtocol.NodeList.Builder listBuilder = list.toBuilder();
        FileSystemProtocol.NodeList.Builder newListBuilder = FileSystemProtocol.NodeList.newBuilder();
        for(int i=0;i<listBuilder.getPathsCount();i++){
            FileSystemProtocol.Node.Builder nodeBuilder = listBuilder.getPaths(i).toBuilder();
            if(nodeBuilder.getPath().equals(src)){
                nodeBuilder.setPath(dst);
                newListBuilder.addPaths(nodeBuilder.build());
            }else{
                newListBuilder.addPaths(nodeBuilder.build());
            }
        }
        FileSystemProtocol.NodeList nodeList = newListBuilder.build();
        WriteBatch batch = new WriteBatch();
        WriteOptions options = new WriteOptions();
        batch.remove(parentDir.getBytes());
        batch.put(parentDir.getBytes(),nodeList.toByteArray());
        if(isDir){
            RocksIterator iterator = db.newIterator();
            for(iterator.seek(src.getBytes());iterator.isValid();iterator.next()){
                String key = new String(iterator.key());
                if(key!=null && key.startsWith(src)){
                    key = key.replaceFirst(src,dst);
                    byte[] val = iterator.value();
                    batch.remove(iterator.key());
                    batch.put(key.getBytes(),val);
                }else break;
            }
        }
        db.write(options,batch);
        return true;
    }
    public boolean modifyFile(String src,String dst) throws Exception {
        return modifyPath(src,dst,false);
    }
    public boolean modifyDirectory(String src,String dst) throws Exception {
        return modifyPath(src,dst,true);
    }


    public NodeFile searchFile(String src) throws Exception {
        if(src==null || !src.startsWith("/")){
            throw new Exception("src must be not null and start with '/'.");
        }
        src = resolvePath(src);
        StringBuffer buf = new StringBuffer();
        if(!db.keyMayExist(src.getBytes(),buf)) {
            throw new IOException("File \""+src+"\" not exist.");
        }
        byte[] value = db.get(src.getBytes());
        FileSystemProtocol.Node file = FileSystemProtocol.Node.parseFrom(value);
        NodeFile nodefile = new NodeFile(file.getPath());
        return nodefile;
    }
    public List<NodePath> searchDirectory(String src) throws Exception {
        if(src==null || !src.startsWith("/")){
            throw new Exception("src must be not null and start with '/'.");
        }
        src = resolvePath(src)+"/";
        StringBuffer buf = new StringBuffer();
        if(!db.keyMayExist(src.getBytes(),buf)) {
            throw new IOException("Directory \""+src+"\" not exist.");
        }
        FileSystemProtocol.NodeList nodeList = FileSystemProtocol.NodeList.parseFrom(db.get(src.getBytes()));
        List<NodePath> nodes = new ArrayList<>();
        for(int i=0;i<nodeList.getPathsCount();i++){
            FileSystemProtocol.Node node = nodeList.getPaths(i);
            NodePath path;
            if(node.getIsdir() == FileSystemProtocol.IsDir.NORMAL_FILE){
                path= new NodeFile(node.getPath());
            }else{
                path= new NodeDirectory(node.getPath());
            }
            nodes.add(path);
        }
        return nodes;
    }
    public void format() throws IOException, RocksDBException {
        RocksIterator iterator = db.newIterator();
        for(iterator.seekToFirst();iterator.isValid();iterator.next()){
            db.remove(iterator.key());
        }
        NodePath root = new NodeDirectory("/");
        db.put("/".getBytes(),root.toByteArray());
    }
    public void printAll(){
        System.out.println("################File System##############################");
        RocksIterator iterator = db.newIterator();
        for(iterator.seekToFirst();iterator.isValid();iterator.next()){
            String key = new String(iterator.key());
            System.out.println(key);
        }
        System.out.println("########################END##############################");
    }
    public static void main(String[] args) throws Exception {
        FileNameSystem fs = new FileNameSystem();
        fs.format();
        fs.createDirectory("/a");
        fs.createFile("/a/b///");
        fs.createDirectory("/a/c");
        List<NodePath> nodes = fs.searchDirectory("/");
        for(NodePath node:nodes){
            System.out.println(node.getPath()+" isDir:"+node.isDir());
        }
        fs.modifyDirectory("/a","/b");
        fs.createDirectory("/b/d");
        fs.createDirectory("/c");
        
        fs.printAll();
    }
}

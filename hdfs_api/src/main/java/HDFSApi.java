import com.jcraft.jsch.IO;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;

/**
 * @author wjx
 * @date 2024/6/13 14:58
 */
public class HDFSApi {
    private FileSystem fs;

    @Before
    public void getFileSystem() throws IOException{
        System.setProperty("HADOOP_USER_NAME", "root");

        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://bigdata01:9000");
        fs = FileSystem.get(conf);
        System.out.println(fs.getClass().getName());
    }

    @After
    public void closeFileSystem() throws IOException{
        fs.close();
    }

    @Test
    public void uploadTest() throws IOException {
        // 需要操作的两个路径，本地源文件的路径和HDFS的路径
        Path src = new Path("D:\\Projects\\bigdata\\homework\\hdfs_api\\src\\main\\resources\\data\\region_cell\\region_cell.log");

        Path dst = new Path("/data");
        // 直接调用方法上传文件即可
        fs.copyFromLocalFile(src, dst);
    }

    /**
     * 文件下载
     */
    @Test
    public void downloadTest() throws IOException {
        // 需要设置两个路径，HDFS的文件路径和本地存储的文件路径
        Path src = new Path("/hadoop-3.3.1-aarch64.tar.gz");
        Path dst = new Path("/Users/shawn/Desktop/hadoop.tar.gz");
        // 直接调用方法下载文件即可
        fs.copyToLocalFile(src, dst);
    }

    /**
     * 创建文件夹
     */
    public void createDir(String dir) throws IOException {
        fs.mkdirs(new Path("/" + dir));
    }

    /**
     * 删除文件夹
     */
    @Test
    public void deleteDirTest() throws IOException {
        // fs.delete(new Path("/input"), true);
        fs.delete(new Path("/file"), true);
    }

    /**
     * 重命名
     */
    @Test
    public void renameTest() throws IOException {
        fs.rename(new Path("/hadoop-3.3.1-aarch64.tar.gz"), new Path("/hadoop.tar.gz"));
    }

    /**
     * 判断文件（夹）是否存在
     */
    @Test
    public void existsTest() throws IOException {
        // System.out.println(fs.exists(new Path("/test3")));

        // 需求：判断/test3是否存在，如果存在，则删除这个文件夹
        Path path = new Path("/test3");
        if (fs.exists(path)) {
            fs.delete(path, true);
        }
    }


    @Test
    public void ioUtilsTest() throws IOException {
        // 设置操作Hadoop的用户为root，杜绝出现权限不足的情况
        System.setProperty("HADOOP_USER_NAME", "root");
        // 实例化描述配置的对象，加载默认的配置文件
        Configuration configuration = new Configuration();
        // 设置分布式文件系统属性
        configuration.set("fs.defaultFS", "hdfs://qianfeng01:9820");
        // 获取DistributedFileSystem对象
        FileSystem fileSystem = FileSystem.get(configuration);

        // 读取本地文件，构建输入流
        // FileInputStream input = new FileInputStream("/Users/shawn/Desktop/HadoopAPI/src/main/java/com/qianfeng/hdfs/HDFSApi.java");
        // 连接HDFS文件，构建输出流
        // FSDataOutputStream output = fileSystem.create(new Path("/HDFSApi.java"));

        // 连接HDFS文件，构建输入流
        FSDataInputStream input = fileSystem.open(new Path("/HDFSApi.java"));
        // 连接本地文件，构建输出流
        FileOutputStream output = new FileOutputStream("/Users/shawn/Desktop/HDFSApi.java");

        // 读取输入流中的数据，写出到输出流，实现的拷贝的功能
        IOUtils.copyBytes(input, output, configuration);

        // 关闭两个流
        IOUtils.closeStream(input);
        IOUtils.closeStream(output);
    }

    /**
     * 文件状态信息查看
     */
    @Test
    public void listFileStatusTest() throws IOException {
        // 获取每一个文件的状态信息列表，迭代器对象
        RemoteIterator<LocatedFileStatus> iterator = fs.listLocatedStatus(new Path("/"));
        //
        while (iterator.hasNext()) {
            // 获取到当前遍历到的文件
            LocatedFileStatus fileStatus = iterator.next();
            System.out.println("基本信息: " + fileStatus);
            // 获取所有的块的信息
            BlockLocation[] blockLocations = fileStatus.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                System.out.println("当前块的所有副本信息: " + Arrays.toString(blockLocation.getHosts()));
                System.out.println("当前块大小: " + blockLocation.getLength());
                System.out.println("当前块的副本的IP地址: " + Arrays.toString(blockLocation.getNames()));
            }
            System.out.println("系统的块大小: " + fileStatus.getBlockSize());
            System.out.println("当前文件的总大小: " + fileStatus.getLen());
        }
    }
}

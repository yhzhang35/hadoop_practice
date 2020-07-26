import component.FlashMapper;
import component.FlashReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class WebFlash {
    private  static  final String  HDFS_URL="hdfs://hadoopmaster:9000";
    private static final String HADOOP_USR_NAME="root";

    public static void main(String[] args){
        //检查参数是否合法
        if(args.length<2){
            System.out.println("参数输入错误，请重新输入");
            return;
        }

        //设置环境变量
        Configuration configuration=new Configuration();
        System.setProperty("HADOOP_USR_NAME",HADOOP_USR_NAME);
        configuration.set("fs.defualtFS",HDFS_URL);

        Job job=null;
        //通过上面的配置的configuration来创建一个Job
        try {
            job=Job.getInstance(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
        //设置job执行的主类
        job.setJarByClass(WebFlash.class);

        //设置mapper和reducer及其输入类型
        job.setMapperClass(FlashMapper.class);
        job.setReducerClass(FlashReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //输出文件是否存在
        FileSystem fileSystem=null;
        try {
            fileSystem= FileSystem.get(new URI(HDFS_URL),configuration,HADOOP_USR_NAME);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        Path outputpath=new Path(args[1]);
        Path intputpath=new Path(args[0]);

        try {
            if(fileSystem.exists(outputpath)){
                fileSystem.delete(outputpath,true);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        //设置输入和输入文件名
        try {
            FileInputFormat.setInputPaths(job,intputpath);
        } catch (IOException e) {
            e.printStackTrace();
        }
        FileOutputFormat.setOutputPath(job,outputpath);




        //提交作业
        boolean result=false;
        try {
            result=job.waitForCompletion(true);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

        //决定是否返回
        System.exit(result?0:-1);
    }

}

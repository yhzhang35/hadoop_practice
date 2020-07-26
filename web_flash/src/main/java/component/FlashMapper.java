package component;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * function:用于log文件的map,输入是ip,date,pageurl
 *
 */

public class FlashMapper extends Mapper<LongWritable,Text,Text,Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line=value.toString();
        String ipResult= null;
        String timeResult=null;
        String pageResult=null;


        String regex="(\\d{1,3}\\.){3}\\d{1,3}";
        Pattern pattern=Pattern.compile(regex);
        Matcher matcher=pattern.matcher(line);
        //ip发现
        if (matcher.find()){
            ipResult=matcher.group();
            System.out.println("###########获取到的ip为##############:"+ipResult);
            String regexTime="\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}";
            Pattern pattern1=Pattern.compile(regexTime);
            Matcher matcher1=pattern1.matcher(line);
            //时间发现
            if (matcher1.find()){
                timeResult=matcher1.group();
            }

            String regexPage="http://.*]";
            Pattern pattern2=Pattern.compile(regexPage);
            Matcher matcher2=pattern2.matcher(line);
            //发现页面
            if(matcher2.find()){
                pageResult=matcher2.group(0);
                pageResult=pageResult.substring(0,pageResult.length()-1);
            }
            context.write(new Text(ipResult),new Text(timeResult+"\t"+pageResult));
        }




    }
}

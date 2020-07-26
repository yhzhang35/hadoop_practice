package component;

import com.sun.tools.javac.util.Log;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import static com.sun.tools.doclint.Entity.lt;

public class FlashReducer extends Reducer<Text,Text,Text,Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //定义格式
        DateFormat format=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); //注意大小写MM和HH需要区分，其他均为小写

        //定义时间
        Date startTime= null;
        Date lastTime=null;
        //时间的字符串格式
        String sTime=null;
        String lTime=null;

        try {
            startTime = format.parse("2030-12-12 12:12:12");//初始取最大
            lastTime=format.parse("1000-10-10 00:00:00");  //初始取最小
        } catch (ParseException e) {
            e.printStackTrace();
        }

        //定义次数和间隔时间
        Long count=0l;
        Long distance=0l;

        //定义网页url
        String startPage=null;
        String lastPage=null;


        String temp=null;
        for (Text text:values){
            //计数
            count=count+1;
            temp=text.toString();
            String as[]=temp.split("\t");
            //获取时间和页面
            String time=as[0];
            String page=as[1];

            //将时间转换为Long
            Date getTime=null;
            try {
                getTime=format.parse(time);
            } catch (ParseException e) {
                e.printStackTrace();
            }


            //判断是否是最早或最晚
            if (startTime.after(getTime)){
                startTime=getTime;
                startPage=page;
            }
            if(lastTime.before(getTime)){
                lastTime=getTime;
                lastPage=page;
            }



        }

        //就间隔时间(单位：分钟)
        Calendar startCalendar=Calendar.getInstance();
        Calendar lastCalendar=Calendar.getInstance();
        startCalendar.setTime(startTime);
        lastCalendar.setTime(lastTime);
        //利用calendar计算间隔
        distance=(lastTime.getTime()-startTime.getTime())/(1000);  //以秒为单位


        //将Long型转换为时间类型
        sTime=format.format(startTime);
        lTime=format.format(lastTime);


        //打印一下结果
        System.out.println(key+" "+sTime+" "+startPage+" "+
                lTime+" "+lastPage+" "+distance);
        //写入数据
        context.write(new Text(key),
        new Text(sTime+"\t"+startPage+"\t"+
                lTime+"\t"+lastPage+"\t"+distance+"\t"+count));


    }

//    @Override
//    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
//        for (Text value:values){
//            context.write(key,value);
//        }
//    }
}

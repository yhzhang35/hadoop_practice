package component;

import java.text.DateFormat;


import java.text.ParseException;
import java.text.SimpleDateFormat;

import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Test {
    String st1="[2016-11-29 00:59:24 INFO ] 2017 (cn.baidu.core.inteceptor.LogInteceptor:55) - [0	136.243.36.87	null	http://www.baidu.cn/word]";
    String st2="[2016-11-29 00:02:06 INFO ] (cn.baidu.global.job.PolyvJob:99) - 远程加载播放列表1444298050493对应的视频内容，获取10条记录";
    String st=st2;
    String st3="hello\tworld\tspark";
    String regexIp="(\\d{1,3}\\.){3}\\d{1,3}";
    String regexTime="(\\d{4})-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}";
    String regexPage="http://.*";

    Pattern timePatern=Pattern.compile(regexTime);
    Pattern ipPatern=Pattern.compile(regexIp);
    Pattern pagePatern=Pattern.compile(regexPage);

    Matcher matcherTime=timePatern.matcher(st);
    Matcher matcherIp=ipPatern.matcher(st);
    Matcher matcherPage=pagePatern.matcher(st);

    public static void main(String[] args) throws ParseException {
        Test test=new Test();
        String time=null;
        if(test.matcherIp.find()){
            System.out.println("找到的ip为："+test.matcherIp.group());

        }

        while(test.matcherTime.find()){
            time=test.matcherTime.group(1);
            System.out.println("找到的time为："+time);

        }

//        if(test.matcherPage.find()){
//            String pageResult=test.matcherPage.group();
//            System.out.println("找到的page为："+pageResult.substring(0,pageResult.length()-1));
//
//        }
//
//        DateFormat dateFormat=new SimpleDateFormat("YYYY-MM-DD HH:mm:ss");
//        Date date;
//        date=dateFormat.parse(time);
//        System.out.println("time转为long："+date.getTime());
//        Date date1=new Date(date.getTime());
//        System.out.println("long转为time："+dateFormat.format(date1));
//        String[] as=test.st3.split("\t-");
//        for (int i = 0; i < as.length; i++) {
//            System.out.println(as[i]);
//        }

    }


}

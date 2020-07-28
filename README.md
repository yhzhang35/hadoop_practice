# hadoop 实战练习（二）



>  引言： 哈哈，时隔几日，坏蛋哥又回来了，继上一篇hadoop实战练习（一）,坏蛋哥准备继续写一个实战练习实例。苏格拉底曾说：所有科学都源于需求。那么我们就抛出今天实战项目的需求：百度采集了一段时间用户的访问日志。需要将数据进行清洗变成结构化的数据，方便后面模型或报表的制作。那么就让我们开始吧！



码字不易，如果大家想持续获得大数据相关内容，请关注和点赞坏蛋哥(haha......)

![image](https://mmbiz.qpic.cn/mmbiz_jpg/OttTy9k3GWh3kHib4JQjpzSeDNyqqI4ryroRwpNJAKh5XUHcXvheibiaW2LdF5rdurw2wZdz6E6gZ1ul9Z3Trr61w/640?wx_fmt=jpeg&tp=webp&wxfrom=5&wx_lazy=1&wx_co=1)



文章目录：**

[TOC]

## 一  项目需求分析

百度采集了用户点击访问的日志（后台回复【baidu】可获得实验数据哦！），现在需要分析日志数据。进行一个轻量级的数据汇总，数据形式如下图所示：

![image-20200727224453450](F:\Typora_note\公众号\hadoop实战练习（二）.assets\image-20200727224453450.png)

![image-20200727224734986](F:\Typora_note\公众号\hadoop实战练习（二）.assets\image-20200727224734986.png)



如图所示，在日志文件中有后台数据的埋点日志（也就类似于System.out.println(“某某怎们样了，我记录一下”)）和下面我圈出来的，用户访问url所打印的信息。现在要就将统计同一个ip地址访问了几次页面，并统计最开始访问和最后访问的页面是什么。最后的结果类似于：

![image-20200727225509489](F:\Typora_note\公众号\hadoop实战练习（二）.assets\image-20200727225509489.png)



## 二 项目实现思路

如果你已经有思路了或者想要尝试一下自己来完成这个小项目，那么就请暂时退出网页，试着自己独立完成，如果中途有什么不懂的，可以上网查取资料。完成后再来看我的思路。如果你对hadoop还不是很熟悉，那么可以先看下我的思路，如果理解了，那么就请自己一个人来独立复现代码哦（相信坏蛋哥这么做是为你好，什么东西都是当你能随心所欲的用于起来了，那么就代表你学会了）。

这个项目主要要写map和reduce函数，map函数主要要实现数据的清洗和提取，这儿主要涉及到的是正则表达式的知识。map将数据变成以ip为键，time和page为value。reduce函数实现对数据的统计，算出timecount和pagecount并比较得出startpage和lastpage。



## 三 具体实现代码讲解



### 3.1 map函数代码的具体讲解：

```java
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
            //将清洗的结果传给下一级
            context.write(new Text(ipResult),new Text(timeResult+"\t"+pageResult));
        }


    }
}

```





###  3.2 reduce函数的具体讲解：

```java
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

            //将String转化为Date类
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


        //将时间格式化
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

}
```





## 四 总结

上面的全部代码可以在后台回复【hadoop练习1】获取github链接，如果能帮到你，希望给坏蛋哥点赞和收藏哦，你的肯定才是坏蛋哥把这个公众号做好的动力，后面我会讲解如何用spark来清晰日志，hive+tez建立电商数仓，flume+kafka的数据收集等相关实战和深入理论。码字不容易，欢迎关注坏蛋哥哦！





> 参考文献：
>
> * Hadoop documention







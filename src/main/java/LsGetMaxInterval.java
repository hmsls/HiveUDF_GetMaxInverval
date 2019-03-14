import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@Description(name="GetMaxInterval",value="start time，eg: 2019-03-13，period is the time that how long to start over，" +
        "overdueNum is the num overduenum.\n",extended="Example:\n"
        +"> select _FUNC_(beginDate,endDate,period,overdueNum) from src;\n")

public class LsGetMaxInterval extends UDF {
    private SimpleDateFormat sdf;
    private Date periodEndDate1; //周期结束时间，比如第一个周期时间是从beginDate到beginDate + period（一个周期的时间）
    private Date periodStartDate1;
    int max = 0; //统计一个周期中的最大值
    long ds = 0; //计算时间区间中有多少个指定的周期

    public LsGetMaxInterval(){
        sdf = new SimpleDateFormat("yyyy-MM-dd");
    }

    public String evaluate(String beginDate,String endDate,int period,String datetm,int overdueNum){
        try {
            Date begin = sdf.parse(beginDate);
            Date end = sdf.parse(endDate);
            Date date = sdf.parse(datetm);
            //计算区间相差天数，根据时间的毫秒差值计算
            long days = (end.getTime()-begin.getTime())/(1000*3600*24);
            //计算输入日期到开始时间的的天数
            long days1 = (date.getTime()-begin.getTime())/(1000*3600*24);
            //开始时间和传入的日期一直的情况
            if(days1%period == 0 && days1/period == 0){
                max = overdueNum;
                return "-";
            }else if(days1%period != 0 && days1/period == 0){ //在第一个周期内的情况
                //从开始时间到第一个周期，开始和结束时间
                periodStartDate1 = begin;
                periodEndDate1 = new Date(begin.getTime() + 1 * period * (1000*3600*24));
                if((date.getTime()<=periodEndDate1.getTime()) && (date.getTime()>=periodStartDate1.getTime())){
                    if(overdueNum>max){
                        max = overdueNum;
                    }
                }
                if(datetm.equals(sdf.format(new Date(periodEndDate1.getTime() - (1000*3600*24))))){
                    return "在"+sdf.format(periodStartDate1)+"和"+sdf.format(periodEndDate1)+"之间，逾期天数最大为："+max;
                }else{
                    return "-";
                }
            }else if(days1/period >= 1){ //在第二个及以后周期的情况
                //这是从大于一个周期开始，每个统计周期的开始和结束时间
                ds = days1/period;
                if(days1%period == 0){
                    max = overdueNum;
                }
                periodStartDate1 = new Date(begin.getTime() + ds * period * (1000*3600*24));
                periodEndDate1 = new Date(begin.getTime() + (ds+1) * (period) * (1000*3600*24));
                //程序需改进地方，判断这个截止日期是不是在指定日期范围内，如果是，就如下，如果超出了，则日期为指定日期
                if(periodEndDate1.getTime()>=end.getTime()){
                    periodEndDate1 = end;
                }
                if((date.getTime()<=periodEndDate1.getTime()) && (date.getTime()>=periodStartDate1.getTime())){
                    if(overdueNum>max){
                        max = overdueNum;
                    }
                }
                //在周期前一天、时间在开始结束区间内、周期之间大于1天打印，最后一天也打印
                if(datetm.equals(sdf.format(new Date(periodEndDate1.getTime() - (1000*3600*24))))
                        && date.getTime() <= end.getTime() && date.getTime() >= begin.getTime()
                        && (periodEndDate1.getTime() - periodStartDate1.getTime()) > (1000*3600*24)
                        || datetm.equals(endDate) ){
                    return "在"+sdf.format(periodStartDate1)+"和"+sdf.format(periodEndDate1)+"之间，逾期天数最大为："+max;
                }else{
                    return "-";
                }
            }

        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void main(String[] args){
        LsGetMaxInterval lsGetMaxInterval = new LsGetMaxInterval();
        String res = lsGetMaxInterval.evaluate("2019-02-01","2019-02-11",5,"2019-02-11",1);
        System.out.println(res);
    }
}

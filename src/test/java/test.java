import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class test {
    public static void main(String[]  args){
        test t = new test();
        t.evaluate("2019-02-11","2019-02-01",10,1);
    }
    public String evaluate(String beginDate,String endDate,int period,int overdueNum){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        try {
            long ds;
            Date begin = sdf.parse(beginDate);
            Date end = sdf.parse(endDate);
            //计算相差天数，根据时间的毫秒差值计算
            long days = (begin.getTime()-end.getTime())/(1000*3600*24);
            System.out.println("days/period===="+days/period);
            System.out.println("days%period===="+days%period);
            if(days%period == 0){
                ds = days/period;
            }else{
                ds = days/period + 1;
            }
            System.out.println(ds);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }
}

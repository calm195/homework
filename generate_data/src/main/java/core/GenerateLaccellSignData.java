package core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Random;

/**
 * 模拟产生基站信令数据
 * 数据格式：d92d8c88-d351-467b-8caf-e72178f02447|101_00002|39.958087|116.296185|1670468511000
 * Created by xuwei
 */
public class GenerateLaccellSignData {
    private static Logger logger = LoggerFactory.getLogger(GenerateLaccellSignData.class);

    public static List<String> getDataFromFile(String filePath) throws IOException {
        ArrayList<String> arr = new ArrayList<String>();


        try (BufferedReader br1 = new BufferedReader(new InputStreamReader(Objects.requireNonNull(GenerateLaccellSignData.class.getClassLoader().getResourceAsStream(filePath))))) {
            String line1;
            while ((line1 = br1.readLine()) != null) {
                String[] splits = line1.split("\\|");
                String imsi = splits[0];
                arr.add(imsi);
            }

        }
        return arr;

    }

    public static void main(String[] args) throws Exception {
        //读取用户基础数据中的imsi

        String fileName1 = "user_info/user_info.log";
        List<String> arr1 = getDataFromFile(fileName1);
//        InputStream in1 = GenerateLaccellSignData.class.getClassLoader().getResourceAsStream(fileName1);
//        BufferedReader br1 = new BufferedReader(new InputStreamReader(in1));
//        String line1;
//        while ((line1 = br1.readLine()) != null) {
//            String[] splits = line1.split("\\|");
//            String imsi = splits[0];
//            arr1.add(imsi);
//        }
        //读取行政区-基站关系数据中的laccell

        String fileName2 = "region_cell/region_cell.log";
        List<String> arr2 = getDataFromFile(fileName2);

        Random r = new Random();
        while (true) {
            //随机获取imsi
            String r_imsi = arr1.get(r.nextInt(arr1.size()));
            //随机获取laccell
            String laccell = arr2.get(r.nextInt(arr2.size()));

            //随机获取基站纬度
            double latitide = r.nextDouble() + r.nextInt(100);
            String latitide_str = new String(latitide + "").substring(0, 10);
            //随机获取基站经度
            double longitude = r.nextDouble() + r.nextInt(100);
            String longitude_str = new String(longitude + "").substring(0, 10);

            //获取当前时间戳
            long currTime = System.currentTimeMillis();

            //组装数据
            String line = r_imsi + "|" + laccell + "|" + latitide_str + "|" + longitude_str + "|" + currTime;

            //记录实时产生的基站信令数据
            logger.info(line);

            int sleeptime = 1000;//默认每隔1秒产生一条数据
            if (args.length == 1) {
                sleeptime = Integer.parseInt(args[0]);
            }
            Thread.sleep(sleeptime);
        }
    }
}

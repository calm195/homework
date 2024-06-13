package core.controller;

import com.alibaba.fastjson.JSONObject;
import core.bean.Status;
import core.utils.HBaseUtil;
import core.utils.MyDbUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.lang.reflect.Array;
import java.util.*;

/**
 * 数据接口V1.0
 * Created by xuwei
 */
@RestController//控制器类
@RequestMapping("/v1")//映射路径
public class DataController {
    private static final Logger logger = LoggerFactory.getLogger(DataController.class);
    /**
     * 测试接口
     * @param name
     * @return
     */
    @RequestMapping(value="/t1",method = RequestMethod.GET)
    public Status test(@RequestParam("name") String name) {
        Status status = new Status();
        System.out.println(name);
        return status;
    }
    /**
     * 查询指定区域、指定时间内停留的某类画像的用户总数
     * @param region_id 区域
     * @param produce_hour 时间
     * @param portrait_id 画像id
     * @return 用户总数
     */
    @RequestMapping(value="/uc",method = RequestMethod.GET)
    public JSONObject queryUserCountByRegionAndTime(@RequestParam("region_id") String region_id,@RequestParam("produce_hour") String produce_hour,@RequestParam("portrait_id") int portrait_id) {
        String sql = "WITH (SELECT IMSI_INDEXES FROM REGION_ID_IMSI_BITMAP WHERE REGION_ID = '"+region_id+"' AND PRODUCE_HOUR = '"+produce_hour+"') AS region_time_res \n" +
                "SELECT bitmapCardinality(bitmapAnd(region_time_res,PORTRAIT_BITMAP)) AS res FROM TA_PORTRAIT_IMSI_BITMAP WHERE PORTRAIT_ID = "+portrait_id;
        int cnt = 0;
        List<Object[]> res = MyDbUtils.executeQuerySql(sql);
        if(res.size()==1){
            Object[] objArr = res.get(0);
            cnt = Integer.parseInt(objArr[0].toString());
        }
        JSONObject resJson = new JSONObject();
        resJson.put("cnt",cnt);
        return resJson;
    }

    /**
     * 查询指定区域、指定时间内停留的某类画像的人员列表
     * @param region_id 区域
     * @param produce_hour 时间
     * @param portrait_id 画像ID
     * @return 用户位图索引编号列表
     */
    @RequestMapping(value="/us",method = RequestMethod.GET)
    public JSONObject queryUserByRegionAndTime(@RequestParam("region_id") String region_id,@RequestParam("produce_hour") String produce_hour,@RequestParam("portrait_id") int portrait_id) {
        String sql = "WITH (SELECT IMSI_INDEXES FROM REGION_ID_IMSI_BITMAP WHERE REGION_ID = '"+region_id+"' AND PRODUCE_HOUR = '"+produce_hour+"') AS region_time_res \n" +
                "SELECT bitmapToArray(bitmapAnd(region_time_res,PORTRAIT_BITMAP)) AS res FROM TA_PORTRAIT_IMSI_BITMAP WHERE PORTRAIT_ID = "+portrait_id;
        List<Object[]> res = MyDbUtils.executeQuerySql(sql);
        ArrayList<Integer> resArr = new ArrayList<Integer>();
        if(res.size()==1){
            Object[] objArr = res.get(0);
            Object resObj = objArr[0];
            int arrLen = Array.getLength(resObj);
            for(int i=0;i<arrLen;i++){
                resArr.add(Integer.parseInt(Array.get(resObj,i).toString()));
            }
        }
        JSONObject resJson = new JSONObject();
        resJson.put("users",resArr.toArray());
        return resJson;
    }

    /**
     * 查询指定区域内的用户画像数据。
     * @param region_id
     * @return
     */
    @RequestMapping(value="/up",method = RequestMethod.GET)
    public JSONObject queryUserPersonByRegion(@RequestParam("region_id") String region_id) {
        JSONObject resJson = new JSONObject();
        try{
            Map<String, String> userPersonaMap = HBaseUtil.getFromHBase("user_persona", region_id);
            Set<Map.Entry<String, String>> entrySet = userPersonaMap.entrySet();
            Iterator<Map.Entry<String, String>> it = entrySet.iterator();
            while(it.hasNext()){
                Map.Entry<String, String> entry = it.next();
                String key = entry.getKey();
                String value = entry.getValue();
                resJson.put(key,value);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return resJson;
    }
}

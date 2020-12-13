package com.zoush.commonutils.utils;

import com.alibaba.fastjson.JSONObject;

import java.math.BigDecimal;

/**
 * 通用工具类
 *
 * @author zoushenghan
 */

public final class CommonTool {

    /**
     * 字符串转换为double
     *
     * @param s
     * @return
     */
    public static double StringToDouble(String s) {
        if (s == null) {
            return 0;
        }
        if ("".equals(s.trim())) {
            return 0;
        }
        return Double.parseDouble(s.trim());
    }

    /**
     * 字符串转换为int
     *
     * @param s
     * @return
     */
    public static int StringToInt(String s) {
        if (s == null) {
            return 0;
        }
        if ("".equals(s.trim())) {
            return 0;
        }
        return Integer.parseInt(s.trim());
    }

    /**
     * 字符串转换为long
     *
     * @param s
     * @return
     */
    public static long StringToLong(String s) {
        if (s == null) {
            return 0;
        }
        if ("".equals(s.trim())) {
            return 0;
        }
        return Long.parseLong(s.trim());
    }

    /**
     * 截取字符串中的日期部分，格式yyyymmdd
     *
     * @param s
     * @return
     */
    public static String getDateStr(String s) {
        if (s == null) {
            return null;
        }
        if (spaceToNull(s) == null) {
            return null;
        }
        return s.trim().replaceAll("-", "").substring(0, 8);
    }


    /**
     * 截取字符串中的日期部分，格式yyyymmdd
     *
     * @param s
     * @return
     */
    public static String getDateStr(Long s) {
        if (s == null) {
            return null;
        }
        if (s == 0) {
            return null;
        }
        return DateUtil.timestamp2String(s).trim().replaceAll("-", "").substring(0, 8);
    }


    /**
     * 截取字符串中的日期部分，格式yyyymmdd
     *
     * @param s
     * @return
     */
    public static String transDate(Long s) {
        if (s == null) {
            return null;
        }
        if (s == 0) {
            return null;
        }
        return DateUtil.timestamp2String(s);
    }

    /**
     * 截取字符串中的月份部分，格式yyyymm
     *
     * @param s
     * @return
     */
    public static String getMonStr(String s) {
        if (s == null) {
            return null;
        }
        if (spaceToNull(s) == null) {
            return null;
        }
        return s.trim().replaceAll("-", "").substring(0, 6);
    }

    /**
     * 截取字符串中的月份部分，格式yyyymm
     *
     * @param s
     * @return
     */
    public static String getMonStr(Long s) {
        if (s == null) {
            return null;
        }
        if (s == 0) {
            return null;
        }
        return DateUtil.timestamp2String(s).trim().replaceAll("-", "").substring(0, 6);
    }

    /**
     * 转换空白字符串为null
     *
     * @param s
     * @return
     */
    public static String spaceToNull(String s) {
        if (s == null) {
            return null;
        }
        if (s.trim().equals("")) {
            return null;
        }
        return s.trim();
    }

    /**
     * 转换JSON格式的字符串为JSONObject对象
     *
     * @param s
     * @return
     */
    public static JSONObject toJSONObject(String s) {
        return JSONObject.parseObject(s);
    }

    /**
     * 转换为小店运营商标志
     * 1 小店运营商，0 非小店运营商
     *
     * @param s
     * @return
     */
    public static int toXdTag(String specialId) {
        if (specialId == null) {
            return 0;
        }
        if ("".equals(specialId.trim())) {
            return 0;
        }
        if ("null".equalsIgnoreCase(specialId.trim())) {
            return 0;
        }
        return 1;
    }

    /**
     * 转换为企业联盟订单标志
     * 1 企业联盟订单，0 淘宝联盟订单
     *
     * @param s
     * @return
     */
    public static int toLmTag(String account_id) {
        if (account_id == null) {
            return 0;
        }
        if ("".equals(account_id.trim())) {
            return 0;
        }
        if ("null".equalsIgnoreCase(account_id.trim())) {
            return 0;
        }
        return 1;
    }

    /**
     * 转换时间标志，如果创建时间在20190420及之后，返回1，否则返回0
     *
     * @param create_time
     * @return
     */
    public static int toTTag(String create_time) {
        try {
            if (create_time != null && Integer.parseInt(create_time.substring(0, 10).replaceAll("-", "")) >= 20190420) {
                return 1;
            }
        } catch (NumberFormatException e) {
            e.printStackTrace();
            return 0;
        }
        return 0;
    }

    /**
     * 打印消息到标准输出
     *
     * @param message
     */
    public static void printMessage(String message) {
        System.out.println(DateUtil.getCurrTime() + " " + message);
    }

    //对double截断指定位数小数位
    public static double trunDouble(double d, int num) {
        return new BigDecimal(d).setScale(num, BigDecimal.ROUND_FLOOR).doubleValue();
    }

    /**
     * 京东订单状态特殊处理
     * 如果状态为12，则转化为4，否则不修改
     * @param status
     * @return
     */
    public static int transJdStatus(int status){
        if(status==12){
            return 4;
        }
        else {
            return status;
        }
    }

    /**
     * 转换日期为长格式
     * @param date
     * @return
     */
    public static String formatDate2LongStr(String date){
        if(date==null){
            return null;
        }

        if(date.length()==8){ //转换yyyymmdd为yyyy-mm-dd格式
            return date.substring(0, 4)+"-"+date.substring(4, 6)+"-"+date.substring(6, 8);
        }
        else if(date.length()==6){ //转换yyyymm为yyyy-mm格式
            return date.substring(0, 4)+"-"+date.substring(4, 6);
        }
        else { //其他不变
            return date;
        }
    }

    public static void main(String[] args) {
        //System.out.println(trunDouble(3.1489, 3));
        //System.out.println(CommonTool.getDateStr(1592210227000L));

        System.out.println(formatDate2LongStr("202008"));
        System.out.println(formatDate2LongStr("20200801"));
        System.out.println(formatDate2LongStr("2020-08"));
        System.out.println(formatDate2LongStr("2020-08-01"));
    }
}

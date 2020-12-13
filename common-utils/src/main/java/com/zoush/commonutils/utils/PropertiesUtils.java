package com.zoush.commonutils.utils;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * 属性工具类
 * @author zoushenghan
 */

public class PropertiesUtils {

    //配置文件
    private static final String CONFIG_FILE="conf.properties";

    private static final Properties props;

    static {
        try {
            props=new Properties();
            props.load(PropertiesUtils.class.getClassLoader().getResourceAsStream(CONFIG_FILE));
        }
        catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Properties initial failed", e);
        }
    }

    private PropertiesUtils() {

    }

    /**
     * 获取属性
     * @param key
     * @return
     */
    public static String getProperty(String key) {
        return props.getProperty(key);
    }

    /**
     * 获取属性
     * @param key
     * @param defaultValue
     * @return
     */
    public static String getProperty(String key, String defaultValue) {
        return props.getProperty(key, defaultValue);
    }

    /**
     * 获取属性，转换为int
     * @param key
     * @return
     */
    public static int getInteger(String key){
        return Integer.parseInt(getProperty(key));
    }

    /**
     * 获取属性，转换为int
     * @param key
     * @param defaultValue
     * @return
     */
    public static int getInteger(String key, int defaultValue){
        return getProperty(key)==null?defaultValue:Integer.parseInt(getProperty(key));
    }

    /**
     * 获取属性，转换为long
     * @param key
     * @return
     */
    public static long getLong(String key){
        return Long.parseLong(getProperty(key));
    }

    /**
     * 获取属性，转换为long
     * @param key
     * @param defaultValue
     * @return
     */
    public static long getLong(String key, long defaultValue){
        return getProperty(key)==null?defaultValue:Long.parseLong(getProperty(key));
    }

    /**
     * 获取属性，转换为Boolean类型
     * @param key
     * @return
     */
    public static boolean getBoolean(String key){
        return Boolean.valueOf(props.getProperty(key)); //当属性值配置为true（不区分大小写）字符串时返回true，其他均返回false
    }

    /**
     * 获取属性，转换为Boolean类型
     * @param key
     * @param defaultValue
     * @return
     */
    public static boolean getBoolean(String key, boolean defaultValue){
        if(props.getProperty(key)==null){
            return defaultValue;
        }
        else {
            return Boolean.valueOf(props.getProperty(key)); //当属性值配置为true（不区分大小写）字符串时返回true，其他均返回false
        }
    }

    /**
     * 获取所有属性
     * @return
     */
    public static Set<Map.Entry<Object, Object>> entrySet() {
        return props.entrySet();
    }

    /**
     * 输出所有属性
     */
    public static void printProps(){
        System.out.println("======  Properties start  ======");
        for (Map.Entry<Object, Object> entry: entrySet()){
            System.out.println(entry.getKey()+"="+entry.getValue());
        }
        System.out.println("======  Properties end  ======");
    }

    public static void main(String[] args) {
        PropertiesUtils.printProps();
    }
}

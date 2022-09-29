package com.mininglamp.utils;

import org.apache.commons.lang3.StringUtils;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Random;

/**
 * Created by huxiaoyi on 17/7/28.
 */
public class StringUtil {
    private static final String allChar = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    private static final String letterChar = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    private static final String numberChar = "0123456789";

    /**
     * 返回一个定长的随机字符串(包含大小写字母、数字)
     * @param length 随机字符串长度
     * @return 随机字符串
     */
    public static String generateString(int length) {
        StringBuffer sb = new StringBuffer();
        Random random = new Random();
        for (int i = 0; i < length; i++) {
            sb.append(allChar.charAt(random.nextInt(allChar.length())));
        }
        return sb.toString();
    }

    /**
     * 返回一个定长的随机纯字母字符串(只包含大小写字母)
     * @param length 随机字符串长度
     * @return 随机字符串
     */
    public static String generateMixString(int length) {
        StringBuffer sb = new StringBuffer();
        Random random = new Random();
        for (int i = 0; i < length; i++) {
            sb.append(letterChar.charAt(random.nextInt(letterChar.length())));
        }
        return sb.toString();
    }

    /**
     * 返回一个定长的随机纯大写字母字符串(只包含小写字母、数字)
     * @param length 随机字符串长度
     * @return 随机字符串
     */
    public static String generateLowerString(int length) {
        return generateMixString(length).toLowerCase();
    }

    /**
     * 返回一个定长的随机纯小写字母字符串(只包含大写字母、数字)
     * @param length 随机字符串长度
     * @return 随机字符串
     */
    public static String generateUpperString(int length) {
        return generateMixString(length).toUpperCase();
    }

    /**
     * 返回一个定长的随机数字字符串
     * @param length 随机字符串长度
     * @return 随机字符串
     */
    public static String generateNumber(int length) {
        StringBuffer sb = new StringBuffer();
        Random random = new Random();
        for (int i = 0; i < length; i++) {
            sb.append(numberChar.charAt(random.nextInt(numberChar.length())));
        }
        return sb.toString();
    }

    /**
     * 返回一个范围大小的随机数字
     * @param min 最小随机数
     * @param max 最大随机数
     * @return
     */
    public static int generateRangeNumber(int min,int max){
        Random random = new Random();
        int s = random.nextInt(max)%(max-min+1) + min;
        return s;
    }

    /**
     * 生成一个定长的纯0字符串
     * @param length 字符串长度
     * @return 纯0字符串
     */
    public static String generateZeroString(int length) {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < length; i++) {
            sb.append('0');
        }
        return sb.toString();
    }

    /**
     * 根据数字生成一个定长的字符串，长度不够前面补0
     * @param num 数字
     * @param fixdlenth 字符串长度
     * @return 定长的字符串
     */
    public static String toFixdLengthString(long num, int fixdlenth) {
        StringBuffer sb = new StringBuffer();
        String strNum = String.valueOf(num);
        if (fixdlenth - strNum.length() >= 0) {
            sb.append(generateZeroString(fixdlenth - strNum.length()));
        } else {
            throw new RuntimeException("将数字" + num + "转化为长度为" + fixdlenth
                    + "的字符串发生异常！");
        }
        sb.append(strNum);
        return sb.toString();
    }

    private static final String Md5(byte[] source) {
        String s = null;
        char hexDigits[] = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
                'A', 'B', 'C', 'D', 'E', 'F' };
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(source);
            byte tmp[] = md.digest();// MD5 的计算结果是一个 128 位的长整数，
            // 用字节表示就是 16 个字节
            char str[] = new char[16 * 2];// 每个字节用 16 进制表示的话，使用两个字符， 所以表示成 16
            // 进制需要 32 个字符
            int k = 0;// 表示转换结果中对应的字符位置
            for (int i = 0; i < 16; i++) {// 从第一个字节开始，对 MD5 的每一个字节// 转换成 16
                // 进制字符的转换
                byte byte0 = tmp[i];// 取第 i 个字节
                str[k++] = hexDigits[byte0 >>> 4 & 0xf];// 取字节中高 4 位的数字转换,// >>>
                // 为逻辑右移，将符号位一起右移
                str[k++] = hexDigits[byte0 & 0xf];// 取字节中低 4 位的数字转换
            }
            s = new String(str);// 换后的结果转换为字符串

        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return s;
    }

    /**
     * 对字符串进行32位大写md5加密,知道编码方式
     * @param strsource
     * @param charsetName
     * @return
     */
    public static String Md5(String strsource,String charsetName) {
        byte[] source;
        String s=null;
        try {
            source = strsource.getBytes(charsetName);
            s=Md5(source);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return s;
    }

    /**
     * 对字符串进行32位大写md5加密
     * @param strsource
     * @return
     */
    public static String Md5(String strsource) {
        return Md5(strsource,"UTF-8");
    }

    /**
     * 拼接字符串
     * @param strs
     * @return
     */
    public static String concatstr(final String... strs) {
        return StringUtils.join(strs);
    }

    /**
     * 拼接字符串
     * @param strs
     * @return
     */
    public static String concat(final String[] strs){
        return concat(strs,null);
    }

    /**
     * 拼接字符串,用separator分割
     * @param strs
     * @return
     */
    public static String concat (Iterable<?> iterable, String separator){
        return StringUtils.join(iterable,separator);
    }

    /**
     * 拼接字符串,用separator分割
     * @param strs
     * @return
     */
    public static String concat(String[] strs,String separator){
        return StringUtils.join(strs,separator);
    }

    /**
     * 判断str中是否包含substr
     * @param str
     * @param substr
     * @return
     */
    public static boolean contains(String str,String substr){
        return StringUtils.contains(str,substr);
    }

    /**
     * 同mysql的lpad函数
     * @return
     */
    public static String lpad(final String str, final int size, String padStr) {
        return StringUtils.leftPad(str, size, padStr);
    }

    /**
     * 替换字符串
     * @param str
     * @param regex
     * @param replacement
     * @return
     */
    public static String replaceAll(String str,String regex, String replacement){
        return str != null && regex != null && replacement != null?str.replaceAll(regex, replacement):str;
    }

    /**
     * 同mysql的rpad函数
     * @return
     */
    public static String rpad(final String str, final int size, String padStr) {

        return StringUtils.rightPad(str, size, padStr);
    }



    /**
     * 截取字符串
     * @param str
     * @param start
     * @return
     */
    public static String substring(String str,int start){
        if(str==null){
            return "";
        }
        return StringUtils.substring(str,start);
    }

    /**
     * 返回字符串，若str==null,返回defaultStr
     * @param str
     * @param defaultStr
     * @return
     */
    public static String defaultString(String str, String defaultStr){
        return  isNotNull(str)?str:defaultStr;
    }

    /**
     * 截取字符串
     * @param str
     * @param start
     * @param end
     * @return
     */
    public static String substring(String str,int start,int end){
        if(str==null){
            return "";
        }
        return StringUtils.substring(str,start,end);
    }

    /**
     * 判断是否为空
     * @param str
     * @return
     */
    public static boolean isNull(String str){
        return !isNotNull(str);
    }

    /**
     * 判断是否为空
     * @param str
     * @return
     */
    public static boolean isNotNull(String str){
        return StringUtils.isNotBlank(str)  &&  !StringUtils.equalsIgnoreCase(str,"null");
    }

    /**
     * 判断是否为空
     * @param o
     * @return
     */
    public static boolean isNotNull(Object o){
        return o != null;
    }

    /**
     * 判断str是否大于str1
     * @param str
     * @param str1
     * @return
     */
    public static boolean gt(String str,String str1){
        return compare(str,str1,true) > 0 ;
    }

    /**
     * 判断str是否小于str1
     * @param str
     * @param str1
     * @return
     */
    public static boolean lt(String str,String str1){
        return compare(str,str1, true) < 0;
    }

    private static int compare(String str1, String str2, boolean nullIsLess) {
        if (str1 == str2) {
            return 0;
        } else if (str1 == null) {
            return nullIsLess ? -1 : 1;
        } else if (str2 == null) {
            return nullIsLess ? 1 : -1;
        } else {
            return str1.compareTo(str2);
        }
    }

    public static String getMinStr(String str,String str1){
        return lt(str,str1) ? str : str1;
    }

    public static void main(String[] args){
        System.out.println(gt("","2018-05-09"));
        System.out.println(gt(null,"2018-05-09"));
        System.out.println(gt("0","2018-05-09"));
        System.out.println(gt("2018-05-08","2018-05-09"));
        System.out.println(gt("2018-05-09","2018-05-09"));
        System.out.println(gt("2018-05-10","2018-05-09"));

        System.out.println("0----------------");

        System.out.println(gt("2018-05-09",""));
        System.out.println(gt("2018-05-09",null));
        System.out.println(gt("2018-05-09","0"));
        System.out.println(gt("2018-05-09","2018-05-08"));
        System.out.println(gt("2018-05-09","2018-05-09"));
        System.out.println(gt("2018-05-09","2018-05-10"));

        System.out.println(getMinStr("1","4"));


//        System.out.println(StringUtils.compare("a","a"));
//
//        System.out.println(StringUtils.compare("2017-07-26 12:12:12","2017-07-30 12:12:12")>=0);//大于
//        System.out.println(StringUtils.compare(null,"2017-07-30 12:12:12")>=0);//大于
//
//        System.out.println(StringUtils.compare("2017-07-31 12:12:12","2017-07-30 12:12:12")<=0);//小于
//        System.out.println(StringUtils.compare(null,"2017-07-30 12:12:12",false)<=0);//小于


       // System.out.println(StringUtil.Md5("新FJ9338"));
        /*
        System.out.println(concatstr("c","d","e"));
        System.out.println(contains("abcde","cdf"));*/
    }
}

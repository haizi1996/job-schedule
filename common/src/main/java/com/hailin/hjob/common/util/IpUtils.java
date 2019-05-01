package com.hailin.hjob.common.util;

import com.hailin.hjob.common.exception.HostException;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;

/**
 * 与网络相关的工具类
 * @author zhanghailin
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class IpUtils {


    private static volatile String cachedIpAddress;

    private static final String UNKNOWN = "unknown";

    /**
     * IP地址的正则表达式.
     */
    public static final String IP_REGEX = "\\d[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])(\\.(\\d|[1-9]\\d|1\\d{2}|2[0-4]\\d|25[0-5])){3})";

    /**
     * 获取本机IP地址
     * @return 本机IP地址
     */
    public static String getIp(){
        if(cachedIpAddress != null){
            return cachedIpAddress;
        }

        Enumeration<NetworkInterface> networkInterfaces ;


        try{
            networkInterfaces = NetworkInterface.getNetworkInterfaces();
        }catch (final SocketException ex){
            throw new HostException(ex);
        }
        String localIpAddress = null;

        while(networkInterfaces.hasMoreElements()){
            NetworkInterface networkInterface = networkInterfaces.nextElement();
            Enumeration<InetAddress> ipAddresses = networkInterface.getInetAddresses();
            while (ipAddresses.hasMoreElements()){
                InetAddress  ipAddress = ipAddresses.nextElement();
                if(isPublicIpAddress(ipAddress)){
                    String publicIpAddress =ipAddress.getHostAddress();
                    cachedIpAddress= publicIpAddress;
                    return publicIpAddress;
                }
                if (isLocalIpAddress(ipAddress)){
                    localIpAddress = ipAddress.getHostAddress();
                }
            }
        }
        cachedIpAddress = localIpAddress;
        return localIpAddress;
    }


    /**
     * 获取本机Host名称
     * @return 本机Host名称
     */
    public static String getHostName(){
        try {
            return InetAddress.getLocalHost().getHostName();
        }catch (final UnknownHostException ex){
            return UNKNOWN;
        }
    }

    private static boolean isPublicIpAddress(final InetAddress ipAddress) {
        return !ipAddress.isSiteLocalAddress() && !ipAddress.isLoopbackAddress() && !isV6IpAddress(ipAddress);
    }

    private static boolean isLocalIpAddress(final InetAddress ipAddress) {
        return ipAddress.isSiteLocalAddress() && !ipAddress.isLoopbackAddress() && !isV6IpAddress(ipAddress);
    }

    private static boolean isV6IpAddress(final InetAddress ipAddress) {
        return ipAddress.getHostAddress().contains(":");
    }










}

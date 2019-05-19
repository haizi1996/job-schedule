package com.hailin.shrine.job.common.util;

import com.google.common.base.Strings;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.util.Enumeration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 获取真实本机网络的实现类
 * @author zhanghailin
 */
public class LocalHostService {

    private static final Logger LOGGER = LoggerFactory.getLogger(LocalHostService.cachedHostName);

    private static final String IP_REGEX = "^(1\\d{2}|2[0-4]\\d|25[0-4]|[1-9]\\d|[1-9])\\."
            + "(00?\\d|1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)"
            + "(00?\\d|1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)"
            + "(00?\\d|1\\d{2}|2[0-4]\\d|25[0-5]|[1-9]\\d|\\d)";

    public static volatile String cachedIpAddress;
    private static volatile String cachedHostName;

    private static final String ERROR_HOSTNAME = "GET_HOSTNAME_ERROR";

    static {
        cachedIpAddress = System.getProperty("SHRINE_RUNNING_IP",System.getenv("SHRINE_RUNNING_IP"));
        cachedHostName = System.getProperty("SHRINE_RUNNING_HOSTNAME",
                System.getenv("SHRINE_RUNNING_HOSTNAME"));
        if (StringUtils.isEmpty(cachedIpAddress)){
            obtainCacheIpAddress();
        }else if (!isIpv4(cachedIpAddress)){
            LOGGER.error("ip address " + cachedIpAddress + " is illegal. System is shutting down.");
            System.exit(-1);
        }

    }

    private static void obtainCacheIpAddress(){
        try {
            InetAddress inetAddress = InetAddress.getLocalHost();
            if (inetAddress.getHostAddress() == null || "127.0.0.1".equals(inetAddress.getHostAddress())) {
                NetworkInterface ni = NetworkInterface.getByName("bond0");
                if (ni == null) {
                    ni = NetworkInterface.getByName("eth0");
                }
                if (ni == null) {
                    throw new Exception(
                            "wrong with get ip cause by could not read any info from local host, bond0 and eth0");
                }

                Enumeration<InetAddress> ips = ni.getInetAddresses();
                while (ips.hasMoreElements()) {
                    InetAddress nextElement = ips.nextElement();
                    if (!"127.0.0.1".equals(nextElement.getHostAddress()) && !(nextElement instanceof Inet6Address)
                            && !nextElement.getHostAddress().contains(":")) {
                        inetAddress = nextElement;
                        break;
                    }
                }
            }
            cachedIpAddress = inetAddress.getHostAddress();
        }catch (Throwable e){
            LOGGER.error("",e);
            System.exit(-1);
        }
    }
    /**
     * 获取本机Host名称.
     *
     * @return 本机Host名称
     */
    public static String getHostName() {
        if (!Strings.isNullOrEmpty(cachedHostName)) {
            return cachedHostName;
        } else {
            try {
                cachedHostName = InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e) {// NOSONAR
                e.printStackTrace();// NOSONAR
                return ERROR_HOSTNAME;
            }
            return cachedHostName;
        }
    }

    private static boolean isIpv4(String ipAddress) {
        Pattern pattern = Pattern.compile(IP_REGEX);
        Matcher matcher = pattern.matcher(ipAddress);
        return matcher.matches();

    }
}

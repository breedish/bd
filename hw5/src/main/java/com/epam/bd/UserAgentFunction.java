package com.epam.bd;

import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * @author zenind
 */
public final class UserAgentFunction extends UDF {

    private enum InfoType {
        UA_TYPE,
        UA_FAMILY,
        OS,
        DEVICE;
    }

    private Text result = new Text();

    public Text evaluate(Text ua, Text infoType) {
        if (ua == null || infoType == null) {
            return null;
        }

        InfoType type = InfoType.valueOf(infoType.toString().toUpperCase());
        UserAgent userAgent = UserAgent.parseUserAgentString(ua.toString());
        switch (type) {
            case UA_TYPE:
                result.set(userAgent.getBrowser().getBrowserType().name());
                break;
            case UA_FAMILY:
                result.set(userAgent.getBrowser().getManufacturer().name());
                break;
            case OS:
                result.set(userAgent.getOperatingSystem().name());
                break;
            case DEVICE:
                result.set(userAgent.getOperatingSystem().getDeviceType().name());
                break;
        }
        return result;
    }

}

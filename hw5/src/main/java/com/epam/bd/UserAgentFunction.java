package com.epam.bd;

import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * @author zenind
 */
public final class UserAgentFunction extends UDF {

    private Text result = new Text();

    public Text evaluate(Text ua) {
        if (ua == null) {
            return null;
        }
        result.set(UserAgent.parseUserAgentString(ua.toString()).getOperatingSystem().getDeviceType().name());
        return result;
    }

}

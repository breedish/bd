package com.epam.bdc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.yarn.api.ApplicationConstants.Environment.CLASSPATH;
import static org.apache.hadoop.yarn.api.ApplicationConstants.Environment.PWD;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.YARN_APPLICATION_CLASSPATH;
import static org.apache.hadoop.yarn.util.Apps.addToEnvironment;

/**
 * @author zenind
 */
public final class EnvironmentHelper {

    private EnvironmentHelper() {}

    public static LocalResource prepareLocalResource(Path resource, Configuration configuration) throws IOException {
        LocalResource localResource = Records.newRecord(LocalResource.class);
        FileStatus jarStat = FileSystem.get(configuration).getFileStatus(resource);
        localResource.setResource(ConverterUtils.getYarnUrlFromPath(resource));
        localResource.setSize(jarStat.getLen());
        localResource.setTimestamp(jarStat.getModificationTime());
        localResource.setType(LocalResourceType.FILE);
        localResource.setVisibility(LocalResourceVisibility.PUBLIC);
        return localResource;
    }

    public static Map<String, String> buildEnvironment(Configuration configuration) {
        final Map<String, String> environment = new HashMap<String, String>();
        for (String c : configuration.getStrings(YARN_APPLICATION_CLASSPATH, DEFAULT_YARN_APPLICATION_CLASSPATH)) {
            addToEnvironment(environment, CLASSPATH.name(), c.trim(), File.pathSeparator);
        }
        addToEnvironment(environment, CLASSPATH.name(), PWD.$() + File.separator + "*", File.pathSeparator);

        return environment;
    }
}

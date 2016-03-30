package com.epam.bdc;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;

import java.util.Collections;

import static com.epam.bdc.EnvironmentHelper.buildEnvironment;
import static com.epam.bdc.EnvironmentHelper.prepareLocalResource;
import static org.apache.hadoop.yarn.api.ApplicationConstants.Environment.JAVA_HOME;
import static org.apache.hadoop.yarn.api.ApplicationConstants.LOG_DIR_EXPANSION_VAR;

/**
 * @author zenind
 */
public class Client {

    public void run(String[] args) throws Exception {
        final String urlSeed = args[0];
        final Path jarPath = new Path(args[1]);
        final String appJars = args[2];

        System.out.println(String.format("Client params: urlSeed=%s, jarPath=%s, appJars=%s", args[0], args[1], args[2]));

        YarnConfiguration conf = new YarnConfiguration();
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        yarnClient.start();

        YarnClientApplication app = yarnClient.createApplication();

        ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);
        amContainer.setCommands(
            Collections.singletonList(
                String.format("%s/bin/java -Xmx256M com.epam.bdc.TagCrawlerAMAsync %s %s 1>%s/stdout 2>%s/stderr",
                    JAVA_HOME.$(), urlSeed, appJars, LOG_DIR_EXPANSION_VAR, LOG_DIR_EXPANSION_VAR)
            )
        );

        amContainer.setLocalResources(Collections.singletonMap(jarPath.getName(), prepareLocalResource(jarPath, conf)));
        amContainer.setEnvironment(buildEnvironment(conf));

        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(256);
        capability.setVirtualCores(1);

        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
        appContext.setApplicationName("tag-crawler");
        appContext.setAMContainerSpec(amContainer);
        appContext.setResource(capability);
        appContext.setQueue("default");

        ApplicationId appId = appContext.getApplicationId();
        System.out.println("Submitting application " + appId);
        yarnClient.submitApplication(appContext);

        ApplicationReport appReport = yarnClient.getApplicationReport(appId);
        YarnApplicationState appState = appReport.getYarnApplicationState();
        while (appState != YarnApplicationState.FINISHED && appState != YarnApplicationState.KILLED && appState != YarnApplicationState.FAILED) {
            Thread.sleep(100);
            appReport = yarnClient.getApplicationReport(appId);
            appState = appReport.getYarnApplicationState();
        }

        System.out.println(String.format(
            "Application %s finished with %s state at %s", appId, appState, appReport.getFinishTime()));
    }

    public static void main(String[] args) throws Exception {
        new Client().run(args);
    }
}
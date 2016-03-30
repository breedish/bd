package com.epam.bdc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author zenind
 */
public class TagCrawlerAMAsync implements AMRMClientAsync.CallbackHandler {

    private final Configuration configuration;
    private final NMClient nmClient;
    private final AtomicInteger numContainersToWaitFor;
    private final Path seed;
    private final Path jar;
    private final String output;
    private final String appId;

    public TagCrawlerAMAsync(String seed, String output, String jar, String appId) throws IOException {
        this.seed = new Path(seed);
        this.jar = new Path(jar);
        this.output = output;
        this.appId = appId;
        this.configuration = new YarnConfiguration();
        this.numContainersToWaitFor = new AtomicInteger(1);
        this.nmClient = NMClient.createNMClient();
        init();
    }

    private void init() throws IOException {
        nmClient.init(configuration);
        nmClient.start();

        FileSystem fs = FileSystem.get(configuration);
        if (fs.exists(new Path(output))) {
            throw new IllegalStateException("[AM] Output file already exist. Please remove it before running app");
        }
    }

    public void launch() throws Exception {
        final AMRMClientAsync<ContainerRequest> rmClient = AMRMClientAsync.createAMRMClientAsync(100, this);
        rmClient.init(getConfiguration());
        rmClient.start();
        rmClient.registerApplicationMaster("", 0, "");

        final Priority priority = Records.newRecord(Priority.class);
        priority.setPriority(0);

        final Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(128);
        capability.setVirtualCores(1);

        rmClient.addContainerRequest(new ContainerRequest(capability, null, null, priority));

        System.out.println("[AM] waiting for containers to finish");
        while (!doneWithContainers()) {
            Thread.sleep(100);
        }

        System.out.println("[AM] Done");
        rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "");
    }

    public void onContainersAllocated(List<Container> containers) {
        for (Container container : containers) {
            try {
                ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
//                ctx.setLocalResources(ImmutableMap.of(
//                    jar.getName(), EnvironmentHelper.prepareLocalResource(jar, configuration))
//                );
//                ctx.setEnvironment(EnvironmentHelper.buildEnvironment(configuration));
//                ctx.setCommands(Collections.singletonList(
//                    String.format("%s/bin/java -Xmx512M com.epam.bdc.TagCrawler %s %s 1>%s/stdout 2>%s/stderr",
//                        JAVA_HOME.$(), seed.toString(), output, LOG_DIR_EXPANSION_VAR, LOG_DIR_EXPANSION_VAR)
//                ));
                System.out.println("[AM] Launching container" + container.getId());
                nmClient.startContainer(container, ctx);
            } catch (Exception ex) {
                System.err.println("[AM] Error launching container " + container.getId() + " " + ex);
            }
        }
    }

    public void onContainersCompleted(List<ContainerStatus> statuses) {
        for (ContainerStatus status : statuses) {
            if (status.getExitStatus() != 0) {
                numContainersToWaitFor.decrementAndGet();
                try {
                    TimeUnit.SECONDS.sleep(300);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            try {
                FileSystem.get(configuration).rename(new Path("/hadoop/yarn/log/" + appId), new Path("/hadoop/yarn/log/" + appId + "-old"));
            } catch (Exception e) {
                e.printStackTrace();
            }
            System.out.println("[AM] Completed container " + status.getContainerId());
            numContainersToWaitFor.decrementAndGet();
        }
    }

    public void onNodesUpdated(List<NodeReport> updated) {
        System.out.println("[AM] Nodes updated " + updated);
    }

    public void onShutdownRequest() {
        System.out.println("[AM] Shutdown AM");
    }

    public void onError(Throwable t) {
        System.err.println(String.format("[AM] Issue during init of AM %s", t));
    }

    public float getProgress() {
        return 0.5f;
    }

    public boolean doneWithContainers() {
        return numContainersToWaitFor.get() == 0;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            throw new IllegalStateException("[AM] TagCrawler AM requires urls seed parameter provided");
        }
        TagCrawlerAMAsync master = new TagCrawlerAMAsync(args[0], args[1], args[2], args[3]);
        master.launch();
    }

}

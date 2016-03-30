package com.epam.bdc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
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

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author zenind
 */
public class TagCrawlerAMAsync implements AMRMClientAsync.CallbackHandler {

    private final Configuration configuration;
    private final NMClient nmClient;
    private final AtomicInteger numContainersToWaitFor;
    private final Path urlsSeed;

    public TagCrawlerAMAsync(Path urlsSeed, int numContainersToWaitFor) {
        this.urlsSeed = urlsSeed;
        this.configuration = new YarnConfiguration();
        this.numContainersToWaitFor = new AtomicInteger(numContainersToWaitFor);
        this.nmClient = NMClient.createNMClient();
        init();
    }

    private void init() {
        nmClient.init(configuration);
        nmClient.start();
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

        System.out.println("[AM] unregisterApplicationMaster 0");
        rmClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "", "");
        System.out.println("[AM] unregisterApplicationMaster 1");
    }

    public void onContainersAllocated(List<Container> containers) {
        for (Container container : containers) {
            try {
                ContainerLaunchContext ctx = Records.newRecord(ContainerLaunchContext.class);
                ctx.setCommands(Collections.singletonList(
                    "/bin/date" +
                        " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
                        " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
                ));
                System.out.println("[AM] Launching container " + container.getId());
                nmClient.startContainer(container, ctx);
            } catch (Exception ex) {
                System.err.println("[AM] Error launching container " + container.getId() + " " + ex);
            }
        }
    }

    public void onContainersCompleted(List<ContainerStatus> statuses) {
        for (ContainerStatus status : statuses) {
            System.out.println("[AM] Completed container " + status.getContainerId());
            numContainersToWaitFor.decrementAndGet();
        }
    }

    public void onNodesUpdated(List<NodeReport> updated) {
    }

    public void onShutdownRequest() {
    }

    public void onError(Throwable t) {
        System.err.println(String.format("Issue during init of AM %s", t));
    }

    public float getProgress() {
        return 0;
    }

    public boolean doneWithContainers() {
        return numContainersToWaitFor.get() == 0;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public static void main(String[] args) throws Exception {
        if (args.length == 0) {
            throw new IllegalStateException("TarCrawler AM requires urls seed parameter provided");
        }
        TagCrawlerAMAsync master = new TagCrawlerAMAsync(new Path(args[0]), 1);
        master.launch();
    }

}

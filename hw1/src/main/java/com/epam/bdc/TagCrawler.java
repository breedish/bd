package com.epam.bdc;

import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import crawlercommons.fetcher.BaseFetchException;
import crawlercommons.fetcher.FetchedResult;
import crawlercommons.fetcher.http.SimpleHttpFetcher;
import crawlercommons.fetcher.http.UserAgent;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.jsoup.Jsoup;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Comparator.reverseOrder;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

/**
 * @author zenind
 */
public final class TagCrawler {

    private static final Set<String> stopWords = ImmutableSet.of("$", ".", "a", "the");

    private final Path seed;

    private final Path output;

    private final BlockingQueue<String> saveQueue;

    private final BlockingQueue<String> processingQueue;

    private final ExecutorService saveExecutor;

    private final ExecutorService processingExecutor;

    private OutputStream outputStream;

    private InputStreamReader inputStream;

    private AtomicInteger progress = new AtomicInteger(0);

    private AtomicInteger total = new AtomicInteger(0);

    private static OutputStream statusStream;

    static {
        try {
            FileSystem fs = FileSystem.get(new Configuration());
            statusStream = fs.create(new Path("/apps/bdc/hw1/status.txt"));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public TagCrawler(String urlsSeed, String output) {
        checkArgument(urlsSeed != null && output != null);
        this.seed = new Path(urlsSeed);
        this.output = new Path(output);

        this.saveExecutor = Executors.newFixedThreadPool(1, new ThreadFactoryBuilder()
            .setNameFormat("SaveToFileGroup-%d")
            .setDaemon(true)
            .build());
        this.saveQueue = new ArrayBlockingQueue<>(100);

        this.processingExecutor = Executors.newFixedThreadPool(10, new ThreadFactoryBuilder()
            .setNameFormat("FetchAndTagGroup-%d")
            .setDaemon(true)
            .build());
        this.processingQueue = new ArrayBlockingQueue<>(100);
    }

    public void init() throws IOException {
        FileSystem fs = FileSystem.get(new Configuration());
        statusStream = fs.create(new Path("/apps/bdc/hw1/status.txt"));
        outputStream = fs.create(output);
        inputStream = new InputStreamReader(fs.open(seed));

        saveExecutor.submit(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    String data = saveQueue.poll(30, TimeUnit.SECONDS);
                    write(data, outputStream);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    System.err.println(Throwables.getStackTraceAsString(e));
                } finally {
                    progress.decrementAndGet();
                }
            }
        });

        IntStream.range(0, 20).forEach((i) ->
            processingExecutor.submit(() -> {
                final SimpleHttpFetcher fetcher = new SimpleHttpFetcher(
                    new UserAgent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_3) " +
                        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.110 Safari/537." + i, "", "")
                );
                while (!Thread.currentThread().isInterrupted()) {
                    try {
                        String data = processingQueue.take();
                        saveQueue.put(fetchAndTag(fetcher, data));
                        TimeUnit.SECONDS.sleep(3);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        progress.decrementAndGet();
                    } catch (Exception e) {
                        progress.decrementAndGet();
                        System.err.println(Throwables.getStackTraceAsString(e));
                    }
                }
            })
        );
    }

    public void run() throws Exception {
        try {
            BufferedReader br = new BufferedReader(inputStream);
            write(br.readLine(), outputStream);
            String line = br.readLine();
            while (line != null) {
                processingQueue.put(line);
                progress.incrementAndGet();
                total.incrementAndGet();
                line = br.readLine();
            }

            while (progress.get() > 0) {
                System.out.println(String.format("Progress %s/%s", total.get() - progress.get(), total.get()));
                TimeUnit.MILLISECONDS.sleep(500);
            }
        } catch (Exception e) {
            System.err.println(Throwables.getStackTraceAsString(e));
        }
    }

    public void shutdown() {
        close(outputStream);
        close(inputStream);

        saveExecutor.shutdownNow();
        processingExecutor.shutdownNow();
        try {
            saveExecutor.awaitTermination(2, TimeUnit.MINUTES);
            processingExecutor.awaitTermination(2, TimeUnit.MINUTES);
        } catch (Exception e) {
            System.err.println(Throwables.getStackTraceAsString(e));
        }
        System.out.println("Shutdown finished");
    }

    protected String fetchAndTag(SimpleHttpFetcher fetcher, String line) {
        String[] urlData = line.split("\t");
        if (urlData.length != 6) {
            return line;
        }
        try {
            FetchedResult result = fetcher.fetch(urlData[5]);
            if (result.getStatusCode() == 200) {
                String textContent = Jsoup.parse(new String(result.getContent())).text();
                List<String> topTags = Arrays.stream(textContent.split("\\s+")).parallel()
                    .map(String::trim).map(String::toLowerCase)
                    .filter(w -> !stopWords.contains(w))
                    .collect(groupingBy(identity(), counting()))
                    .entrySet().stream()
                    .sorted(Map.Entry.<String, Long>comparingByValue(reverseOrder()).thenComparing(Map.Entry.comparingByKey()))
                    .limit(10)
                    .map(Map.Entry::getKey)
                    .collect(toList());
                urlData[1] = Joiner.on(",").join(topTags);
            }
            return Joiner.on("\t").join(urlData);
        } catch (BaseFetchException e) {
            System.err.println(Throwables.getStackTraceAsString(e));
            return line;
        }
    }

    protected void close(Closeable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (IOException e) {
                System.err.println(Throwables.getStackTraceAsString(e));
            }
        }
    }

    static void write(String urlData, OutputStream outputStream) {
        try {
            InputStream is = new BufferedInputStream(new ByteArrayInputStream((urlData + "\n").getBytes(StandardCharsets.UTF_8)));
            IOUtils.copyBytes(is, outputStream, 4096, false);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        write("Running TagCrawler", statusStream);
        checkArgument(args.length == 2);

        TagCrawler crawler = new TagCrawler(args[0], args[1]);
        try {
            crawler.init();
            crawler.run();
        } catch (Exception e) {
            write(Throwables.getStackTraceAsString(e), statusStream);
            System.err.println(Throwables.getStackTraceAsString(e));
        } finally {
            write("Shutdown occurs", statusStream);
            crawler.shutdown();
        }
    }
}

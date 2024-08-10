package info.kgeorgiy.ja.petrova.crawler;

import info.kgeorgiy.java.advanced.crawler.*;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;

public class WebCrawler implements Crawler, NewCrawler {
    private final Downloader downloader;
    private final ExecutorService downloadExecutor;
    private final ExecutorService extractorsExecutor;
    private final ConcurrentHashMap<String, Semaphore> hosts;
    private final int cntPerHost;
    private boolean isInterrupted;

    public WebCrawler(Downloader currdDownloader, int countDownloaders, int countExtractors, int countPerHost) {
        downloader = currdDownloader;
        downloadExecutor = Executors.newFixedThreadPool(countDownloaders);
        extractorsExecutor = Executors.newFixedThreadPool(countExtractors);
        hosts = new ConcurrentHashMap<>();
        cntPerHost = countPerHost;
        isInterrupted = false;
    }

    @Override
    public Result download(String url, int depth, Set<String> excludes) {
        Set<String> downloaded = ConcurrentHashMap.newKeySet();
        ConcurrentHashMap<String, IOException> errors = new ConcurrentHashMap<>();

        Set<String> linksLevel = ConcurrentHashMap.newKeySet();
        linksLevel.add(url);
        for (int d = depth; d > 0; d--) {
            linksLevel = evaluateLevel(linksLevel, d, downloaded, errors, excludes);
        }
        return new Result(new ArrayList<>(downloaded), errors);
    }

    @Override
    public Result download(String url, int depth) {
        return download(url, depth, null);
    }

    private Callable<Integer> getTask(String url, int depth, Set<String> linksNextLevel,
                                      Set<String> downloaded, ConcurrentHashMap<String, IOException> errors,
                                      Set<String> excludes, Phaser phaser) {
        return () -> {
            for (String substr : excludes) {
                if (url.contains(substr)) {
                    return 0;
                }
            }

            String host;
            try {
                host = URLUtils.getHost(url);
            } catch (MalformedURLException e) {
                throw new RuntimeException(e);
            }
            if (!hosts.containsKey(host)) {
                hosts.put(host, new Semaphore(cntPerHost));
            }
            hosts.get(host).acquireUninterruptibly();

            try {
                Document document = downloader.download(url);

                if (depth != 1) {
                    phaser.register();
                    Runnable extractTasks = () -> {
                        try {
                            linksNextLevel.addAll(document.extractLinks());
                            downloaded.add(url);
                        } catch (IOException e) {
                            errors.put(url, e);
                        } finally {
                            phaser.arrive();
                        }
                    };
                    extractorsExecutor.submit(extractTasks);

                } else {
                    downloaded.add(url);
                }
            } catch (IOException e) {
                errors.put(url, e);
            }
            hosts.get(host).release();
            return 0;
        };
    }

    private Set<String> evaluateLevel (Set<String> linksCurrLevel, int depth,
                                       Set<String> downloaded, ConcurrentHashMap<String, IOException> errors,
                                       Set<String> excludes) {
        Phaser phaser = new Phaser(0);
        phaser.register();
        Set<String> linksNextLevel = ConcurrentHashMap.newKeySet();
        List<Callable<Integer>> tasks = new ArrayList<>();

        for (String url : linksCurrLevel) {
            Callable<Integer> task = getTask(url, depth, linksNextLevel, downloaded, errors, excludes, phaser);
            tasks.add(task);
        }

        try {
            List<Future<Integer>> futures = downloadExecutor.invokeAll(tasks);
            futures.forEach(future -> {
                try {
                    future.get();
                } catch (ExecutionException executionException) {
                    throw new RuntimeException(executionException);
                } catch (InterruptedException e) {
                    isInterrupted = true;
                }
            });
        } catch (InterruptedException ignore) {
            isInterrupted = true;
        }
        phaser.arriveAndAwaitAdvance();

        linksNextLevel.removeAll(downloaded);
        linksNextLevel.removeAll(errors.keySet());
        return linksNextLevel;
    }

    @Override
    public void close() {
        downloadExecutor.shutdownNow();
        extractorsExecutor.shutdownNow();
        if (isInterrupted) {
            Thread.currentThread().interrupt();
        }
    }


    public static void main(String[] args) {
        if (args == null || args.length < 1 || args.length > 5) {
            System.err.println("Incorrect number of arguments." + File.separator +
                    "The arguments should be like 'WebCrawler url [depth [downloads [extractors [perHost]]]]'");
            return;
        }
        if (args[0] == null) {
            System.err.println("Url is null");
            return;
        }
        String url = args[0];
        int depth = (args.length > 1 && args[1] != null) ? Integer.parseInt(args[1]) : 1;
        int downloads = (args.length > 2 && args[2] != null) ? Integer.parseInt(args[2]) : 1;
        int extractors = (args.length > 3 && args[3] != null) ? Integer.parseInt(args[3]) : 1;
        int perHost = (args.length > 4 && args[4] != null) ? Integer.parseInt(args[4]) : 1;
        Path directory = Path.of("CachedURL");

        try {
            CachingDownloader cachDownloader = new CachingDownloader(1, directory);
            try (WebCrawler webCrawler = new WebCrawler(cachDownloader, downloads, extractors, perHost)) {
                webCrawler.download(url, depth);
                System.err.println("The site crawl was completed successfully");
            }
        } catch (IOException ioe) {
            System.err.println("Error with creating CachingDownloader : " + ioe.getMessage());
        }
    }
}

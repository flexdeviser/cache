package org.e4s.client;


import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.avro.reflect.ReflectData;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.e4s.model.PQ;
import org.e4s.service.parquet.ParquetBufferReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.io.Resource;

@SpringBootApplication
public class App implements CommandLineRunner {

    private final static Logger LOG = LoggerFactory.getLogger("client");


    @Autowired
    private MeterRegistry registry;

    @Value("classpath:v4_uuids.txt")
    Resource uuids;

    public static void main(String[] args) {
        SpringApplication.run(App.class);
    }

    @Override
    public void run(String... args) throws Exception {

        Timer fetchViaRest = registry.timer("fetch", "parquet", "duration");

        PoolingHttpClientConnectionManager poolingConnManager = new PoolingHttpClientConnectionManager();
        poolingConnManager.setMaxTotal(50);
        poolingConnManager.setDefaultMaxPerRoute(30);

        // httpclient load data from api and convert to parquet
        final CloseableHttpClient client = HttpClients.custom().setConnectionManager(poolingConnManager).build();

        ExecutorService runner = Executors.newFixedThreadPool(5, new ThreadFactoryBuilder().setNameFormat(
            "client-data-request-%d").build());
        List<String> ids = Files.readAllLines(Path.of(uuids.getURI()));
        CountDownLatch writeLatch = new CountDownLatch(ids.size());

        long loadStart = System.currentTimeMillis();

        ids.forEach(id -> {
            runner.submit(fetchViaRest.wrap(() -> {
                final HttpGet request = new HttpGet("http://localhost:8081/pq/" + id);

                try (CloseableHttpResponse resp = client.execute(request)) {
                    // get bytes
                    final byte[] parquet = IOUtils.toByteArray(resp.getEntity().getContent());

                    try {
                        if (parquet != null) {
                            ParquetReader<PQ> reader = AvroParquetReader.<PQ>builder(new ParquetBufferReader(parquet))
                                .withCompatibility(false).withDataModel(new ReflectData(PQ.class.getClassLoader()))
                                .withConf(new Configuration()).build();
                            PQ pq;

                            while ((pq = reader.read()) != null) {
                                LOG.debug("PQ: {}", pq);
                            }
                            reader.close();
                            writeLatch.countDown();

                            if (writeLatch.getCount() % 1000 == 0) {
                                LOG.info("{} remains", writeLatch.getCount());
                            }
                        }

                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                } catch (IOException e) {
                    LOG.error("CAUGHT EXCEPTION: ", e);
                }
            }));
        });

        writeLatch.await();

        LOG.info("Loading finished, COUNT: {}, TOTAL-Time: {} s, MAX: {} s, AVG: {} s, Duration: {} s",
                 fetchViaRest.count(),
                 fetchViaRest.totalTime(
                     TimeUnit.SECONDS), fetchViaRest.max(TimeUnit.SECONDS),
                 fetchViaRest.mean(TimeUnit.SECONDS), Duration.of(System.currentTimeMillis() - loadStart,
                                                                  ChronoUnit.MILLIS).toSeconds());


    }
}

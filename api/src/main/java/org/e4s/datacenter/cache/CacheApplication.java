package org.e4s.datacenter.cache;

import static org.apache.parquet.hadoop.ParquetFileWriter.Mode.OVERWRITE;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.LZ4_RAW;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PreDestroy;
import java.io.ByteArrayOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.e4s.model.PQ;
import org.e4s.service.parquet.ParquetBufferWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.io.Resource;

@SpringBootApplication
public class CacheApplication implements CommandLineRunner {

    private static final Logger LOG = LoggerFactory.getLogger("CACHE-LOADER");


    @Autowired
    private HazelcastInstance hzClient;

    @Autowired
    private List<UUID> keys;


    @Autowired
    private MeterRegistry registry;


    @Value("classpath:${spring.profiles.active}_v4_uuids.txt")
    Resource uuids;


    public static void main(String[] args) {
        SpringApplication.run(CacheApplication.class, args);
    }


    @PreDestroy
    public void onExit() {
        // cleanup
        IMap<UUID, byte[]> pqCache = hzClient.getMap("pq");
        pqCache.destroy();
    }


    @Override
    public void run(String... args) throws Exception {

        Timer compressionDurationTimer = registry.timer("compression", "parquet",
                                                        "duration");

        Timer readTimer = registry.timer("read", "parquet",
                                         "duration");

        IMap<UUID, byte[]> pqCache = hzClient.getMap("pq");

        // start executor

        ExecutorService runner = Executors.newFixedThreadPool(8,
                                                              new ThreadFactoryBuilder().setNameFormat(
                                                                  "data-loader-%d").build());
        List<String> ids = Files.readAllLines(Path.of(uuids.getURI()));

        long writeStart = System.currentTimeMillis();

        CountDownLatch writeLatch = new CountDownLatch(ids.size());
        long startTs = System.currentTimeMillis();
        final Random r = new Random();
        final float max = 255;
        final float min = 100;

        IntStream.range(0, ids.size()).forEachOrdered(num -> {

            UUID id = UUID.fromString(ids.get(num));
            runner.submit(compressionDurationTimer.wrap(() -> {
                try {

                    keys.add(id);

                    final ByteArrayOutputStream out = new ByteArrayOutputStream();
                    ParquetWriter<PQ> parquetWriter = AvroParquetWriter.<PQ>builder(new ParquetBufferWriter(out))
                        .withSchema(ReflectData.AllowNull.get().getSchema(PQ.class))
                        .withDataModel(ReflectData.get())
                        .withConf(new Configuration())
                        .withCompressionCodec(LZ4_RAW)
                        .withWriteMode(OVERWRITE)
                        .build();

                    for (int i = 0; i < 6048; i++) {

                        PQ pq = new PQ(ids.get(num), startTs + i * (1000 * 60 * 5));

                        pq.setVoltageA(min + r.nextFloat() * (max - min));
                        pq.setVoltageB(min + r.nextFloat() * (max - min));
                        pq.setVoltageC(min + r.nextFloat() * (max - min));

                        pq.setActivePowerA(min + r.nextFloat() * (max - min));
                        pq.setActivePowerB(min + r.nextFloat() * (max - min));
                        pq.setActivePowerC(min + r.nextFloat() * (max - min));

                        pq.setInactivePowerA(min + r.nextFloat() * (max - min));
                        pq.setInactivePowerB(min + r.nextFloat() * (max - min));
                        pq.setInactivePowerC(min + r.nextFloat() * (max - min));
                        pq.setHistogram(new int[]{0, 1, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0});

                        parquetWriter.write(pq);
                    }

                    parquetWriter.close();
                    // put data onto IMap
                    pqCache.put(id, out.toByteArray());
                    out.close();
                    writeLatch.countDown();
                    if (writeLatch.getCount() % 1000 == 0) {
                        LOG.info("{} remains", writeLatch.getCount());
                    }

                } catch (Exception ex) {
                    LOG.error("CAUGHT EXCEPTION:", ex);
                }
            }));
        });

        writeLatch.await();

        LOG.info("Loading finished, COUNT: {}, TOTAL-Time: {} s, MAX: {} s, AVG: {} s, Duration: {} s",
                 compressionDurationTimer.count(),
                 compressionDurationTimer.totalTime(
                     TimeUnit.SECONDS), compressionDurationTimer.max(TimeUnit.SECONDS),
                 compressionDurationTimer.mean(TimeUnit.SECONDS), Duration.of(System.currentTimeMillis() - writeStart,
                                                                              ChronoUnit.MILLIS).toSeconds());
    }
}

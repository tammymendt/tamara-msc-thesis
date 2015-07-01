package eu.stratosphere.procrustes.experiments.operator.stats;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.operatorstatistics.OperatorStatistics;
import org.apache.flink.contrib.operatorstatistics.OperatorStatisticsAccumulator;
import org.apache.flink.contrib.operatorstatistics.OperatorStatisticsConfig;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class HeavyHitter {


    private static final Logger LOG = LoggerFactory.getLogger(Processing.class);

    private static final String ACCUMULATOR_NAME = "op-stats";

    private static final String[] ALGORITHMS = {"lossy", "countmin"};

    public static void main(String[] args) throws Exception {

        if (!parseParameters(args)) {
            return;
        }

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<Integer, Integer>> elementFrequencies =
                env.readTextFile(inputPath).flatMap(new CountInt(configOperatorStatistics())).groupBy(0).sum(1);
        Collection collection = elementFrequencies.filter(new CountGreaterThan(minFrequency)).collect();

        LOG.info("\nActual heavy hitters: (min frequency -> " + minFrequency + ")");
        System.out.println("\nActual heavy hitters: (min frequency -> " + minFrequency + ")");
        for (Object tuple : collection) {
            LOG.info(tuple.toString());
            System.out.println(tuple.toString());
        }

        JobExecutionResult result = env.getLastJobExecutionResult();

        OperatorStatistics globalStats = result.getAccumulatorResult(ACCUMULATOR_NAME);
        LOG.info("\nGlobal Stats: " + globalStats.toString());
        System.out.println("\nGlobal Stats: " + globalStats.toString());

        LOG.info("\nReal Frequencies for detected heavy hitters: ");
        System.out.println("\nReal Frequencies for detected heavy hitters: ");
        collection = elementFrequencies.filter(new HeavyHitters(globalStats.getHeavyHitters())).collect();
        for (Object tuple : collection) {
            LOG.info(tuple.toString());
            System.out.println(tuple.toString());
        }

    }

    // *************************************************************************
    // USER FUNCTIONS
    // *************************************************************************

    public static class CountInt extends RichFlatMapFunction<String, Tuple2<Integer, Integer>> {

        OperatorStatisticsConfig operatorStatisticsConfig;

        Accumulator<Object, Serializable> globalAccumulator;

        Accumulator<Object, Serializable> localAccumulator;

        public CountInt(OperatorStatisticsConfig config) {
            operatorStatisticsConfig = config;
        }

        @Override
        public void open(Configuration parameters) {

            globalAccumulator = getRuntimeContext().getAccumulator(ACCUMULATOR_NAME);
            if (globalAccumulator == null) {
                getRuntimeContext().addAccumulator(ACCUMULATOR_NAME, new OperatorStatisticsAccumulator(operatorStatisticsConfig));
                globalAccumulator = getRuntimeContext().getAccumulator(ACCUMULATOR_NAME);
            }

            int subTaskIndex = getRuntimeContext().getIndexOfThisSubtask();
            localAccumulator = getRuntimeContext().getAccumulator(ACCUMULATOR_NAME + "-" + subTaskIndex);
            if (localAccumulator == null) {
                getRuntimeContext().addAccumulator(ACCUMULATOR_NAME + "-" + subTaskIndex, new OperatorStatisticsAccumulator(operatorStatisticsConfig));
                localAccumulator = getRuntimeContext().getAccumulator(ACCUMULATOR_NAME + "-" + subTaskIndex);
            }
        }

        @Override
        public void flatMap(String value, Collector<Tuple2<Integer, Integer>> out) throws Exception {
            int intValue;
            try {
                intValue = Integer.parseInt(value);
                localAccumulator.add(intValue);
                out.collect(new Tuple2(intValue, 1));
            } catch (NumberFormatException ex) {
                LOG.warn("Number format exception: " + value);
            }
        }

        @Override
        public void close() {
            globalAccumulator.merge(localAccumulator);
        }
    }

    public static class HeavyHitters implements FilterFunction<Tuple2<Integer, Integer>> {

        private Map<Object, Long> hitters;

        public HeavyHitters(Map hitters) {
            this.hitters = hitters;
        }

        @Override
        public boolean filter(Tuple2<Integer, Integer> tuple) {
            return hitters.containsKey(tuple.f0);
        }
    }

    public static class CountGreaterThan implements FilterFunction<Tuple2<Integer, Integer>> {

        long boundary = 0;

        public CountGreaterThan(long b) {
            boundary = b;
        }

        @Override
        public boolean filter(Tuple2<Integer, Integer> tuple) {
            return tuple.f1 > boundary;
        }
    }

    // *************************************************************************
    // UTIL METHODS
    // *************************************************************************

    private static String inputPath;

    private static String algorithm;

    private static long cardinality;

    private static double fraction = 0.00125;

    private static double error = 0.0001;

    public static long minFrequency;

    private static boolean parseParameters(String[] args) {

        if (args.length >= 3) {
            inputPath = args[0];
            algorithm = args[1];
            if (!Arrays.asList(ALGORITHMS).contains(algorithm)) {
                LOG.error("Not a valid algorithm. Algorithm options: lossy, countmin");
                System.out.println("Not a valid algorithm. Algorithm options: lossy, countmin");
                return false;
            }

            cardinality = Long.parseLong(args[2]);

            if (args.length >= 5) {
                if (args[3].equals("-fraction")) {
                    fraction = Double.parseDouble(args[4]);
                } else if (args[3].equals("-error")) {
                    error = Double.parseDouble(args[4]);
                }
            }
            if (args.length >= 7) {
                if (args[3].equals("-fraction")) {
                    fraction = Double.parseDouble(args[4]);
                } else if (args[3].equals("-error")) {
                    error = Double.parseDouble(args[4]);
                }
            }

            if (error < 0 || error > 1 || fraction < 0 || fraction > 1) {
                LOG.error("Error and fraction must be between 0 and 1");
                System.out.println("Error and fraction must be between 0 and 1");
                return false;
            }
            if (!(fraction > error)) {
                LOG.error("Fraction must be larger than error.");
                System.out.println("Fraction must be larger than error.");
                return false;
            }
            minFrequency = Math.round(cardinality * (fraction - error));
            return true;

        } else {
            LOG.error("Usage: OperatorStatistics <input path> <algorithm> <cardinality> <fraction> [<error>]");
            LOG.error("Algorithm options: lossy, countmin");
            LOG.error("0 < fraction < 1");
            System.out.println("Usage: OperatorStatistics <input path> <algorithm> <cardinality> <fraction> [<error>]");
            System.out.println("Algorithm options: lossy, countmin");
            System.out.println("0 < fraction < 1");
            return false;
        }
    }

    private static OperatorStatisticsConfig configOperatorStatistics() {

        OperatorStatisticsConfig opStatsConfig = new OperatorStatisticsConfig(false);
        System.out.println("Parsing configuration parameter");
        if (algorithm.equals("lossy")) {
            opStatsConfig.collectHeavyHitters = true;
            opStatsConfig.heavyHitterAlgorithm = OperatorStatisticsConfig.HeavyHitterAlgorithm.LOSSY_COUNTING;
        } else if (algorithm.equals("countmin")) {
            opStatsConfig.collectHeavyHitters = true;
            opStatsConfig.heavyHitterAlgorithm = OperatorStatisticsConfig.HeavyHitterAlgorithm.COUNT_MIN_SKETCH;
            opStatsConfig.setHeavyHitterConfidence(0.99);
        }
        opStatsConfig.setHeavyHitterFraction(fraction);
        opStatsConfig.setHeavyHitterFraction(error);
        return opStatsConfig;
    }
}

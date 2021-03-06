package eu.stratosphere.procrustes.experiments.operator.stats;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
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

public class FlatMap {

    private static final Logger LOG = LoggerFactory.getLogger(Processing.class);

    private static final String ACCUMULATOR_NAME = "op-stats";

    private static final String[] ALGORITHMS = {"none", "counter", "minmax", "lossy", "countmin", "hyperloglog", "linearc", "all"};

    public static void main(String[] args) throws Exception {

        if (!parseParameters(args)) {
            return;
        }

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        if (algorithm.equals("none")) {
            env.readTextFile(inputPath).flatMap(new CountInt()).filter(new MultipleOfFive()).output(new DiscardingOutputFormat());
        } else if (algorithm.equals("counter")) {
            env.readTextFile(inputPath).flatMap(new CountIntAccumulator()).filter(new MultipleOfFive()).output(new DiscardingOutputFormat());
        } else {
            env.readTextFile(inputPath)
               .flatMap(new CountIntOpStatsAccumulator(configOperatorStatistics()))
               .filter(new MultipleOfFive())
               .output(new DiscardingOutputFormat());
        }

        JobExecutionResult result = env.execute("FlatMap Experiment");

        if (algorithm.equals("none")) {
            LOG.info("\nJob finished. No stats collected.");
            System.out.println("\nJob finished. No stats collected.");
        } else if (algorithm.equals("counter")) {
            int cardinality = result.getIntCounterResult(ACCUMULATOR_NAME);
            LOG.info("\nTotal Cardinality: " + cardinality);
            System.out.println("\nTotal Cardinality: " + cardinality);
        } else {
            OperatorStatistics globalStats = result.getAccumulatorResult(ACCUMULATOR_NAME);
            LOG.info("\nGlobal Stats: " + globalStats.toString());
            System.out.println("\nGlobal Stats: " + globalStats.toString());
        }

    }

    // *************************************************************************
    // USER FUNCTIONS
    // *************************************************************************

    public static class CountInt implements FlatMapFunction<String, Tuple2<Integer, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<Integer, Integer>> out) throws Exception {
            int intValue;
            try {
                intValue = Integer.parseInt(value);
                out.collect(new Tuple2(intValue, 1));
            } catch (NumberFormatException ex) {}
        }
    }

    public static class CountIntAccumulator extends RichFlatMapFunction<String, Tuple2<Integer, Integer>> {

        IntCounter accumulator;

        @Override
        public void open(Configuration parameters) {

            accumulator = getRuntimeContext().getIntCounter(ACCUMULATOR_NAME);
        }

        @Override
        public void flatMap(String value, Collector<Tuple2<Integer, Integer>> out) throws Exception {
            int intValue;
            try {
                intValue = Integer.parseInt(value);
                accumulator.add(1);
                out.collect(new Tuple2(intValue, 1));
            } catch (NumberFormatException ex) {}
        }
    }

    public static class CountIntOpStatsAccumulator extends RichFlatMapFunction<String, Tuple2<Integer, Integer>> {

        OperatorStatisticsConfig operatorStatisticsConfig;

        Accumulator<Object, Serializable> accumulator;

        public CountIntOpStatsAccumulator(OperatorStatisticsConfig config) {
            operatorStatisticsConfig = config;
        }

        @Override
        public void open(Configuration parameters) {

            accumulator = getRuntimeContext().getAccumulator(ACCUMULATOR_NAME);
            if (accumulator == null) {
                getRuntimeContext().addAccumulator(ACCUMULATOR_NAME, new OperatorStatisticsAccumulator(operatorStatisticsConfig));
                accumulator = getRuntimeContext().getAccumulator(ACCUMULATOR_NAME);
            }
        }

        @Override
        public void flatMap(String value, Collector<Tuple2<Integer, Integer>> out) throws Exception {
            int intValue;
            try {
                intValue = Integer.parseInt(value);
                accumulator.add(intValue);
                out.collect(new Tuple2(intValue, 1));
            } catch (NumberFormatException ex) {}
        }
    }

    public static class MultipleOfFive implements FilterFunction<Tuple2<Integer, Integer>> {

        @Override
        public boolean filter(Tuple2<Integer, Integer> tuple) {
            return (tuple.f0 % 5 == 0);
        }
    }

    // *************************************************************************
    // UTIL METHODS
    // *************************************************************************

    private static String inputPath;

    private static String algorithm;

    private static int log2m = 10;

    private static int bitmap = 100000000;

    private static double fraction = 0.00125;

    private static double error = 0.0001;

    private static boolean parseParameters(String[] args) {

        // parse input arguments
        if (args.length >= 2) {
            inputPath = args[0];
            algorithm = args[1];
            if (!Arrays.asList(ALGORITHMS).contains(algorithm)) {
                return false;
            }
            if (args.length >= 4) {
                if (args[2].equals("-log2m")) {
                    log2m = Integer.parseInt(args[3]);
                } else if (args[2].equals("-bitmap")) {
                    bitmap = Integer.parseInt(args[3]);
                } else if (args[2].equals("-fraction")) {
                    fraction = Double.parseDouble(args[3]);
                } else if (args[2].equals("-error")) {
                    error = Double.parseDouble(args[3]);
                } else {
                    return false;
                }
            }
            if (args.length >= 6) {
                if (args[4].equals("-fraction")) {
                    fraction = Double.parseDouble(args[5]);
                } else if (args[4].equals("-error")) {
                    error = Double.parseDouble(args[5]);
                } else {
                    return false;
                }
            }
            return true;
        } else {
            LOG.error("Usage: OperatorStatistics <input path> <algorithm> [-log2m|-bitmap|-error|-fraction <algorithm params>]");
            LOG.error("Algorithm options: none, minmax, lossy, countmin, hyperloglog, linearc");
            System.out.println("Usage: OperatorStatistics <input path> <algorithm> [-log2m|-bitmap|-error|-fraction <algorithm params>]");
            System.out.println("Algorithm options: none, minmax, lossy, countmin, hyperloglog, linearc");
            return false;
        }
    }

    private static OperatorStatisticsConfig configOperatorStatistics() {

        OperatorStatisticsConfig opStatsConfig = new OperatorStatisticsConfig(false);
        System.out.println("Parsing configuration parameter");
        if (algorithm.equals("minmax")) {
            opStatsConfig.collectMin = true;
            opStatsConfig.collectMax = true;
        } else if (algorithm.equals("hyperloglog")) {
            opStatsConfig.collectCountDistinct = true;
            opStatsConfig.countDistinctAlgorithm = OperatorStatisticsConfig.CountDistinctAlgorithm.HYPERLOGLOG;
            opStatsConfig.setCountDlog2m(log2m);
        } else if (algorithm.equals("linearc")) {
            opStatsConfig.collectCountDistinct = true;
            opStatsConfig.countDistinctAlgorithm = OperatorStatisticsConfig.CountDistinctAlgorithm.LINEAR_COUNTING;
            opStatsConfig.setCountDbitmap(bitmap);
        } else if (algorithm.equals("lossy")) {
            opStatsConfig.collectHeavyHitters = true;
            opStatsConfig.heavyHitterAlgorithm = OperatorStatisticsConfig.HeavyHitterAlgorithm.LOSSY_COUNTING;
            opStatsConfig.setHeavyHitterError(error);
            opStatsConfig.setHeavyHitterFraction(fraction);
        } else if (algorithm.equals("countmin")) {
            opStatsConfig.collectHeavyHitters = true;
            opStatsConfig.heavyHitterAlgorithm = OperatorStatisticsConfig.HeavyHitterAlgorithm.COUNT_MIN_SKETCH;
            opStatsConfig.setHeavyHitterError(error);
            opStatsConfig.setHeavyHitterFraction(fraction);
        } else if (algorithm.equals("all")) {
            opStatsConfig = new OperatorStatisticsConfig(true);
            opStatsConfig.countDistinctAlgorithm = OperatorStatisticsConfig.CountDistinctAlgorithm.HYPERLOGLOG;
            opStatsConfig.heavyHitterAlgorithm = OperatorStatisticsConfig.HeavyHitterAlgorithm.LOSSY_COUNTING;
            opStatsConfig.setCountDlog2m(log2m);
            opStatsConfig.setHeavyHitterError(error);
            opStatsConfig.setHeavyHitterFraction(fraction);
        }
        return opStatsConfig;
    }
}

package eu.stratosphere.procrustes.experiments.operator.stats;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.Histogram;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.*;
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
import java.util.TreeMap;

public class FlatMap {

    private static final Logger LOG = LoggerFactory.getLogger(FlatMap.class);

    private static final String ACCUMULATOR_NAME = "op-stats";

    private static final String[] ALGORITHMS = {"none", "counter", "minmax", "lossy", "countmin", "hyperloglog", "linearc", "all", "histogram"};

    public static void main(String[] args) throws Exception {

        if (!parseParameters(args)) {
            LOG.error("Usage: OperatorStatistics <input path> <algorithm> [--log2m|--bitmap|--error|--fraction|--confidence <param value>]");
            LOG.error("Algorithm options: none, minmax, lossy, countmin, hyperloglog, linearc, counter, histogram, all");
            System.out.println("Usage: OperatorStatistics <input path> <algorithm> [--log2m|--bitmap|--error|--fraction|--confidence <param value>]");
            System.out.println("Algorithm options: none, minmax, lossy, countmin, hyperloglog, linearc, counter, histogram, all");
            return;
        }

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        if (algorithm.equals("none")) {
            env.readTextFile(inputPath).map(new CountInt()).filter(new MultipleOfFive()).output(new DiscardingOutputFormat());
        } else if (algorithm.equals("counter")) {
            env.readTextFile(inputPath).map(new CountLongAccumulator()).filter(new MultipleOfFive()).output(new DiscardingOutputFormat());
        } else if (algorithm.equals("histogram")) {
            env.readTextFile(inputPath).map(new CountHistogramAccumulator()).filter(new MultipleOfFive()).output(new DiscardingOutputFormat());
        } else {
            env.readTextFile(inputPath)
               .map(new CountIntOpStatsAccumulator(configOperatorStatistics()))
               .filter(new MultipleOfFive())
               .output(new DiscardingOutputFormat());
        }

        JobExecutionResult result = env.execute("FlatMap Experiment");

        if (algorithm.equals("none")) {
            LOG.info("\nJob finished. No stats collected.");
            System.out.println("\nJob finished. No stats collected.");
        } else if (algorithm.equals("counter")) {
            long cardinality = result.getAccumulatorResult(ACCUMULATOR_NAME);
            LOG.info("\nTotal Cardinality: " + cardinality);
            System.out.println("\nTotal Cardinality: " + cardinality);
        } else if (algorithm.equals("histogram")) {
            TreeMap<Integer, Integer> histogram = result.getAccumulatorResult(ACCUMULATOR_NAME);
            LOG.info("\nHistogram Result: " + histogram.toString());
            System.out.println("\nHistogram Result: " + histogram.toString());
        } else {
            OperatorStatistics globalStats = result.getAccumulatorResult(ACCUMULATOR_NAME);
            LOG.info("\nGlobal Stats: " + globalStats.toString());
            System.out.println("\nGlobal Stats: " + globalStats.toString());
        }

    }

    // *************************************************************************
    // USER FUNCTIONS
    // *************************************************************************

    public static class CountInt implements MapFunction<String, Tuple2<Integer, Integer>> {

        @Override
        public Tuple2<Integer,Integer> map(String value) throws Exception {
            return new Tuple2<>(Integer.parseInt(value),1);
        }
    }

    public static class CountHistogramAccumulator extends RichMapFunction<String, Tuple2<Integer, Integer>> {

        Histogram accumulator;

        @Override
        public void open(Configuration parameters) {

            accumulator = getRuntimeContext().getHistogram(ACCUMULATOR_NAME);
        }

        @Override
        public Tuple2<Integer,Integer> map(String value) throws Exception {
            int intValue = Integer.parseInt(value);
            accumulator.add(intValue);
             return new Tuple2(intValue, 1);
        }
    }

    public static class CountLongAccumulator extends RichMapFunction<String, Tuple2<Integer, Integer>> {

        LongCounter accumulator;

        @Override
        public void open(Configuration parameters) {

            accumulator = getRuntimeContext().getLongCounter(ACCUMULATOR_NAME);
        }

        @Override
        public Tuple2<Integer,Integer> map(String value) throws Exception {
            int intValue = Integer.parseInt(value);
            accumulator.add(1L);
            return new Tuple2(intValue, 1);
        }
    }

    public static class CountIntOpStatsAccumulator extends RichMapFunction<String, Tuple2<Integer, Integer>> {

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
        public Tuple2<Integer,Integer> map(String value) throws Exception {
            int intValue = Integer.parseInt(value);
            accumulator.add(intValue);
            return new Tuple2(intValue, 1);
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

    private static int bitmap = 1000000;

    private static double fraction = 0.05;

    private static double error = 0.0005;

    private static double confidence = 0.99;

    private static boolean parseParameters(String[] args) {

        // parse input arguments
        if (args.length >= 2) {
            inputPath = args[0];
            algorithm = args[1];
            if (!Arrays.asList(ALGORITHMS).contains(algorithm)) {
                return false;
            }
        }
        if (args.length>2){
            if (args.length % 2 == 0) {
                for (int i = 2; i < args.length - 1; i += 2) {
                    if (!parseParameters(args[i], args[i + 1])) {
                        return false;
                    }
                }
                return true;
            }else {
                return false;
            }
        }else {
            return true;
        }
    }

    private static boolean parseParameters(String p1,String p2){
        switch(p1){
            case "--log2m":
                log2m = Integer.parseInt(p2);
                return true;
            case "--bitmap":
                bitmap = Integer.parseInt(p2);
                return true;
            case "--fraction":
                fraction = Double.parseDouble(p2);
                return true;
            case "--error":
                error = Double.parseDouble(p2);
                return true;
            case "--confidence":
                confidence = Double.parseDouble(p2);
                return true;
            default:
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
            opStatsConfig.setHeavyHitterConfidence(confidence);
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

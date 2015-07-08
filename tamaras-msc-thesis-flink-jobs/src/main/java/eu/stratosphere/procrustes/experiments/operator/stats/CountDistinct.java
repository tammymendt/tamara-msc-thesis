package eu.stratosphere.procrustes.experiments.operator.stats;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.functions.FlatMapFunction;
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

public class CountDistinct {

    private static final Logger LOG = LoggerFactory.getLogger(CountDistinct.class);

    public static void main(String[] args) throws Exception {

        if (!parseParameters(args)) {
            LOG.error("Usage: OperatorStatistics <input path>");
            System.out.println("Usage: OperatorStatistics <input path>");
            return;
        }

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<Integer, Integer>> distinctElements = env.readTextFile(inputPath).flatMap(new CountInt()).distinct();

        long countDistinct = distinctElements.count();

        LOG.info("\nReal Count Distinct: " + countDistinct);
        System.out.println("\nReal Count Distinct: " + countDistinct);
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


    // *************************************************************************
    // UTIL METHODS
    // *************************************************************************

    private static String inputPath;

    private static boolean parseParameters(String[] args) {

        if (args.length == 1) {
            inputPath = args[0];
            return true;
        } else {
            return false;
        }
    }
}

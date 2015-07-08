package eu.stratosphere.procrustes.experiments.operator.stats;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple3;

import java.util.Collection;

public class HeavyHitter {


    private static final Logger LOG = LoggerFactory.getLogger(HeavyHitter.class);

    public static void main(String[] args) throws Exception {

        if (!parseParameters(args)) {
            LOG.error("Usage: OperatorStatistics <input path> <parameters>");
            LOG.error("parameters: --cardinality, --fraction, --error");
            System.out.println("Usage: OperatorStatistics <input path> <parameters>");
            System.out.println("parameters: --cardinality, --fraction, --error");
            return;
        }

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple3<Integer, Integer,String>> elementFrequencies =
                env.readTextFile(inputPath).flatMap(new CountInt())
                        .groupBy(0).sum(1).flatMap(new CheckFrequency(frequencyLowerBound,frequency));
        Collection collection = elementFrequencies.collect();

        LOG.info("\nActual heavy hitters: (min frequency -> " + frequencyLowerBound + ", frequency -> "+frequency+")");
        System.out.println("\nActual heavy hitters: (min frequency -> " + frequencyLowerBound + ", fyequency -> "+frequency+")");
        for (Object tuple : collection) {
            LOG.info(tuple.toString());
            System.out.println(tuple.toString());
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
            } catch (NumberFormatException ex) {
                LOG.warn("Number format exception: " + value);
            }
        }
    }

    public static class CheckFrequency implements FlatMapFunction<Tuple2<Integer, Integer>, Tuple3<Integer,Integer,String>> {

        long frequencyLowerBound = 0;
        long frequency = 0;

        public CheckFrequency(long frequencyLowerBound,long frequency) {
            this.frequencyLowerBound = frequencyLowerBound;
            this.frequency = frequency;
        }

        @Override
        public void flatMap(Tuple2<Integer, Integer> value, Collector<Tuple3<Integer, Integer, String>> out) throws Exception {
            if (value.f1>=frequency){
                out.collect(new Tuple3(value.f0,value.f1,"True Frequent"));
            }else if (value.f1>=frequencyLowerBound){
                out.collect(new Tuple3(value.f0,value.f1,"Above min frequency threshold"));
            }
        }
    }

    // *************************************************************************
    // UTIL METHODS
    // *************************************************************************

    private static String inputPath;

    private static long cardinality;

    private static double fraction;

    private static double error;

    public static long frequencyLowerBound;
    public static long frequency;

    private static boolean parseParameters(String[] args) {

        if (args.length == 7) {
            inputPath = args[0];
            for (int i=0;i<3;i++){
                if (!parseParameters(args[i*2+1],args[i*2+2])){
                    return false;
                };
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
            frequencyLowerBound = Math.round(cardinality * (fraction - error));
            frequency = Math.round(cardinality * fraction);
            return true;
        } else {
            return false;
        }
    }

    private static boolean parseParameters(String p1, String p2){
        switch(p1){
            case "--cardinality":
                cardinality = Long.parseLong(p2);
                return true;
            case "--fraction":
                fraction = Double.parseDouble(p2);
                return true;
            case "--error":
                error = Double.parseDouble(p2);
                return true;
            default:
                return false;
        }
    }

}

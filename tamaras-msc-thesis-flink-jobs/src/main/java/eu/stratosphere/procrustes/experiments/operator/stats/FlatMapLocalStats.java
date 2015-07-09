package eu.stratosphere.procrustes.experiments.operator.stats;

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.operatorstatistics.OperatorStatisticsAccumulator;
import org.apache.flink.contrib.operatorstatistics.OperatorStatisticsConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;

public class FlatMapLocalStats {

	private static final Logger LOG = LoggerFactory.getLogger(HeavyHitter.class);
	private static final String ACCUMULATOR_NAME = "op-stats";
	private static final String[] ALGORITHMS = {"lossy", "countmin"};

	public static void main(String[] args) throws Exception {

		if (!parseParameters(args)) {
			LOG.error("Usage: OperatorStatistics <input path> <parameters>");
			LOG.error("parameters: --fraction, --error");
			System.out.println("Usage: OperatorStatistics <input path> <parameters>");
			System.out.println("parameters: --fraction, --error");
			return;
		}

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		env.readTextFile(inputPath).map(new CountInt(configOperatorStatistics())).output(new DiscardingOutputFormat());;

	}

	// *************************************************************************
	// USER FUNCTIONS
	// *************************************************************************

	public static class CountInt extends RichMapFunction<String, Tuple2<Integer, Long>> {

		private OperatorStatisticsConfig opStatsConfig;
		Accumulator<Object, Serializable> globalAccumulator;
		Accumulator<Object, Serializable> localAccumulator;

		public CountInt(OperatorStatisticsConfig opStatsConfig){
			this.opStatsConfig = opStatsConfig;
		}

		@Override
		public void open(Configuration parameters) {

			RuntimeContext context = getRuntimeContext();

			globalAccumulator = context.getAccumulator(ACCUMULATOR_NAME);
			if (globalAccumulator == null) {
				getRuntimeContext().addAccumulator(ACCUMULATOR_NAME, new OperatorStatisticsAccumulator(opStatsConfig));
				globalAccumulator = getRuntimeContext().getAccumulator(ACCUMULATOR_NAME);
			}

			localAccumulator = context.getAccumulator(ACCUMULATOR_NAME+context.getIndexOfThisSubtask());
			if (localAccumulator == null) {
				getRuntimeContext().addAccumulator(ACCUMULATOR_NAME, new OperatorStatisticsAccumulator(opStatsConfig));
				localAccumulator = getRuntimeContext().getAccumulator(ACCUMULATOR_NAME+context.getIndexOfThisSubtask());
			}
		}


		@Override
		public Tuple2<Integer,Long> map(String value) throws Exception {
			int intValue;
			try {
				intValue = Integer.parseInt(value);
				localAccumulator.add(intValue);
				return new Tuple2(intValue, 1);
			} catch (NumberFormatException ex) {
				LOG.warn("Number format exception: " + value);
				throw ex;
			}
		}

		@Override
		public void close(){
			globalAccumulator.merge(localAccumulator);
		}
	}

	// *************************************************************************
	// UTIL METHODS
	// *************************************************************************

	private static String inputPath;

	private static String algorithm;

	private static double fraction;

	private static double error;

	private static boolean parseParameters(String[] args) {

		if (args.length == 6) {
			inputPath = args[0];
			algorithm = args[1];

			if (!Arrays.asList(ALGORITHMS).contains(algorithm)) {
				return false;
			}

			for (int i=0;i<2;i++){
				if (!parseParameters(args[i*2+1],args[i*2+2])){
					return false;
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
			return true;
		} else {
			return false;
		}
	}

	private static boolean parseParameters(String p1, String p2){
		switch(p1){
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

	private static OperatorStatisticsConfig configOperatorStatistics() {

		System.out.println("Parsing configuration parameter");
		OperatorStatisticsConfig opStatsConfig = new OperatorStatisticsConfig(false);
		opStatsConfig.collectHeavyHitters = true;
		opStatsConfig.setHeavyHitterError(error);
		opStatsConfig.setHeavyHitterFraction(fraction);

		if (algorithm.equals("lossy")) {
			opStatsConfig.heavyHitterAlgorithm = OperatorStatisticsConfig.HeavyHitterAlgorithm.LOSSY_COUNTING;
		} else if (algorithm.equals("countmin")) {
			opStatsConfig.heavyHitterAlgorithm = OperatorStatisticsConfig.HeavyHitterAlgorithm.COUNT_MIN_SKETCH;
		}
		return opStatsConfig;
	}
}


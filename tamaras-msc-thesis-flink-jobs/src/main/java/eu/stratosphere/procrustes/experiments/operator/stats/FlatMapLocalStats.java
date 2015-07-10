package eu.stratosphere.procrustes.experiments.operator.stats;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.Histogram;
import org.apache.flink.api.common.accumulators.LongCounter;
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

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;

public class FlatMapLocalStats {

	private static final Logger LOG = LoggerFactory.getLogger(HeavyHitter.class);
	private static final String ACCUMULATOR_NAME = "op-stats";
	private static final String[] ALGORITHMS = {"lossy", "countmin", "histogram"};

	public static void main(String[] args) throws Exception {

		if (!parseParameters(args)) {
			LOG.error("Usage: OperatorStatistics <input path> <parameters>");
			LOG.error("parameters: --fraction, --error");
			System.out.println("Usage: OperatorStatistics <input path> <parameters>");
			System.out.println("parameters: --fraction, --error");
			return;
		}

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().disableObjectReuse();

		if (algorithm.equals("histogram")){
			env.readTextFile(inputPath).map(new HistogramCountInt()).output(new DiscardingOutputFormat());
		}else{
			env.readTextFile(inputPath).map(new CountInt(configOperatorStatistics())).output(new DiscardingOutputFormat());
		}

		JobExecutionResult result = env.execute("FlatMap Experiment with Local Stats");

		System.out.println("\nGlobal Stats\n" + result.getAccumulatorResult(ACCUMULATOR_NAME).toString());
		LOG.info("\nGlobal Stats\n" + result.getAccumulatorResult(ACCUMULATOR_NAME).toString());

		Map<String,Object> accResults = result.getAllAccumulatorResults();
		for (String accumulatorName:accResults.keySet()){
			if (accumulatorName.contains(ACCUMULATOR_NAME+"-")){
				System.out.println("\nLocal Stats " + accumulatorName + ":\n" + (accResults.get(accumulatorName)).toString());
				LOG.info("\nLocal Stats " + accumulatorName + ":\n" + (accResults.get(accumulatorName)).toString());
			}
		}

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
			int subTaskIndex = context.getIndexOfThisSubtask();

			globalAccumulator = context.getAccumulator(ACCUMULATOR_NAME);
			if (globalAccumulator == null) {
				globalAccumulator = (Accumulator) new OperatorStatisticsAccumulator(opStatsConfig);
				getRuntimeContext().addAccumulator(ACCUMULATOR_NAME, globalAccumulator);
			}

			localAccumulator = context.getAccumulator(ACCUMULATOR_NAME+"-"+subTaskIndex);
			if (localAccumulator == null) {
				localAccumulator = (Accumulator) new OperatorStatisticsAccumulator(opStatsConfig);
				getRuntimeContext().addAccumulator(ACCUMULATOR_NAME+"-"+subTaskIndex, localAccumulator);
			}
		}


		@Override
		public Tuple2<Integer,Long> map(String value) throws Exception {

			localAccumulator.add(Integer.parseInt(value));
			return new Tuple2(Integer.parseInt(value), 1L);
		}

		@Override
		public void close(){
			globalAccumulator.merge(localAccumulator);
		}
	}

	public static class HistogramCountInt extends RichMapFunction<String, Tuple2<Integer, Long>> {

		Histogram globalAccumulator;
		Histogram localAccumulator;

		@Override
		public void open(Configuration parameters) {

			RuntimeContext context = getRuntimeContext();
			int subTaskIndex = context.getIndexOfThisSubtask();

			globalAccumulator = context.getHistogram(ACCUMULATOR_NAME);
			localAccumulator = context.getHistogram(ACCUMULATOR_NAME + "-" + subTaskIndex);
		}


		@Override
		public Tuple2<Integer,Long> map(String value) throws Exception {
			try {
				localAccumulator.add(Integer.parseInt(value));
				return new Tuple2(Integer.parseInt(value), 1L);
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

	private static double fraction = 0.001;

	private static double error = 0.0001;

	private static boolean parseParameters(String[] args) {

		if (args.length >= 2) {
			inputPath = args[0];
			algorithm = args[1];

			if (!Arrays.asList(ALGORITHMS).contains(algorithm)) {
				return false;
			}

			if (algorithm.equals("histogram")) {
				return true;
			}
		}
		if (args.length==6){
			for (int i=1;i<3;i++){
				if (!parseParameters(args[i*2],args[i*2+1])){
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


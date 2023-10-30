package com.hudi.flink.quickstart;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.data.writer.BinaryWriter;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.util.HoodiePipeline;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * A Flink program that writes data as a streaming source to Apache Hudi.
 */
public class HudiDataStreamWriter {

	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Usage: HudiDataStreamWriter <targetTable> <basePath>");
			System.exit(1);
		}

		String targetTable = args[0];
		String basePath = args[1];

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// Enable checkpointing
		configureCheckpointing(env);

		Map<String, String> options = createHudiOptions(basePath);

		DataStreamSource<RowData> dataStream = env.addSource(new SampleDataSource());
		HoodiePipeline.Builder builder = createHudiPipeline(targetTable, options);

		builder.sink(dataStream, false); // The second parameter indicates whether the input data stream is bounded
		env.execute("Api_Sink");
	}

	/**
	 * Configure Flink checkpointing settings.
	 *
	 * @param env The Flink StreamExecutionEnvironment.
	 */
	private static void configureCheckpointing(StreamExecutionEnvironment env) {
		env.enableCheckpointing(60000); // Checkpoint every 60 seconds
		CheckpointConfig checkpointConfig = env.getCheckpointConfig();
		checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
		checkpointConfig.setMinPauseBetweenCheckpoints(1000); // Minimum time between checkpoints
		checkpointConfig.setCheckpointTimeout(60000); // Checkpoint timeout in milliseconds
		checkpointConfig.setCheckpointStorage("file:///tmp/hudi_flink_checkpoint_2");
	}

	/**
	 * Create Hudi options for the data sink.
	 *
	 * @param basePath The base path for Hudi data.
	 * @return A Map containing Hudi options.
	 */
	private static Map<String, String> createHudiOptions(String basePath) {
		Map<String, String> options = new HashMap<>();
		options.put(FlinkOptions.PATH.key(), basePath);
		options.put(FlinkOptions.TABLE_TYPE.key(), HoodieTableType.MERGE_ON_READ.name());
		options.put(FlinkOptions.PRECOMBINE_FIELD.key(), "ts");
		options.put(FlinkOptions.IGNORE_FAILED.key(), "true");
		return options;
	}

	/**
	 * Create a HudiPipeline.Builder with the specified target table and options.
	 *
	 * @param targetTable The name of the Hudi table.
	 * @param options     The Hudi options for the data sink.
	 * @return A HudiPipeline.Builder.
	 */
	private static HoodiePipeline.Builder createHudiPipeline(String targetTable, Map<String, String> options) {
		return HoodiePipeline.builder(targetTable)
				.column("uuid VARCHAR(20)")
				.column("name VARCHAR(10)")
				.column("age INT")
				.column("ts TIMESTAMP(3)")
				.column("`partition` VARCHAR(20)")
				.pk("uuid")
				.partition("partition")
				.options(options);
	}

	public static List<RowData> DATA_SET_INSERT = Arrays.asList(
			insertRow(StringData.fromString("id1"), StringData.fromString("Danny"), 23,
					TimestampData.fromEpochMillis(1), StringData.fromString("par1")),
			insertRow(StringData.fromString("id2"), StringData.fromString("Stephen"), 33,
					TimestampData.fromEpochMillis(2), StringData.fromString("par1")),
			insertRow(StringData.fromString("id3"), StringData.fromString("Julian"), 53,
					TimestampData.fromEpochMillis(3), StringData.fromString("par2")),
			insertRow(StringData.fromString("id4"), StringData.fromString("Fabian"), 31,
					TimestampData.fromEpochMillis(4), StringData.fromString("par2")),
			insertRow(StringData.fromString("id5"), StringData.fromString("Sophia"), 18,
					TimestampData.fromEpochMillis(5), StringData.fromString("par3")),
			insertRow(StringData.fromString("id6"), StringData.fromString("Emma"), 20,
					TimestampData.fromEpochMillis(6), StringData.fromString("par3")),
			insertRow(StringData.fromString("id7"), StringData.fromString("Bob"), 44,
					TimestampData.fromEpochMillis(7), StringData.fromString("par4")),
			insertRow(StringData.fromString("id8"), StringData.fromString("Han"), 56,
					TimestampData.fromEpochMillis(8), StringData.fromString("par4"))
	);

	public static final DataType ROW_DATA_TYPE = DataTypes.ROW(
					DataTypes.FIELD("uuid", DataTypes.VARCHAR(20)),// record key
					DataTypes.FIELD("name", DataTypes.VARCHAR(10)),
					DataTypes.FIELD("age", DataTypes.INT()),
					DataTypes.FIELD("ts", DataTypes.TIMESTAMP(3)), // precombine field
					DataTypes.FIELD("partition", DataTypes.VARCHAR(10)))
			.notNull();

	public static final RowType ROW_TYPE = (RowType) ROW_DATA_TYPE.getLogicalType();

	public static BinaryRowData insertRow(Object... fields) {
		return insertRow(ROW_TYPE, fields);
	}

	public static BinaryRowData insertRow(RowType rowType, Object... fields) {
		LogicalType[] types = rowType.getFields().stream().map(RowType.RowField::getType)
				.toArray(LogicalType[]::new);
		BinaryRowData row = new BinaryRowData(fields.length);
		BinaryRowWriter writer = new BinaryRowWriter(row);
		writer.reset();
		for (int i = 0; i < fields.length; i++) {
			Object field = fields[i];
			if (field == null) {
				writer.setNullAt(i);
			} else {
				BinaryWriter.write(writer, i, field, types[i], InternalSerializers.create(types[i]));
			}
		}
		writer.complete();
		return row;
	}

	/**
	 * Sample data source for generating RowData objects.
	 */
	static class SampleDataSource implements SourceFunction<RowData> {
		private volatile boolean isRunning = true;
		private final Random random = new Random();

		@Override
		public void run(SourceContext<RowData> ctx) throws Exception {
			while (isRunning) {
				for (RowData row : DATA_SET_INSERT) {
					ctx.collect(row);
				}
				TimeUnit.MILLISECONDS.sleep(1000); // Simulate a delay
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}
	}
}

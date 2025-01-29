import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

public class PathWithNHop {

    // First mapper to construct the initial graph structure
    public static class GraphBuilderMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] nodes = value.toString().split("[\t,\s]+", 2);
            if (nodes.length == 2) { // Create source-to-destination mapping for each node
                context.write(new Text(nodes[0]), new Text(nodes[1]));
            }
        }
    }

    // Reducer for building the initial graph structure
    public static class GraphBuilderReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set<String> neighbors = new HashSet<>();
            for (Text value : values) {
                neighbors.add(value.toString());
            }
            context.write(key, new Text(String.join(",", neighbors))); // Generate neighbor list and pass to the next steps
        }
    }

    // Does not perform any special operation, just forwards the value
    public static class BFSMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("\t", 2);
            if (line.length == 2) {
                context.write(new Text(line[0]), new Text(line[1]));
            }
        }
    }

    public static class BFSReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text currentNode, Iterable<Text> inputValues, Context context) throws IOException, InterruptedException {
            // Maximum hop count from configuration
            int maxHop = context.getConfiguration().getInt("n", 3);
            // Check if this is the last iteration
            boolean isLastIteration = context.getConfiguration().getInt("currentIteration", 0) == maxHop;

            // Set to store neighboring nodes
            Set<String> adjacencySet = new HashSet<>();
            // List to store hop messages
            List<String> hopMessages = new ArrayList<>();

            // Process all values associated with the current node
            for (Text value : inputValues) {
                // Split the input into message components (if any)
                String[] components = value.toString().split("\\|");

                if (components.length == 2) {
                    // If the input is a hop message
                    int hopCount = Integer.parseInt(components[1]) + 1; // Increment hop count
                    if (hopCount == maxHop) {
                        // If the final hop count is reached, record output
                        context.write(new Text(components[0]), currentNode);
                    } else if (!isLastIteration) {
                        // If not the last iteration, store the hop message
                        hopMessages.add(components[0] + "|" + hopCount);
                    }
                } else if (!isLastIteration) {
                    // If the input is a list of neighbors
                    String[] neighbors = value.toString().split(",");
                    for (String neighbor : neighbors) {
                        adjacencySet.add(neighbor); // Add neighbors to the set
                    }
                    // Rewrite neighbor list to maintain graph structure
                    context.write(currentNode, value);
                }
            }

            // Send hop messages to neighboring nodes
            if (!isLastIteration) {
                for (String neighbor : adjacencySet) {
                    if (hopMessages.isEmpty()) {
                        // If no hop messages exist, send an initial hop message with hop count zero
                        context.write(new Text(neighbor), new Text(currentNode + "|" + 0));
                    } else {
                        // Send all hop messages to neighboring nodes
                        for (String hopMessage : hopMessages) {
                            context.write(new Text(neighbor), new Text(hopMessage));
                        }
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        // Check input arguments
        if (args.length != 3) {
            System.err.println("please pass parameters truely: input output steps");
            System.exit(-1);
        }

        // Retrieve input arguments
        String inputPath = args[0]; // Input path
        String outputPath = args[1]; // Final output path
        int maxHop = Integer.parseInt(args[2]); // Final hop count

        // Hadoop configuration
        Configuration configuration = new Configuration();

        // Step 1: Build graph structure
        String initialOutputPath = "output_0"; // Temporary output path for step 1
        Job graphStructureJob = Job.getInstance(configuration, "Graph Structure Builder");
        graphStructureJob.setJarByClass(PathWithNHop.class);
        graphStructureJob.setMapperClass(GraphBuilderMapper.class);
        graphStructureJob.setReducerClass(GraphBuilderReducer.class);
        graphStructureJob.setOutputKeyClass(Text.class);
        graphStructureJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(graphStructureJob, new Path(inputPath));
        FileOutputFormat.setOutputPath(graphStructureJob, new Path(initialOutputPath));

        // Execute graph structure job
        if (!graphStructureJob.waitForCompletion(true)) {
            System.exit(1); // Exit on job failure
        }

        // Step 2: Iterative BFS execution
        for (int iteration = 0; iteration <= maxHop; iteration++) {
            // Set hop count and current iteration
            configuration.setInt("n", maxHop);
            configuration.setInt("currentIteration", iteration);

            // Define input and output paths for this iteration
            String currentInputPath = "output_" + iteration;
            String currentOutputPath = "output_" + (iteration + 1);

            // Configure job for BFS iteration
            Job bfsIterationJob = Job.getInstance(configuration, "BFS Iteration " + iteration);
            bfsIterationJob.setJarByClass(PathWithNHop.class);
            bfsIterationJob.setMapperClass(BFSMapper.class);
            bfsIterationJob.setReducerClass(BFSReducer.class);
            bfsIterationJob.setOutputKeyClass(Text.class);
            bfsIterationJob.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(bfsIterationJob, new Path(currentInputPath));
            FileOutputFormat.setOutputPath(bfsIterationJob, new Path(currentOutputPath));

            // Execute BFS iteration job
            if (!bfsIterationJob.waitForCompletion(true)) {
                System.exit(1); // Exit on job failure
            }
        }

        // Step 3: Generate final output
        Job finalOutputJob = Job.getInstance(configuration, "Generate Final Output");
        finalOutputJob.setJarByClass(PathWithNHop.class);
        finalOutputJob.setMapperClass(GraphBuilderMapper.class); // Reuse graph structure mapper
        finalOutputJob.setReducerClass(GraphBuilderReducer.class); // Reuse graph structure reducer
        finalOutputJob.setOutputKeyClass(Text.class);
        finalOutputJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(finalOutputJob, new Path("output_" + (maxHop + 1)));
        FileOutputFormat.setOutputPath(finalOutputJob, new Path(outputPath));

        // Execute final output job
        if (!finalOutputJob.waitForCompletion(true)) {
            System.exit(1); // Exit on job failure
        }
    }
}

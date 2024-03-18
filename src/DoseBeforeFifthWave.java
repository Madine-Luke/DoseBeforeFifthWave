import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Scanner;
import java.util.StringTokenizer;

public class DoseBeforeFifthWave {
    public static class DoseBeforeFifthWaveMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Scanner scanner = new Scanner(value.toString());
            String row = "";

            if (scanner.hasNextLine()) {
                row = scanner.nextLine();
            }

//            StringTokenizer itr = new StringTokenizer(value.toString(), ",");;
//            String date = itr.nextToken();
//            String ageGroup = itr.nextToken();
//            String gender = itr.nextToken();
//            String s = "";

//            for(int i = 0; i < 12; i++){
//                s += itr.nextToken() + " ";
//            }

            String[] data = row.split(",");
            String date = "";
            String ageGroup = "";
            String remain = "";

            for (int i = 0; i < data.length; i++) {
                if (i == 0) {
                    date = data[i];
                }
                if (date.compareTo("2021-10-31") >= 0 && date.compareTo("2021-12-30") <= 0) {
                    if (i == 1) {
                        ageGroup = data[i];
                    } else if (i > 2) {
                        remain += "," + data[i];
                    }
                }
            }

            context.write(new Text(ageGroup), new Text(remain));
        }
    }

    public static class DoseBeforeFifthWaveReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String ageGroup = key.toString();
            StringTokenizer bio6 = new StringTokenizer("");
            int[] doses = new int[12];
            String out = "";
            for (Text val : values) {
                String[] data = val.toString().split(",");
                for (int i = 1; i < data.length; i++) {
                    if (i == data.length - 1) {
                        bio6 = new StringTokenizer(data[i]);
                        doses[i - 1] = Integer.parseInt(bio6.nextToken());
                    } else {
                        doses[i - 1] += Integer.parseInt(data[i]);
                    }
                }
            }

            for (int dose : doses) {
                out += "," + dose;
            }

            String kvString = ageGroup + out;

            if(ageGroup.compareTo("") != 0){
                context.write(new Text(kvString), new Text());
            }

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length < 2) {
            System.err.println("Usage: DoseBeforeFifthWave <in> [<in>...] <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "DoseBeforeFifthWave by Jinchuan");
        job.setJarByClass(DoseBeforeFifthWave.class);

        job.setMapperClass(DoseBeforeFifthWaveMapper.class);
        job.setReducerClass(DoseBeforeFifthWaveReducer.class);
        job.setOutputKeyClass(Text.class); //output key
        job.setOutputValueClass(Text.class); //output value

        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

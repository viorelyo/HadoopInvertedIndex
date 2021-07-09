import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class WordIndexerJob {
    static private Set<String> stopWords = loadStopWords();

    static private Set<String> loadStopWords() {
        Configuration conf = new Configuration();
        try {
            FileSystem fs = FileSystem.get(conf);

            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(new Path("hdfs:/stopwords.txt"))));

            Set<String> readStopWords = new HashSet<>();
            String line = br.readLine();
            while (line != null) {
                readStopWords.add(line.toLowerCase());
                line = br.readLine();
            }

            return readStopWords;
        } catch (IOException e) {
            e.printStackTrace();
        }

        return new HashSet<>();
    }

    public static class InversedIndexMapper extends Mapper<Object, Text, Text, Text> {
        // https://community.cloudera.com/t5/Support-Questions/Line-Number-in-TextInput-Format/td-p/14040
        // It is not possible to obtain line number, so we preprocess the input files to add the line numbers
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // It is the default InputFormat of MapReduce. TextInputFormat treats each line of each input file as a separate record and performs no parsing.
            // https://data-flair.training/blogs/hadoop-inputformat/

            StringTokenizer itr = new StringTokenizer(value.toString(), "\"\',.()?![]#$*-;:_+/\\<>@%& ");
            if (!itr.hasMoreTokens())
                return;

            String fileName = itr.nextToken();  // first token is the file name (set by first map-reduce job)
            String lineNr = itr.nextToken();    // second token is the line number (set by first map-reduce job)

            Set<String> uniqueWords = new HashSet<>();
            while (itr.hasMoreTokens()) {
                String word = itr.nextToken();
                if (!WordIndexerJob.stopWords.contains(word.toLowerCase()))
                    uniqueWords.add(word);
            }

            for (String word : uniqueWords) {
                context.write(new Text(word), new Text(fileName + " " + lineNr));
            }
        }
    }

    public static class InversedIndexReducer extends Reducer<Text, Text, Text, Text> {

        // Ex: word: (file#1, line#1, line#2, ….) (file#4, line#1, line#2,…) …)
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, List<String>> result = new HashMap<>();

            for (Text val : values) {
                String[] content = val.toString().split(" ");
                String fileName = content[0];
                String lineNr = content[1];

                if (!result.containsKey(fileName)) {
                    result.put(fileName, new ArrayList<>());
                }

                result.get(fileName).add(lineNr);
            }


            StringBuilder formattedResult = new StringBuilder();
            result.forEach((fileName, lines) -> {
                formattedResult.append('(');
                formattedResult.append(fileName);
                formattedResult.append(" | ");
                formattedResult.append(String.join(", ", lines));
                formattedResult.append(") ");
            });

            context.write(key, new Text(formattedResult.toString()));
        }
    }
}

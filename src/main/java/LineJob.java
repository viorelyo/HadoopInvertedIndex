import model.LineWrapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class LineJob {
    public static class OffsetMapper extends Mapper<LongWritable, Text, Text, LineWrapper> {

        @Override
        public void map(LongWritable offset, Text text, Context context) throws IOException, InterruptedException {
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

            context.write(
                    new Text(fileName),
                    new LineWrapper(text.toString(), offset.get())
            );
        }
    }

    public static class OffsetReducer extends Reducer<Text, LineWrapper, Text, Text> {
        @Override
        public void reduce(Text filenameKey, Iterable<LineWrapper> values, Context context) throws IOException, InterruptedException {
            List<LineWrapper> originalLinesList = new ArrayList<>();
            // https://stackoverflow.com/questions/3481914/manipulating-iterator-in-mapreduce
            values.forEach(el -> originalLinesList.add(new LineWrapper(el)));

            List<LineWrapper> sortedLines = originalLinesList.stream()
                    .sorted((x, y) -> (int) (x.getOffset() - y.getOffset()))
                    .collect(Collectors.toList());

            long currentLineNumber = 0;
            for (LineWrapper currentElement : sortedLines) {
                currentLineNumber++;
                context.write(
                        new Text(),
                        new Text(filenameKey.toString() + ' ' + currentLineNumber + ' ' + currentElement.getLineString())
                );
            }
        }
    }
}

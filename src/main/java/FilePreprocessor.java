import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.util.Scanner;
import java.io.FileWriter;

public class FilePreprocessor {

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("Input file is required");
            return;
        }

        int counter = 0;
        String line;

        try {
            File inFile = new File(args[0]);

            File outFile = new File(inFile.getParent() + File.separator + "out_" + inFile.getName());
            if (!outFile.exists())
                Files.createFile(outFile.toPath());

            FileWriter myWriter = new FileWriter(outFile);
            Scanner myReader = new Scanner(inFile);
            while (myReader.hasNextLine()) {
                counter++;
                line = String.valueOf(counter);
                String data = myReader.nextLine();
                line = line + " " + data;
                myWriter.write(line);
                myWriter.write('\n');
            }
            myReader.close();
            myWriter.close();
        } catch (FileNotFoundException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
    }
}

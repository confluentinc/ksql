package io.confluent.ksql;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 *
 */
public class TopologyFileGenerator {

    public static void main(final String[] args) throws IOException {

        if (args.length < 1) {
            System.out.println("The directory to write topology files to is required");
            System.exit(1);
        }

        System.out.println("Starting to write topology files");
        final Path dirPath = Paths.get(args[0]);

        if (!dirPath.toFile().exists()) {
            Files.createDirectory(dirPath);
        } else {
            System.out.println(String.format("Directory %s already exists, if you want to re-generate topology"
                                             + " files then delete %s and run this program again", dirPath, dirPath));
            System.exit(1);
        }

        EndToEndEngineTestUtil.writeExpectedTopologyFiles(dirPath.toString());
        System.out.println(String.format("Done writing topology files to %s", dirPath));
        System.exit(0);
    }

}

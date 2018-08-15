package io.confluent.ksql;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * This class is used to generate the topology files to ensure safe
 * upgrades of KSQL across releases.
 *
 * There are some manual steps in using this class but this should be ok as
 * we only need to create new topology files at the end of a release cycle.
 *
 * The steps to generate topology files:
 *
 * 1. Run this class with the name of the directory.  This requires a relative path from the root
 * of the KSQL project i.e "ksql-engine/src/test/resources/DIR_NAME".  The {@link QueryTranslationTest} expects to find
 * the expected topology files in a directory under src/test/resources.
 *
 * 2. Update the CURRENT_TOPOLOGY_CHECKS_DIR variable in the {@link QueryTranslationTest} class
 * so all tests run against the newly written topology files by default.
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

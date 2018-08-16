package io.confluent.ksql;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

/**
 * This class is used to generate the topology files to ensure safe
 * upgrades of KSQL across releases.
 *
 * There are some manual steps in using this class but this should be ok as
 * we only need to create new topology files at the end of a release cycle.
 *
 * The steps to generate topology files:
 *
 * 1. Run this class BEFORE you update the pom with a new version.  This class will generate expected topology files
 * for the version about to be released in ksql-engine/src/test/resources/VERSION_NUM_expected_topology directory.  Where
 * VERSION_NUM is the version defined in ksql-engine/pom.xml &lt;parent&gt;&lt;version&gt; element.
 *
 * 2. Update the CURRENT_TOPOLOGY_CHECKS_DIR variable in the {@link QueryTranslationTest} class with the newly generated directory name
 * so all tests run against the newly written topology files by default.
 *
 */
public class TopologyFileGenerator {

    private static final String BASE_DIRECTORY = "ksql-engine/src/test/resources/";
    private static final String DIRECTORY_NAME_SUFFIX = "_expected_topology";

    public static void main(final String[] args) throws IOException, ParserConfigurationException, SAXException {

        String formattedVersion = getFormattedVersionFromPomFile();
        String generatedTopologyPath = BASE_DIRECTORY + formattedVersion + DIRECTORY_NAME_SUFFIX;

        System.out.println(String.format("Starting to write topology files to %s", generatedTopologyPath));
        final Path dirPath = Paths.get(generatedTopologyPath);

        if (!dirPath.toFile().exists()) {
            Files.createDirectory(dirPath);
        } else {
            System.out.println(String.format("Directory %s already exists, if you want to re-generate topology"
                                             + " files then delete %s and run this program again", dirPath, dirPath));
            System.exit(1);
        }

        EndToEndEngineTestUtil.writeExpectedTopologyFiles(generatedTopologyPath);
        System.out.println(String.format("Done writing topology files to %s", dirPath));
        System.exit(0);
    }


    private static String getFormattedVersionFromPomFile() throws IOException, ParserConfigurationException, SAXException {
        File pomFile = new File("pom.xml");
        DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
        Document doc = dBuilder.parse(pomFile);

        NodeList versionNodeList = doc.getElementsByTagName("version");
        String versionName = versionNodeList.item(0).getTextContent();

        return versionName.replaceAll("-SNAPSHOT?", "").replaceAll("\\.", "_");

    }

}

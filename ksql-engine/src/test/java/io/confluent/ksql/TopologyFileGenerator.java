package io.confluent.ksql;

import io.confluent.ksql.EndToEndEngineTestUtil.Query;
import java.util.List;
import java.util.stream.Collectors;
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
 * 1. Run this class BEFORE you update the pom with a new version.
 *
 * 2. This class will generate expected topology files
 * for the version specified in the pom file.  The program writes the files to
 * ksql-engine/src/test/resources/expected_topology/VERSION_NUM directory.  Where
 * VERSION_NUM is the version defined in ksql-engine/pom.xml &lt;parent&gt;&lt;version&gt; element.
 *
 * 3. Update the CURRENT_TOPOLOGY_VERSION variable in the {@link QueryTranslationTest}
 * class with the version number for the
 * newly generated directory name so all tests run against the newly written topology files by default.
 *
 */
public class TopologyFileGenerator {

    private static final String BASE_DIRECTORY = "ksql-engine/src/test/resources/expected_topology/";

    public static void main(final String[] args) throws IOException, ParserConfigurationException, SAXException {

        final String formattedVersion = getFormattedVersionFromPomFile();
        final String generatedTopologyPath = BASE_DIRECTORY + formattedVersion;

        System.out.println(String.format("Starting to write topology files to %s", generatedTopologyPath));
        final Path dirPath = Paths.get(generatedTopologyPath);

        if (!dirPath.toFile().exists()) {
            Files.createDirectory(dirPath);
        } else {
            System.out.println(String.format("Directory %s already exists, this will re-generate topology files", dirPath));
        }

        EndToEndEngineTestUtil.writeExpectedTopologyFiles(generatedTopologyPath, getQueryList());
        System.out.println(String.format("Done writing topology files to %s", dirPath));
        System.exit(0);
    }

    private static List<Query>  getQueryList() {
        return QueryTranslationTest.buildQueryList()
            .filter(q -> !q.isAnyExceptionExpected())
            .collect(Collectors.toList());
    }


    private static String getFormattedVersionFromPomFile() throws IOException, ParserConfigurationException, SAXException {
        final File pomFile = new File("pom.xml");
        final DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
        final DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
        final Document pomDoc = documentBuilder.parse(pomFile);

        final NodeList versionNodeList = pomDoc.getElementsByTagName("version");
        final String versionName = versionNodeList.item(0).getTextContent();

        return versionName.replaceAll("-SNAPSHOT?", "").replaceAll("\\.", "_");

    }

}

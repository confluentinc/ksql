package io.confluent.ksql;


import jline.TerminalFactory;
import jline.console.ConsoleReader;
import jline.console.completer.AnsiStringsCompleter;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;


public class KSQL {

    private KSQL() {}

    public static void main(String[] args)
            throws Exception
    {
        try {
            ConsoleReader console = new ConsoleReader();
            console.setPrompt("ksql> ");
            console.addCompleter(new AnsiStringsCompleter("select", "show", "from", "where"));
            String line = null;
            while ((line = console.readLine()) != null) {
//                console.println(line);
                if (line.toLowerCase().startsWith("show")) {
                    console.println("");
                    console.println(" Streams: ");
                    console.println("--------------- ");
                    console.println(" orders ");
                    console.println(" shipments ");
                    console.println(" inventory_changelog ");
                    console.println("");
                } else if (line.toLowerCase().startsWith("describe")) {
                    console.println("");
                    console.println("      Column       |  Type   |                   Comment                   ");
                    console.println("-------------------+---------+---------------------------------------------");
                    console.println(" ordertime         | bigint  | Order time                                ");
                    console.println(" orderid           | varchar | order Id                                ");
                    console.println(" itemId            | varchar | Item Id                                ");
                    console.println(" orderunits        | bigint  | Order units                                ");
                    console.println("");
                }  else if (line.toLowerCase().startsWith("exit")) {
                    console.println("");
                    console.println("Goodbye!");
                    console.println("");
                    console.flush();
                    System.exit(0);
                }  else if (line.toLowerCase().startsWith("select")) {
                    console.println("");
                    console.println("Executing your query. The output will be written to 'large_orders'.");
                    console.println("Type 'terminate' to end the execution.");
                    console.println("");
                    console.flush();

//                    QueryEngine queryEngine = new QueryEngine();
//                    if(!line.endsWith(";")) {
//                        line = line + ";";
//                    }
//                    System.out.println(line);
//                    queryEngine.processQuery(line.toUpperCase());

//                    if ((line = console.readLine()) != null) {
//                        if (line.toLowerCase().startsWith("terminate")) {
//                            console.println("");
//                            console.println("Terminating the KSQL query.");
//                            console.println("");
//                            break;
//                        }
//                    }
                    while (true) {
                        console.flush();
                        QueryEngine queryEngine = new QueryEngine();
                        if(!line.endsWith(";")) {
                            line = line + ";";
                        }
                        System.out.println(line);
                        queryEngine.processQuery(line.toUpperCase());
                        Thread.sleep(5000);
                        if ((line = console.readLine()) != null) {
                            if (line.toLowerCase().startsWith("terminate")) {
                                console.println("");
                                console.println("Terminating the KSQL query.");
                                console.println("");
                                break;
                            }
                        }
                    }
                }
            }
        } catch(IOException e) {
            e.printStackTrace();
        } finally {
            try {
                TerminalFactory.get().restore();
            } catch(Exception e) {
                e.printStackTrace();
            }
        }
    }

}

package de.hpi.ifis;

import asu.edu.rule_miner.rudik.api.RudikApi;
import asu.edu.rule_miner.rudik.api.model.HornRuleResult;
import asu.edu.rule_miner.rudik.api.model.RudikResult;
import asu.edu.rule_miner.rudik.model.horn_rule.HornRule;
import asu.edu.rule_miner.rudik.model.horn_rule.RuleAtom;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import jena.schemagen;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.bson.Document;

import javax.sound.midi.Soundbank;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;


public class GenerateRules {

    private static List<String> readFile(String filePath) {
        List<String> lines = new LinkedList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                lines.add(line);
            }
        } catch (FileNotFoundException e) {
            System.err.println(String.format("The file %s does not exist", filePath));
            e.printStackTrace();
        } catch (IOException e) {
            System.err.println("Some other I/O error occurred");
            e.printStackTrace();
        }

        return lines;
    }

    public static void main(String[] args) {
        MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");
        MongoDatabase database = mongoClient.getDatabase("sherlox");
        MongoCollection<Document> rules = database.getCollection("rules");

        System.out.println("Reading predicates from file ...");
        // Queue<String> predicates = (Queue) readFile("src/main/resources/dbpedia_predicates.txt");
        Queue<String> predicates = new LinkedList<>();
        predicates.add("http://dbpedia.org/ontology/spouse");
        System.out.println(String.format("Read %s predicates", predicates.size()));

        System.out.println("StartingTrying RuDik API ...");
        RudikApi API = new RudikApi("src/main/config/DbpediaConfiguration.xml",
                5 * 60,
                true,
                500);


        while(!predicates.isEmpty()){
            String predicate = predicates.poll();

            final RudikResult result = API.discoverPositiveRules(predicate, 20, 20);

            for (final HornRuleResult oneResult : result.getResults()) {
                Document rule = new Document("predicate", oneResult.getTargetPredicate());
                System.out.println(String.format("Generated for predicate: %s", oneResult.getTargetPredicate()));
                // get type of the rule = positive

                // manually decode RuleType
                if(oneResult.getType() == HornRuleResult.RuleType.positive){
                    rule.append("rule_type", true);
                }
                else{
                    rule.append("rule_type", false);
                }
                System.out.println(String.format("Rule type: %s", oneResult.getType()));

                // 1) get the output Horn Rule
                final HornRule r = oneResult.getOutputRule();
                // System.out.println("-----------------------------------------------------------------");
                rule.append("premise", r.toString());
                rule.append("conclusion", String.format("%s(%s,%s)", oneResult.getTargetPredicate(), "subject", "object"));
                rule.append("hashcode", r.hashCode());
                // System.out.println("-----------------------------------------------------------------");

                // iterate over all atoms of the rule
                // this will construct the premise of a rule
                List<Document> premise_triples = new LinkedList<>();
                for (final RuleAtom atom : r.getRules()) {
                    Document triple = new Document();
                    // get <subject,relation,object> of one atom - this could be something like <subject,child,v0> (with variables)
                    System.out.println(String.format("(%s, %s, %s)", atom.getSubject(), atom.getRelation(), atom.getObject()));
                    triple.append("subject", atom.getSubject())
                            .append("predicate", atom.getRelation())
                            .append("object", atom.getObject());
                    premise_triples.add(triple);
                }
                rule.append("premise_triples", premise_triples);

                // construct the conclusion
                Document conclusion_triple = new Document();
                conclusion_triple.append("subject", "subject")
                        .append("predicate", oneResult.getTargetPredicate())
                        .append("object", "object");
                rule.append("conclusion_triple", conclusion_triple);

                Document exists = rules.find(new Document("hashcode", rule.get("hashcode"))).first();
                if(exists == null){
                    rules.insertOne(rule);
                }
            }
            System.out.printf("%s predicates remaining", predicates.size());
        }
        System.exit(0);
    }
}


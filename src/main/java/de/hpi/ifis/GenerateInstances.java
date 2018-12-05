package de.hpi.ifis;

import asu.edu.rule_miner.rudik.api.RudikApi;
import asu.edu.rule_miner.rudik.api.model.HornRuleInstantiation;
import asu.edu.rule_miner.rudik.api.model.HornRuleResult;
import asu.edu.rule_miner.rudik.api.model.RudikResult;
import asu.edu.rule_miner.rudik.model.horn_rule.HornRule;
import asu.edu.rule_miner.rudik.model.horn_rule.RuleAtom;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;
import org.bson.Document;
import org.bson.types.ObjectId;

import javax.xml.bind.DatatypeConverter;
import java.io.File;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import static asu.edu.rule_miner.rudik.rudikUI.DiscoverNewRules.isInList;

public class GenerateInstances {

    public static String computeHash(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            md.update(input.getBytes());
            byte[] digest = md.digest();
            return DatatypeConverter.printHexBinary(digest).toUpperCase();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void main(String[] args) {

        // load backend configuration
        String backend_config_path = "src/main/config/backend.xml";
        XMLConfiguration backend_config = null;
        try {
            backend_config = new XMLConfiguration(backend_config_path);
        } catch (ConfigurationException e) {
            System.err.println(String.format("No configuration file could be found at the path: %s", backend_config_path));
            e.printStackTrace();
        }

        String host = backend_config.getString("backend.host", "");
        Integer port = backend_config.getInt("backend.port");
        String username = backend_config.getString("backend.username");
        String password = backend_config.getString("backend.password");
        String authSource = backend_config.getString("backend.authSource");
        String authMechanism = backend_config.getString("backend.authMechanism");
        String database = backend_config.getString("backend.database");

        String rudik_config = "src/main/config/DbpediaConfiguration.xml";
        int max_instances = 500;
        System.out.println(args.length);

        if(args.length == 1){
            max_instances = Integer.parseInt(args[0]);
        }else if(args.length == 2){
            max_instances = Integer.parseInt(args[0]);
            rudik_config = args[1];
        }else if(args.length > 2){
            System.err.println("Three parameter where passed to GenerateInstances only two are allowed!");
            System.exit(1);
        }else{
            System.out.println("Using default parameters");
        }
        System.out.println(String.format("Maximum number of generated instances: %s", max_instances));
        System.out.println(String.format("Using RuDik configuration: %s", rudik_config));


        //store the atoms to use them to construct the graph
        Map<String, List<RuleAtom>> rulesAtomsDict = new HashMap<>();
        Map<String, String> rules_entities_dict = new HashMap<>();
        List<String> returnResult = new ArrayList<>();

        // Build MongoDB connection string
        String connectionURI = String.format("mongodb://%s:%s@%s:%s/?authSource=%s",
                username,
                password,
                host,
                port,
                authSource,
                authMechanism);

        MongoClient mongoClient = MongoClients.create(connectionURI);
        MongoDatabase db = mongoClient.getDatabase(database);
        MongoCollection<Document> rules = db.getCollection("rules");
        MongoCollection<Document> instances = db.getCollection("instances");

        long rule_count = rules.countDocuments();
        System.out.printf("Generating instances for %s rules\n", rule_count);
        // Document rule = rules.find(new Document("_id", new ObjectId("5c0142dda08b340c0ed763ba"))).first();
        for (Document rule : rules.find()) {

            ObjectId rule_id = (ObjectId) rule.get("_id");
            String predicate = (String) rule.get("predicate");

            Boolean rule_type = (Boolean) rule.get("rule_type");
            HornRuleResult.RuleType type = null;
            if (rule_type) {
                type = HornRuleResult.RuleType.positive;
            } else {
                type = HornRuleResult.RuleType.negative;
            }

            HornRule horn_rule = HornRule.createHornRule(rule);
            System.out.println(horn_rule);

            RudikApi API = new RudikApi(rudik_config,
                    5 * 60,
                    true,
                    max_instances);

            final RudikResult result = API.instantiateSingleRule(horn_rule, predicate, type);
            if (result != null) {
                for (final HornRuleResult oneResult : result.getResults()) {

                    // get all instantiation of the rule over the KB
                    final List<HornRuleInstantiation> ruleInst = oneResult.getAllInstantiations();
                    // iterate over all instantiation
                    for (final HornRuleInstantiation instance : ruleInst) {
                        // get <subject,object> of the instantiation - this could be something like <Barack_Obama,Michelle_Obama>

                        String sep = "";
                        String entitieSep = "";
                        String temp = "";
                        String ruleEntities = "";
                        //store the previous atom to compare with the new one and remove the instantiation if two following atoms are the same
                        List<String> ruleAtoms = new ArrayList<String>();
                        int i = 0;
                        List<RuleAtom> ruleAtomsList = new LinkedList<>();

                        // iterate over all instantiated atoms of the rule
                        for (final RuleAtom atom : instance.getInstantiatedAtoms()) {
                            //list of atoms composing a rule
                            // get <subject,relation,object> of one atom - this could be something like
                            // <Barack_Obama,child,Sasha_Obama> (now the atoms are instantiated, so they contain actual values and not
                            // variables)

                            File relation = new File(atom.getRelation());
                            //if the current atoms is already in the list of atoms set i to 1
                            String constructed_name = relation.getName() + "(" + atom.getSubject() + "," + atom.getObject() + ")";
                            if (isInList(ruleAtoms, constructed_name)) {
                                i++;
                            }
                            temp += sep + relation.getName() + "(" + atom.getSubject() + "," + atom.getObject() + ")";
                            sep = " & ";
                            ruleAtoms.add(relation.getName() + "(" + atom.getSubject() + "," + atom.getObject() + ")");
                            ruleAtomsList.add(atom);
                            //construct the string of entities that compose the instantiated rule
                            if (!ruleEntities.contains(atom.getSubject())) {
                                ruleEntities += entitieSep + atom.getSubject();
                                entitieSep = ";";
                            }
                            if (!ruleEntities.contains(atom.getObject())) {
                                ruleEntities += entitieSep + atom.getObject();
                                entitieSep = ";";

                            }
                        }
                        rulesAtomsDict.put(temp, ruleAtomsList);
                        rules_entities_dict.put(temp, ruleEntities);
                        if (!isInList(returnResult, temp) & i == 0) {
                            returnResult.add(temp);

                            // add instantiation to MongoDB
                            Document inst = new Document("rule_id", rule_id)
                                    .append("predicate", predicate);

                            List<Document> instance_atoms = new LinkedList<>();
                            Document assignment = new Document();
                            StringBuilder premise = new StringBuilder();
                            for (RuleAtom atom : ruleAtomsList) {
                                // build rule entities
                                assignment.append("subject", atom.getSubject());
                                assignment.append("object", atom.getObject());

                                instance_atoms.add(new Document("subject", atom.getSubject())
                                        .append("predicate", atom.getRelation())
                                        .append("object", atom.getObject()));


                                if (!premise.toString().equals("")) {
                                    premise.append(" & ").append(atom.getRelation()).append("(").append(atom.getSubject()).append(",").append(atom.getObject()).append(")");
                                } else {
                                    premise.append(atom.getRelation()).append("(").append(atom.getSubject()).append(",").append(atom.getObject()).append(")");
                                }
                            }

                            inst.append("premise_triples", instance_atoms)
                                    .append("assignment", assignment)
                                    .append("premise", premise.toString());

                            // build conclusion
                            StringBuilder conclusion = new StringBuilder();
                            Document conclusion_triple = new Document("subject", assignment.get("subject"))
                                    .append("predicate", predicate)
                                    .append("object", assignment.get("object"));
                            conclusion.append(predicate).append("(").append(assignment.get("subject")).append(",").append(assignment.get("object")).append(")");

                            inst.append("conclusion", conclusion.toString());
                            inst.append("conclusion_triple", conclusion_triple);

                            // compute an MD5 hash for the instance
                            String joined = String.format("%s %s", premise.toString(), conclusion.toString());
                            String md5_hash = computeHash(String.format("%s %s", premise.toString(), conclusion.toString()));
                            inst.append("hashcode", md5_hash);

                            // add label and details field
                            inst.append("label", -1);
                            inst.append("details", new Document());


                            // if the same same instance does not already exist for a specific rule => add it
                            Document exists = instances.find(new Document("rule_id", rule_id).append("hashcode", md5_hash)).first();
                            if (exists == null) {
                                instances.insertOne(inst);
                            }

                        }
                    }
                }
            }
            rule_count--;
            System.out.printf("%s rules remaining ...\n", rule_count);
        }
        System.exit(0);
    }
}
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
import org.bson.Document;
import org.bson.types.ObjectId;

import java.io.File;
import java.util.*;

import static asu.edu.rule_miner.rudik.rudikUI.DiscoverNewRules.isInList;

public class GenerateInstances {

    public static String removeRad(String stringWithRad){
        List<String> radToRemove= new ArrayList<>();
        String stringWithoutRad=stringWithRad;
        radToRemove.add("^^http://www.w3.org/2001/XMLSchema#date");
        radToRemove.add("^^http://www.w3.org/2001/XMLSchema#gYear");
        radToRemove.add("^^http://www.w3.org/2001/XMLSchema#integer");
        radToRemove.add("http://dbpedia.org/resource/");
        radToRemove.add("http://yago-knowledge.org/resource/");
        radToRemove.add("http://dbpedia.org/ontology/");
        for (String rad : radToRemove){
            stringWithoutRad=stringWithoutRad.replace(rad,"");
        }
        return stringWithoutRad;
    }

    public static void main(String[] args) {
        System.out.print("Generate some instances");
        String rudik_config = "src/main/config/DbpediaConfiguration.xml";


        //store the atoms to use them to construct the graph
        Map<String, List<RuleAtom>> rulesAtomsDict = new HashMap<>();
        Map<String, String> rules_entities_dict = new HashMap<>();
        List<String> returnResult= new ArrayList<>();


        MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");
        MongoDatabase database = mongoClient.getDatabase("sherlox");
        MongoCollection<Document> rules = database.getCollection("rules");
        MongoCollection<Document> instances = database.getCollection("instances");

        Document rule = rules.find(new Document("_id", new ObjectId("5c0142dda08b340c0ed763ba"))).first();
        ObjectId rule_id = (ObjectId) rule.get("_id");
        String predicate = (String) rule.get("predicate");

        Boolean rule_type = (Boolean) rule.get("rule_type");
        HornRuleResult.RuleType type = null;
        if(rule_type){
            type = HornRuleResult.RuleType.positive;
        }else{
            type = HornRuleResult.RuleType.negative;
        }

        HornRule horn_rule = HornRule.createHornRule(rule);
        System.out.println(horn_rule);

        RudikApi API = new RudikApi(rudik_config,
                5 * 60,
                true,
                500);

        final RudikResult result = API.instantiateSingleRule(horn_rule, predicate, type);
        if( result != null) {
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
                    rulesAtomsDict.put(removeRad(temp), ruleAtomsList);
                    rules_entities_dict.put(removeRad(temp), ruleEntities);
                    if (!isInList(returnResult, removeRad(temp)) & i == 0) {
                        returnResult.add(removeRad(temp));

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

                        // TODO: Compute hashcode for instance

                        instances.insertOne(inst);
                    }
                }
            }
        }
    }
}
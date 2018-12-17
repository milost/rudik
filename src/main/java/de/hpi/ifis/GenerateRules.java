package de.hpi.ifis;

import asu.edu.rule_miner.rudik.api.RudikApi;
import asu.edu.rule_miner.rudik.api.model.HornRuleResult;
import asu.edu.rule_miner.rudik.api.model.RudikResult;
import asu.edu.rule_miner.rudik.configuration.Constant;
import asu.edu.rule_miner.rudik.model.horn_rule.HornRule;
import asu.edu.rule_miner.rudik.model.horn_rule.RuleAtom;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import jena.schemagen;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.bson.Document;

import javax.sound.midi.Soundbank;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.ArrayList;
import java.util.HashMap;

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
		String backend_config_path = "src/main/config/backend.xml";
		String filePath = "src/main/resources/dbpedia_predicates.txt";
		String rudik_config = "src/main/config/DbpediaConfiguration.xml";
		String parameters_path = "src/main/config/ParametersConfiguration.xml";

		if (args.length == 1) {
			filePath = args[0];
			System.out.println("Reading predicates from: " + filePath);
			System.out.println("Reading configuration from: " + rudik_config);
		} else if (args.length == 2) {
			filePath = args[0];
			rudik_config = args[1];
			System.out.println("Reading predicates from: " + filePath);
			System.out.println("Reading configuration from: " + rudik_config);
		} else if (args.length > 2) {
			System.err.println("Three parameter where passed to GenerateRules, only two are allowed!");
			System.exit(1);
		} else {
			System.out.println("Using default parameters");
		}

		XMLConfiguration backend_config = null;
		try {
			backend_config = new XMLConfiguration(backend_config_path);
		} catch (ConfigurationException e) {
			System.err.println(
					String.format("No configuration file could be found at the path: %s", backend_config_path));
			e.printStackTrace();
		}

		String host = backend_config.getString("backend.host", "localhost");
		Integer port = backend_config.getInt("backend.port", 27017);
		String username = backend_config.getString("backend.username", "");
		String password = backend_config.getString("backend.password", "");
		String authSource = backend_config.getString("backend.authSource", "admin");
		String authMechanism = backend_config.getString("backend.authMechanism");
		String database = backend_config.getString("backend.database");

		// Build MongoDB connection string
		String connectionURI = "";
		if (username.length() != 0 && password.length() != 0 && database.length() != 0) {
			connectionURI = String.format("mongodb://%s:%s@%s:%s/%s?authSource=%s", username, password, host, port,
					database, authSource, authMechanism);
		} else {
			connectionURI = String.format("mongodb://%s:%s", host, port);
		}
		
		MongoClient mongoClient = MongoClients.create(connectionURI);
		MongoDatabase db = mongoClient.getDatabase(database);
		MongoCollection<Document> rules = db.getCollection("rules");

		System.out.printf("Reading predicates from %s ...\n", filePath);
		Queue<String> predicates = (Queue) readFile(filePath);
		// Queue<String> predicates = new LinkedList<>();
		// predicates.add("http://dbpedia.org/ontology/spouse");
		System.out.println(String.format("Read %s predicates", predicates.size()));

		System.out.printf("Instantiating RuDik API with config: %s ...\n", rudik_config);

		RudikApi API = new RudikApi(rudik_config, 5 * 60, true, 500);

		XMLConfiguration param_config = null;
		List<Map<String, Double>> score_params = new ArrayList<>();
		Map<String, Double> score = new HashMap<String, Double>();
		int[][] num_examples = new int[][] {{20,20}};
		int[] max_length_params = new int[] { 2 };
		try {
			param_config = new XMLConfiguration(parameters_path);
			
			// Get alpha, beta, gamma parameters
			if (param_config.containsKey(Constant.CONF_SCORE_PARAMS)) {
				List<HierarchicalConfiguration> scores = param_config.configurationsAt(Constant.CONF_SCORE_PARAMS);
				for(HierarchicalConfiguration score_tmp : scores) {
					score.put("alpha", score_tmp.getDouble("alpha"));
					score.put("beta", score_tmp.getDouble("beta"));
					score.put("gamma", score_tmp.getDouble("gamma"));
					score_params.add(0, score);
				}
			}
			
			// Get number of positive negative examples parameters
			if (param_config.containsKey(Constant.CONF_EXAMPLES_PARAMS)) {
				List<HierarchicalConfiguration> examples = param_config.configurationsAt(Constant.CONF_EXAMPLES_PARAMS);
				num_examples = new int[examples.size()][2];
				
				for (int i = 0; i < examples.size(); i++) {
					HierarchicalConfiguration example_tmp = examples.get(i);
					num_examples[i][0] = example_tmp.getInt("positive");
					num_examples[i][1] = example_tmp.getInt("negative");
				}
			}
			
			// Get max length rule parameters
			if (param_config.containsKey(Constant.CONF_MAX_LENGTH_PARAMS)) {
				List<HierarchicalConfiguration> max_length_rules = param_config.configurationsAt(Constant.CONF_MAX_LENGTH_PARAMS);
				max_length_params = new int[max_length_rules.size()];
				int n = max_length_rules.size();
				
				for (int i = 0; i < max_length_rules.size(); i++) {
					HierarchicalConfiguration max_length = max_length_rules.get(i);
					max_length_params[i] = Integer.parseInt(max_length.getRootNode().getValue().toString());
				}
			}
			
		} catch (ConfigurationException e) {
			System.err.println(
					String.format("No configuration file could be found at the path: %s", backend_config_path));
			e.printStackTrace();
		}

		while (!predicates.isEmpty()) {
			String predicate = predicates.poll();

			for (int i = 0; i < max_length_params.length; i++) {
				int max_rule_length = max_length_params[i];
				for (int j = 0; j < score_params.size(); j++) {
					double alpha = score_params.get(j).get("alpha") != null ? score_params.get(j).get("alpha") : 0.3;
					double beta = score_params.get(j).get("beta") != null ? score_params.get(j).get("beta") : 0.7;
					double gamma = score_params.get(j).get("gamma") != null ? score_params.get(j).get("gamma") : 0.0;

					for (int l = 0; l < num_examples.length; l++) {
						int nb_negative_examples = num_examples[l][0];
						int nb_positive_examples = num_examples[l][1];

						API.setAlphaBetaGammaParameter(alpha, beta, gamma);
						API.setMaxRuleLength(max_rule_length);
						API.setNegativeExamplesLimit(nb_negative_examples);
						API.setPositiveExamplesLimit(nb_positive_examples);
						API.setSampling(alpha, beta, gamma, true);

						for (int k = 0; k < 2; k++) {
							RudikResult result = new RudikResult();

							if (k != 0) {
								result = API.discoverNegativeRules(predicate, nb_positive_examples,
										nb_negative_examples);
							} else {
								result = API.discoverPositiveRules(predicate, nb_positive_examples,
										nb_negative_examples);
							}

							for (final HornRuleResult oneResult : result.getResults()) {
								Document rule = new Document("predicate", oneResult.getTargetPredicate());
								System.out.println(
										String.format("Generated for predicate: %s", oneResult.getTargetPredicate()));
								// get type of the rule = positive

								// manually decode RuleType
								if (oneResult.getType() == HornRuleResult.RuleType.positive) {
									rule.append("rule_type", true);
								} else {
									rule.append("rule_type", false);
								}
								System.out.println(String.format("Rule type: %s", oneResult.getType()));

								// 1) get the output Horn Rule
								final HornRule r = oneResult.getOutputRule();
								// System.out.println("-----------------------------------------------------------------");
								rule.append("premise", r.toString());
								rule.append("conclusion", String.format("%s(%s,%s)", oneResult.getTargetPredicate(),
										"subject", "object"));
								rule.append("hashcode", r.hashCode());
								// System.out.println("-----------------------------------------------------------------");

								// iterate over all atoms of the rule
								// this will construct the premise of a rule
								List<Document> premise_triples = new LinkedList<>();
								for (final RuleAtom atom : r.getRules()) {
									Document triple = new Document();
									// get <subject,relation,object> of one atom - this could be something like
									// <subject,child,v0> (with variables)
									System.out.println(String.format("(%s, %s, %s)", atom.getSubject(),
											atom.getRelation(), atom.getObject()));
									triple.append("subject", atom.getSubject()).append("predicate", atom.getRelation())
											.append("object", atom.getObject());
									premise_triples.add(triple);
								}
								rule.append("premise_triples", premise_triples);

								// construct the conclusion
								Document conclusion_triple = new Document();
								conclusion_triple.append("subject", "subject")
										.append("predicate", oneResult.getTargetPredicate()).append("object", "object");
								rule.append("conclusion_triple", conclusion_triple);

								rule.append("alpha", alpha);
								rule.append("beta", beta);
								rule.append("gamma", gamma);
								rule.append("nb_negative_examples", nb_negative_examples);
								rule.append("nb_positive_examples", nb_positive_examples);
								rule.append("max_rule_length", max_rule_length);
								rule.append("human_confidence", -1);

								Document exists = rules.find(new Document("hashcode", rule.get("hashcode"))).first();
								if (exists == null) {
									rules.insertOne(rule);
								}
							}
						}

					}
				}
			}

			System.out.printf("%s predicates remaining", predicates.size());
		}
		System.exit(0);
	}

	private static int customHashCode(HornRule r, int max_rule_length, double alpha, double beta, double gamma,
			int nb_positive_examples, int nb_negative_examples) {
		final int prime = 31;
		int result = 1;
		int alpha_hashcode = (int) (alpha * 100);
		int beta_hashcode = (int) (beta * 100);
		int gamma_hashcode = (int) (gamma * 100);
		result = prime * result + r.hashCode() + alpha_hashcode + beta_hashcode + gamma_hashcode + max_rule_length
				+ nb_negative_examples + nb_positive_examples;
		return result;
	}

}

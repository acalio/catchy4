package io.acalio.nlp;
import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

import com.github.pemistahl.lingua.api.*;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.util.CoreMap;
import eu.fbk.dh.tint.runner.TintPipeline;


/**
 * This class provide the ability to detect a language 
 * of a raw text and retrieve its sentiment. 
 * Even though we are able to detect several langauges,
 * only the sentiment for italian and english texts can be extracted 
 *
 */
public class SentimentAnalyzer implements Serializable {

    private transient static TintPipeline itPipeline = new TintPipeline();
    private transient static StanfordCoreNLP enPipeline = null;
    private transient static LanguageDetector detector = null;

    static {
	try {
	    //initialize the pipeline for the italian language
	    itPipeline.loadDefaultProperties();
	    itPipeline.setProperty("annotators", "tokenize, ssplit, pos, parse, sentiment");
	    itPipeline.load();
	    
	    //initialize core nlp
	    Properties props = new Properties();
	    props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
	    enPipeline = new StanfordCoreNLP(props);
	    
	    //initialize language detector
	    detector = LanguageDetectorBuilder
		.fromLanguages(Language.ENGLISH,
			       Language.ITALIAN,
			       Language.GERMAN,
			       Language.FRENCH,
			       Language.SPANISH).build();
	    
	} catch (IOException e) {
		e.printStackTrace();
	}

    }


    

    /**
     * Get the sentiment associated with the text
     * provided as input. 
     * The sentiment is given by the average sentiment
     * among all the senteces in the text
     *
     * @param text: String
     * @return  the text sentiment
     */ 
    public static double getSentiment(String text) {
	Language language = detector.detectLanguageOf(text);
	Annotation annotations = null;
	switch (language) {
	case ITALIAN:
	    annotations = itPipeline.runRaw(text);
	    break;
	case ENGLISH:
	    annotations = enPipeline.process(text);
	    break;
	default:
	    //other languages are ignored
	    return Double.MAX_VALUE;
	}
	double sentiment = 0;
	for(CoreMap sentence : annotations.get(CoreAnnotations.SentencesAnnotation.class))
	    sentiment += RNNCoreAnnotations
		.getPredictedClass(
		   sentence.get(SentimentCoreAnnotations.SentimentAnnotatedTree.class)		      
		);

	sentiment /= (double) annotations.get(CoreAnnotations.SentencesAnnotation.class).size();
	return sentiment;
    }

    /**
     * Get the language of the text proivded as input
     * 
     * @param text
     * @return the text language
     *
     */
    public static Language getLanguage(String text) {
	return detector.detectLanguageOf(text);
    }

    

}

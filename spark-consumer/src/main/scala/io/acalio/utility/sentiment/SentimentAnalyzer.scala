// package io.acalio.utility.sentiment
// import edu.stanford.nlp.pipeline.Annotation
// import edu.stanford.nlp.ling.CoreAnnotations
// import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
// import edu.stanford.nlp.trees.Tree;
// import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
// import edu.stanford.nlp.pipeline.StanfordCoreNLP
// import eu.fbk.dh.tint.runner.TintPipeline
// import java.{util => ju}
// import com.github.pemistahl.lingua.api._
// import com.github.pemistahl.lingua.api.Language._

// object SentimentAnalyzer {

//   @transient private var enSentimentPipeline : StanfordCoreNLP = _
//   @transient private var itSentimentPipeline : TintPipeline = _

//   // import Language.Language
//   // private var lan : Language = Language.EN

//   // def setLanguage(l: Language) {
//   //   lan = l
//   // }

//   def getLanguage(text: String): Language = {
//     val detector = LanguageDetectorBuilder
//       .fromLanguages(ENGLISH, ITALIAN)
//       .build
//     return detector.detectLanguageOf(text)
//   }

//   def sentiment(text:String): Option[Double] = {
//     var annotation: Annotation = null
//     val lan = getLanguage(text)
//     lan match {
//       case ENGLISH => {
//         annotation = sentimentEn(text)
//       }
//       case ITALIAN => {
//         annotation = sentimentIt(text)
//       }
//       case _ => return None
//     }

//     //val the the sentiment score of each sentence
//     var sentimentScore: Double = 0
//     annotation
//       .get(classOf[CoreAnnotations.SentencesAnnotation])
//       .forEach(s =>
//         sentimentScore+=RNNCoreAnnotations
//           .getPredictedClass(s.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])))
//     //the sentiment score is the average sentiment among every tree in the text
//     return Some(sentimentScore / annotation.get(classOf[CoreAnnotations.SentencesAnnotation]).size().toDouble)
//   }


//   def sentimentEn(text: String): Annotation = {
//     if (enSentimentPipeline == null) {
//       val props = new ju.Properties
//       props.setProperty("annotators", "tokenize, ssplit, parse, sentiment")
//       enSentimentPipeline = new StanfordCoreNLP(props)
//     }
//     // Get the original Annotation (Stanford CoreNLP)
//     return enSentimentPipeline.process(text);
//   }

//   def sentimentIt(text: String): Annotation = {
//     if (itSentimentPipeline == null) {
//       itSentimentPipeline = new TintPipeline
//       // Load the default properties
//       // see https://github.com/dhfbk/tint/blob/master/tint-runner/src/main/resources/default-config.properties
//       itSentimentPipeline.loadDefaultProperties();
//       // Add a custom property
//       itSentimentPipeline.setProperty("annotators", "tokenize, ssplit, pos, parse, sentiment");
//       // Load the models
//       itSentimentPipeline.load();
//     }
//     return itSentimentPipeline.runRaw(text)
//   }
  
// }

// // object Language extends Enumeration {
// //   type Language = Value
// //   val EN, IT = Value
// //   def get(l: String): Language = {
// //     l match {
// //       case "en" => return EN
// //       case "it" => return IT
// //       case _ => throw new RuntimeException(s"Unrecognized option: $l")
// //     }

// //   }
// // }



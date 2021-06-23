package io.acalio.nlp;
public class App 
{
    public static void main( String[] args )
    {
        
	double sentiment = SentimentAnalyzer.getSentiment("ti voglio bene");
	System.out.println("Sentiment: "+sentiment);

	sentiment = SentimentAnalyzer.getSentiment("I love you");
	System.out.println("Sentiment: "+sentiment);

	sentiment = SentimentAnalyzer.getSentiment("Je m'appelle charlie, je vien du quebec");
	System.out.println("Sentiment: "+ sentiment);
    }
}

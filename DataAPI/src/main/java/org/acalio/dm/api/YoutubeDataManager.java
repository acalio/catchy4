package org.acalio.dm.api;

import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;

import com.google.api.services.youtube.YouTube;
import com.google.api.services.youtube.model.Activity;
import com.google.api.services.youtube.model.ActivityListResponse;
import com.google.api.services.youtube.model.Channel;
import com.google.api.services.youtube.model.ChannelListResponse;
import com.google.api.services.youtube.model.Comment;
import com.google.api.services.youtube.model.CommentListResponse;
import com.google.api.services.youtube.model.CommentThread;
import com.google.api.services.youtube.model.CommentThreadListResponse;
import com.google.api.services.youtube.model.SearchListResponse;
import com.google.api.services.youtube.model.SearchResult;
import com.google.api.services.youtube.model.Subscription;
import com.google.api.services.youtube.model.SubscriptionListResponse;
import com.google.api.services.youtube.model.Video;
import com.google.api.services.youtube.model.VideoListResponse;

import org.acalio.dm.model.ModelFactory;
import org.acalio.dm.model.avro.YChannel;
import org.acalio.dm.model.avro.YComment;
import org.acalio.dm.model.avro.YLike;
import org.acalio.dm.model.avro.YSubscription;
import org.acalio.dm.model.avro.YVideo;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

public class YoutubeDataManager {

    //configuration class for common option in the api calls
    public static class CallCommonConfiguration {
	//order option
	public static class ORDER_OPTION {
	    public static String RELEVANCE = "relevance";
	    public static String ALPHABETICAL = "alphabetical";
	    public static String UNREAD = "unread";
	    public static String TIME = "time";
	}
	    
	public static long MAX_RESULTS = 10;
	public static String ORDER = ORDER_OPTION.RELEVANCE;
    }
    
    //type of activity to be considered as like activity
    private static final Set<String> LIKE_ACTIVITIES = new HashSet<>(Arrays.asList("favorite", "like", "playlistitem", "recommendation"));
    

    // You need to set this value for your code to compile.
    private static String DEVELOPER_KEY = "<your-key>";

    private static final String APPLICATION_NAME = "My-App";
    private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

    /**
     * Change the current developer key to the one provided
     * as input
     * 
     * @param newKey 
     */
    public static void setDeveloperKey(String newKey){
	DEVELOPER_KEY = newKey;
    }

    public static void checkDeveloperKey(){
	if(DEVELOPER_KEY==null)
	    throw new DeveloperKeyNotSetException ();
    }
    /**
     * Build and return an authorized API client service.
     *
     * @return an authorized API client service
     * @throws GeneralSecurityException, IOException
     */
    public static YouTube getService() throws GeneralSecurityException, IOException, DeveloperKeyNotSetException {
	checkDeveloperKey();
        final NetHttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();

        return new YouTube.Builder(httpTransport, JSON_FACTORY, new HttpRequestInitializer() {
            @Override
            public void initialize(HttpRequest httpRequest) throws IOException {
                httpRequest.setConnectTimeout(0);  // no timeout
                httpRequest.setReadTimeout(0);  // no timeout
            }})
            .setApplicationName(APPLICATION_NAME)
            .build();
    }


    /**
     * Return the list of video related to the query provided as input
     *
     *
     * @param query, the query to be executed 
     * 
     * @param the maximum number of video the be retrieved
     *
     * @return a list of youtube videos
     */
    public static List<YVideo> executeQuery(String query, long maxNumberOfVideos) throws GeneralSecurityException,
											 IOException, GoogleJsonResponseException, DeveloperKeyNotSetException {
	checkDeveloperKey();
	YouTube youTubeService = getService();
	YouTube.Search.List request = youTubeService
	    .search()
	    .list("snippet")
	    .setQ(query)
	    .setMaxResults(CallCommonConfiguration.MAX_RESULTS)
	    .setOrder(CallCommonConfiguration.ORDER)
	    .setKey(DEVELOPER_KEY);

	List<YVideo> videoList = new LinkedList<>();
	SearchListResponse response = null;
	String nextToken = null;
	do {
	    response = request.execute();
	    Iterator<SearchResult> it = response.getItems().iterator();
	    while (it.hasNext() && videoList.size() < maxNumberOfVideos) {
		SearchResult sr = it.next();
		if(sr.getId().getVideoId()!=null)
		    videoList.add(ModelFactory.createVideo(sr));
	    }
	    nextToken = response.getNextPageToken();
	    request.setPageToken(nextToken);
	} while(nextToken!=null && videoList.size()<maxNumberOfVideos);

	return videoList;
    }


    /**
     * Return the comment threads related to the video provided as input
     * 
     *
     * @param videoId, the id of the video
     * 
     * @param maxCommmentThreads, the number of comment threads to be retrieved
     * 
     * @return a list of comments
     */
    public static List<YComment> commentThread(String videoId, long maxCommmentThreads) throws GeneralSecurityException,
											       IOException, GoogleJsonResponseException, DeveloperKeyNotSetException {
	checkDeveloperKey();
	YouTube youtubeService = getService();
	YouTube.CommentThreads.List request = youtubeService.commentThreads()
            .list("snippet, replies")
	    .setVideoId(videoId)
	    .setOrder(CallCommonConfiguration.ORDER)
	    .setMaxResults(CallCommonConfiguration.MAX_RESULTS)
	    .setKey(DEVELOPER_KEY);
	
	CommentThreadListResponse response = null;
	List<YComment> commentList = new LinkedList<>();
	int numOfThread = 0;
	String nextToken = null;
	do {
	    response = request.execute(); //comments might be disabled
	    Iterator<CommentThread> cit = response.getItems().iterator();
	    for (; cit.hasNext() && numOfThread < maxCommmentThreads; numOfThread++) {
		//extract the top level comment
		CommentThread t = cit.next();
		Comment topLevel = t.getSnippet().getTopLevelComment();
		YComment comment = ModelFactory.createComment(topLevel);
		commentList.add(comment);
		long repliesCount = t.getSnippet().getTotalReplyCount();

		//this thread does not have any reply
		if(repliesCount==0)
		    continue;

		//iterate over each reply to the topLevelComment
		//there are two scenarios: (i) all the replies to the topLevel comment
		//are retrieved in this call; (ii) there are more replies thatn the one
		//downloaded with the current call, thus we execute another API requst
		List<Comment> replies = null;
		if(t.getReplies().getComments().size()==repliesCount){
		    //first case
		    replies = t.getReplies().getComments();
		}else {
		    //secondCase - make a second  call to the api
		    YouTube.Comments.List commentRequest = youtubeService.comments()
			.list("snippet")
			.setParentId(topLevel.getId())
			.setMaxResults(repliesCount)
			.setKey(DEVELOPER_KEY);
		    
		    CommentListResponse commentResponse = commentRequest.execute();
		    replies = commentResponse.getItems();
		}

		//create a YComment for each reply to the top level comment
		Iterator<Comment> rit = replies.iterator();
		while(rit.hasNext()) {
		    Comment c = rit.next();
		    YComment repComment = ModelFactory.createComment(c, topLevel.getSnippet().getVideoId());
		    repComment.setParentID(topLevel.getId());
		    commentList.add(repComment);
		}
	    }
		nextToken = response.getNextPageToken();
		request.setPageToken(nextToken);
	} while (nextToken != null && numOfThread < maxCommmentThreads);

	return commentList;
    }


    
    public static List<YSubscription> getSubscriptions(String channelId, long maxResults)
	throws GeneralSecurityException, IOException, GoogleJsonResponseException, DeveloperKeyNotSetException {
	checkDeveloperKey();
	YouTube youtubeService = getService();
	YouTube.Subscriptions.List request = youtubeService.subscriptions()
            .list("snippet")
	    .setChannelId(channelId)
	    .setMaxResults(CallCommonConfiguration.MAX_RESULTS)
	    .setKey(DEVELOPER_KEY);
	
	SubscriptionListResponse response = null;
	List<YSubscription> subscriptionList = new LinkedList<>();
	String nextToken = null;
	do{
	    response = request.execute();
	    Iterator<Subscription> it = response.getItems().iterator();
	    while(it.hasNext() && subscriptionList.size()<maxResults)
		subscriptionList.add(ModelFactory.createSubscription(it.next(), channelId));
	    nextToken = response.getNextPageToken();
	    request.setPageToken(nextToken);
	}while(nextToken!=null && subscriptionList.size()<maxResults);

	return subscriptionList;
    }

    public static List<YSubscription> getSubscriptionsAlternative(String channelId, long maxResults)
	throws GeneralSecurityException, IOException, GoogleJsonResponseException, DeveloperKeyNotSetException {
	checkDeveloperKey();
	YouTube youtubeService = getService();
	YouTube.Activities.List request = youtubeService.activities()
            .list("snippet, contentDetails")
	    .setChannelId(channelId)
	    .setMaxResults(CallCommonConfiguration.MAX_RESULTS)
	    .setKey(DEVELOPER_KEY);
	
	ActivityListResponse response = null;
	List<YSubscription> subscriptionList = new LinkedList<>();
	String nextToken = null;
	do{
	    response = request.execute();
	    Iterator<Activity> it = response.getItems().iterator();
	    while(it.hasNext() && subscriptionList.size()<maxResults){
		Activity activity = it.next();
		String activityType = activity.getSnippet().getType().toLowerCase();
		if(activityType == "subscription") {
		    subscriptionList.add(ModelFactory.createSubscription(activity, channelId));
		}
	    }
	    nextToken = response.getNextPageToken();
	    request.setPageToken(nextToken);
	}while(nextToken!=null && subscriptionList.size()<maxResults);

	return subscriptionList;
    }


    public static YChannel getChannel(String channelId)
	throws GeneralSecurityException, IOException, GoogleJsonResponseException,  DeveloperKeyNotSetException {
	checkDeveloperKey();
	YouTube youtubeService = getService();
	YouTube.Channels.List request = youtubeService.channels()
	    .list("snippet,contentDetails,statistics,status")
	    .setId(channelId)
	    .setKey(DEVELOPER_KEY);

	ChannelListResponse response = request.execute();
	Channel channel = response.getItems().iterator().next();

	return ModelFactory.createChannel(channel);
    }

    public static YVideo getVideo(String videoId)
	throws GeneralSecurityException, IOException, GoogleJsonResponseException, DeveloperKeyNotSetException {
	checkDeveloperKey();
	YouTube youtubeService = getService();
	YouTube.Videos.List request = youtubeService.videos()
	    .list("snippet,statistics,contentDetails")
	    .setId(videoId)
	    .setKey(DEVELOPER_KEY);

	VideoListResponse response = request.execute();
	Video video = response.getItems().iterator().next();
	return ModelFactory.createVideo(video);
    }

    /**
     * Get the likes of a user from the activity logs.
     * Action like ones inserted in the @{LIKES_ACTIVITIES} are 
     * considered as the expression of a like action to a video
     *
     * @param channelId is the id of the channel you are interested in
     * @param maxResults is the maximum number of like you want to retrieve
     * 
     * @return a list of like actions
     */
    public static List<YLike> getLikes(String channelId, long maxResults) 
	throws GeneralSecurityException, IOException, GoogleJsonResponseException, DeveloperKeyNotSetException {
	    checkDeveloperKey();
	    YouTube youtubeService = getService();
	    YouTube.Activities.List request = youtubeService.activities()
		.list("snippet, contentDetails")
		.setChannelId(channelId)
		.setMaxResults(CallCommonConfiguration.MAX_RESULTS)
		.setKey(DEVELOPER_KEY);
	    ActivityListResponse response = null;
	    String nextToken = null;
	    List<YLike> likes = new LinkedList<>();
	    do {
		response = request.execute();
		Iterator<Activity> it = response.getItems().iterator();
		while(it.hasNext() && likes.size()< maxResults) {
		    Activity activity = it.next();
		    String activityType = activity.getSnippet().getType().toLowerCase();
		    if (LIKE_ACTIVITIES.contains(activityType)) 
			likes.add(ModelFactory.createLike(activity, channelId, activityType));
		}
		nextToken = response.getNextPageToken();
		request.setPageToken(nextToken);
	} while (nextToken != null && likes.size() < maxResults);

	    return likes;
    }

}

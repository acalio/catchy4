package org.acalio.dm;

import java.util.List;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import org.acalio.dm.api.YoutubeDataManager;
import org.acalio.dm.model.avro.*;



public class App {

    private static void executeQuery(String query, long limit) {
	try {
	    System.out.println("Executing query: "+query);
	    List<YVideo> vList = YoutubeDataManager.executeQuery(query, limit);
	    for(YVideo v : vList)
	    {
	       	System.out.println("====================");
		System.out.println(v);
	    }
	} catch (Exception e) {
	    System.out.println("Error " + e.getMessage());
	    e.printStackTrace();
	}
    }

    private static void getCommentThreads(String videoId, long maxResult) {
	try {
	    System.out.println("Fetching comments for videoId: "+videoId);
	    List<YComment> cList = YoutubeDataManager.commentThread(videoId, maxResult);
	    for(YComment c : cList)
	    {
	       	System.out.println("====================");
		System.out.println(c);
	    }
	} catch (Exception e) {
	    System.out.println("Error " + e.getMessage());
	    e.printStackTrace();
	}
    }

    private static void getChannelSubscriptions(String channelId, long maxResult) {
	try {
	    System.out.println("Fetching subscriptions for channel id: "+channelId);
	    List<YSubscription> sList = YoutubeDataManager.getSubscriptions(channelId, maxResult);
	    for(YSubscription s : sList)
	    {
	       	System.out.println("====================");
		System.out.println(s);
	    }
	} catch (Exception e) {
	    System.out.println("Error " + e.getMessage());
	    e.printStackTrace();
	}
    }

    private static void getChannelInfo(String channelId) {
	try {
	    System.out.println("Fetching subscriptions for channel id: "+channelId);
	    YChannel channel = YoutubeDataManager.getChannel(channelId);
	    System.out.println(channel);
	} catch (Exception e) {
	    System.out.println("Error " + e.getMessage());
	    e.printStackTrace();
	}
    }

    private static void getChannelLikes(String channelId, long maxResults) {
	try {
	    System.out.println("Fetching likes for channel id: "+channelId);
	    List<YLike> yList = YoutubeDataManager.getLikes(channelId, maxResults);
	    for(YLike l : yList){
		System.out.println("========================");
		System.out.println(l);
	    }
	} catch (Exception e) {
	    System.out.println("Error: " + e.getMessage());
	    e.printStackTrace();
	}
    }

    private static void getVideoInfo(String videoId) {
	try {
	    System.out.println("Fetching subscriptions for video id: "+videoId);
	    YVideo video = YoutubeDataManager.getVideo(videoId);
	    System.out.println(video);
	} catch (Exception e) {
	    System.out.println("Error " + e.getMessage());
	    e.printStackTrace();
	}
    }


    public static void main(String[] args) {
	CommandQuery commQuery = new CommandQuery();
	CommandComments commComments = new CommandComments();
	CommandSubscriptions commSubscriptions = new CommandSubscriptions();
	CommandChannel commChannel = new CommandChannel();
	CommandLikes commLikes = new CommandLikes();
	CommandVideo commVideo = new CommandVideo();
	
	JCommander jc = JCommander.newBuilder()
	    .addCommand("query",commQuery)
	    .addCommand("comments", commComments)
	    .addCommand("subscriptions", commSubscriptions)
	    .addCommand("channel", commChannel)
	    .addCommand("likes", commLikes)
	    .addCommand("video", commVideo)
	    .build();

	jc.parse(args);
	String parsedComand = jc.getParsedCommand();
	switch (parsedComand) {
	case "query":
	    executeQuery(commQuery.query, commQuery.maxVideos);
	    break;
	case "comments":
	    getCommentThreads(commComments.video, commComments.limit);
	    break;
	case "subscriptions":
	    getChannelSubscriptions(commSubscriptions.channelId, commSubscriptions.limit);
	    break;
	case "channel":
	    getChannelInfo(commChannel.channelId);
	    break;
	case "likes":
	    getChannelLikes(commLikes.channelId, commLikes.limit);
	    break;
	case "video":
	    getVideoInfo(commVideo.videoId);
	default:
	    System.out.println("Unrecognized option");
	}
	
    }
}

@Parameters(separators = "=", commandDescription = "Execute a query on youtube and obtain the related videos")
class CommandQuery {
    @Parameter(names={"--query", "-q"}, description = "Query to be executed")
    public String query;

    @Parameter(names ={"--max-results", "--mr"}, description = "Maximum number of videos")
    public long maxVideos = Long.MAX_VALUE;
}

@Parameters(separators = "=", commandDescription = "Get the comment associated with a video")
class CommandComments {
    @Parameter(names = {"--videoId", "-vid"}, description = "id of the video")
    public String video;

    @Parameter(names = {"--max-results", "-mr"}, description = "maximum number of threads")
    public long limit = Long.MAX_VALUE;
    
}

@Parameters(separators = "=", commandDescription = "Get the subscriptions of a user/channel")
class CommandSubscriptions {
    @Parameter(names = {"--channelId", "-c" }, description = "id of the channel")
    public String channelId;

    @Parameter(names = {"--max-results", "--mr"}, description = "maximum number of subscriptions")
    public long limit = Long.MAX_VALUE;
}

@Parameters(separators = "=", commandDescription = "Get the channel information")
class CommandChannel {
    @Parameter(names = { "--channelId", "-c" }, description = "id of the channel")
    public String channelId;
}

@Parameters(separators = "=", commandDescription = "Get the likes of a user")
class CommandLikes {
    @Parameter(names = { "--channelId", "-c" }, description = "id of the channel")
    public String channelId;

    @Parameter(names = {"--max-results", "-mr"}, description = "maximum number of likes")
    public long limit = Long.MAX_VALUE;
}

@Parameters(separators = "=", commandDescription = "Get info about a video")
class CommandVideo {
    @Parameter(names = { "--videoId", "-vid" }, description = "id of the video")
    public String videoId;

}

    

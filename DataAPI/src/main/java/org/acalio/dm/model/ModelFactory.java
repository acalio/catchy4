/**
 * File containinga set a collection of
 * simple static utility methods for creating DataModel objects startging
 * from the objects obtained by the youtube API
 */
package org.acalio.dm.model;

import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.math.BigInteger;
import java.time.Duration;
import java.util.LinkedList;
import java.util.Optional;

import com.google.api.client.util.DateTime;
import com.google.api.services.youtube.model.Activity;
import com.google.api.services.youtube.model.Channel;
import com.google.api.services.youtube.model.Comment;
import com.google.api.services.youtube.model.SearchResult;
import com.google.api.services.youtube.model.Subscription;
import com.google.api.services.youtube.model.Video;
import com.google.api.services.youtube.model.VideoContentDetails;
import com.google.api.services.youtube.model.VideoSnippet;
import com.google.api.services.youtube.model.VideoStatistics;

import org.acalio.dm.model.avro.YChannel;
import org.acalio.dm.model.avro.YComment;
import org.acalio.dm.model.avro.YLike;
import org.acalio.dm.model.avro.YSubscription;
import org.acalio.dm.model.avro.YVideo;

public class ModelFactory {

    public static YVideo createVideo(SearchResult video) {
	YVideo.Builder yvideoBuilder = YVideo.newBuilder()
	    .setId(video.getId().getVideoId())
	    .setChannelId(video.getSnippet().getChannelId())
	    .setTitle(video.getSnippet().getTitle())
	    .setDescription(video.getSnippet().getDescription())
	    .setPublishedAt(video.getSnippet().getPublishedAt().getValue());
	return yvideoBuilder.build();
    }

    public static YVideo createVideo(Video video) {
	YVideo.Builder yvideoBuilder = YVideo.newBuilder()
			.setId(video.getId());
	    // .setChannelId(video.getSnippet().getChannelId())
	    // .setTitle(video.getSnippet().getTitle())
	    // .setDescription(video.getSnippet().getDescription())
	    // .setPublishedAt(video.getSnippet().getPublishedAt().getValue())
	    // .setViewCount(video.getStatistics().getViewCount().longValue())
	    // .setLikeCount(video.getStatistics().getLikeCount().longValue())
	    // .setDislikeCount(video.getStatistics().getDislikeCount().longValue());

	VideoSnippet  snippet = video.getSnippet();
	VideoStatistics stats = video.getStatistics();
	VideoContentDetails details = video.getContentDetails();
	
	yvideoBuilder.setChannelId(snippet.getChannelId());
	yvideoBuilder.setTitle(snippet.getTitle());
	yvideoBuilder.setDescription(snippet.getDescription());

	DateTime t = null;
	if((t = snippet.getPublishedAt())!=null) 
	    yvideoBuilder.setPublishedAt(t.getValue());
	else 
	    yvideoBuilder.setPublishedAt(0L);
	
	   
	BigInteger value = null;
	if((value = stats.getViewCount())!=null) 
	    yvideoBuilder.setViewCount(value.longValue());
	else 
	    yvideoBuilder.setViewCount(0L);


	if((value = stats.getLikeCount())!=null)
	    yvideoBuilder.setLikeCount(value.longValue());
	else
	    yvideoBuilder.setLikeCount(0L);

	
	if((value = stats.getDislikeCount())!=null)
	    yvideoBuilder.setDislikeCount(value.longValue());
	else
	    yvideoBuilder.setDislikeCount(0L);

	if((value = stats.getCommentCount())!=null)
	    yvideoBuilder.setCommentCount(value.longValue());
	else
	    yvideoBuilder.setCommentCount(0L);
	
	String strValue = null;
	if((strValue=snippet.getDefaultAudioLanguage())!=null)
	    yvideoBuilder.setDefaultAudioLanguage(strValue);
	else
	    yvideoBuilder.setDefaultAudioLanguage("unk");

	if((strValue=snippet.getDefaultLanguage())!=null)
	    yvideoBuilder.setDefaultLanguage(strValue);
	else
	    yvideoBuilder.setDefaultLanguage("unk");

	// Biginteger commentCount = null;
	// if((commentCount = video.getStatistics().getCommentCount())!=null)
	//     yvideoBuilder.setCommentCount(commentCount.longValue());

	// String tmpAudioLanguage = null;
	// if((tmpAudioLanguage=video.getSnippet().getDefaultAudioLanguage())!=null)
	//     yvideoBuilder.setDefaultAudioLanguage(tmpAudioLanguage);

	// String tmpLanguage = null;
	// if((tmpLanguage = video.getSnippet().getDefaultLanguage())!=null)
	//     yvideoBuilder.setDefaultLanguage(tmpLanguage);

	List<String> tags = Optional.ofNullable(snippet.getTags()).orElse(new LinkedList<String>());
	yvideoBuilder.setTags(tags.stream().map(String::toString).collect(Collectors.toList()));

	//get the duration
	Duration duration = Duration.parse(Optional.ofNullable(details.getDuration()).orElse("0"));
	yvideoBuilder.setDuration(duration.toMillis()/1000); //get the durartion in seconds
	    
	return yvideoBuilder.build();
    }
    
    public static YComment createComment(Comment comment) {
	YComment.Builder ycommentBuilder = YComment.newBuilder()
		.setId(comment.getId())
		.setText(comment.getSnippet().getTextOriginal().toString())
		.setVideoId(comment.getSnippet().getVideoId())
	        .setParentID(comment.getId()) // by default the parent of a comment is the comment itself
		.setAuthorChannelUrl(comment.getSnippet().getAuthorChannelUrl())
		.setAuthorDisplayName(comment.getSnippet().getAuthorDisplayName())
		.setLikeCount(comment.getSnippet().getLikeCount())
	        .setPublishedAt(comment.getSnippet().getPublishedAt().getValue());


	//get the authorChannelId object which contains a value field
	String authorChannelId = comment.getSnippet().getAuthorChannelId().toString();
	//extract the field valu
	String id = "unk";
	try {
	    Pattern pattern = Pattern.compile("\\{value=([a-zA-Z-0-9_\\-]+)\\}");
	    Matcher matcher = pattern.matcher(authorChannelId);
	    if (matcher.find()) 
		id = matcher.group(1);

        }catch (Exception e) {}
	ycommentBuilder.setAuthorChannelId(id);
	return ycommentBuilder.build();
    }

    public static YComment createComment(Comment comment, String videoId) {
	comment.getSnippet().setVideoId(videoId);
	YComment yComment = createComment(comment);
	comment.getSnippet().setVideoId(null);
	return yComment;
    }

    public static YSubscription createSubscription(Subscription subscription, String subscriberId) {
	YSubscription.Builder ysubscriptionBuilder = YSubscription.newBuilder()
	    .setSubscriberId(subscriberId)
	    .setChannelId(subscription.getSnippet().getResourceId().getChannelId())
	    .setPublishedAt(subscription.getSnippet().getPublishedAt().getValue());
	return ysubscriptionBuilder.build();
    }


    //in case the subscriptions can't be dervide directly, but via the activity.
    //I am assuming the activity refers to a subscription action
    public static YSubscription createSubscription(Activity activity, String subscriberId) {
	YSubscription.Builder ysubscriptionBuilder = YSubscription.newBuilder()
	    .setSubscriberId(subscriberId)
	    .setChannelId(activity.getContentDetails().getSubscription().getResourceId().getChannelId())
	    .setPublishedAt(activity.getSnippet().getPublishedAt().getValue());
	return ysubscriptionBuilder.build();
    }

    //I am assuming that the activity refers to a like action
    public static YLike createLike(Activity activity, String userId, String type) {
	YLike.Builder ylikeBuilder = YLike.newBuilder()
	    .setUserId(userId)
	    .setPublishedAt(activity.getSnippet().getPublishedAt().getValue());
	switch (type) {
	case "like":
	    ylikeBuilder.setVideoId(activity.getContentDetails().getLike().getResourceId().getVideoId());
	    break;
	case "favorite":
	    ylikeBuilder.setVideoId(activity.getContentDetails().getFavorite().getResourceId().getVideoId());
	   break;
	case "playlistitem":
	    ylikeBuilder.setVideoId(activity.getContentDetails().getPlaylistItem().getResourceId().getVideoId());
	    break;
	case "recommendation":
	    ylikeBuilder.setVideoId(activity.getContentDetails().getRecommendation().getResourceId().getVideoId());
	}
	return ylikeBuilder.build();
    }

    public static YChannel createChannel(Channel channel) {
	YChannel.Builder ychannelBuilder = YChannel.newBuilder()
	    .setId(channel.getId())
	    .setTitle(channel.getSnippet().getTitle())
	    .setDescription(channel.getSnippet().getDescription())
	    .setCountry(channel.getSnippet().getCountry())
	    .setLikedVideosPlaylistId(channel.getContentDetails().getRelatedPlaylists().getFavorites())
	    .setViewCount(channel.getStatistics().getViewCount().longValue())
	    .setVideoCount(channel.getStatistics().getVideoCount().longValue())
	    .setPublishedAt(channel.getSnippet().getPublishedAt().getValue())
	    .setVerified(channel.getStatus().getLongUploadsStatus()=="allowed");// only verified account has this status

	boolean hasHiddenSubscriber = channel.getStatistics().getHiddenSubscriberCount().booleanValue();
	if(hasHiddenSubscriber)
	    ychannelBuilder.setSubscriberCount(-1);
	else
	    ychannelBuilder.setSubscriberCount(channel.getStatistics().getSubscriberCount().longValue());

	ychannelBuilder.setHiddensubscribercount(hasHiddenSubscriber);

	return ychannelBuilder.build();
    }
}





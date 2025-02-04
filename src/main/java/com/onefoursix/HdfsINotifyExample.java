/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.onefoursix;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSInotifyEventInputStream;
import org.apache.hadoop.hdfs.client.HdfsAdmin;
import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.hadoop.hdfs.inotify.Event.CreateEvent;
import org.apache.hadoop.hdfs.inotify.Event.AppendEvent;
import org.apache.hadoop.hdfs.inotify.Event.UnlinkEvent;
import org.apache.hadoop.hdfs.inotify.Event.RenameEvent;
import org.apache.hadoop.hdfs.inotify.Event.CloseEvent;
import org.apache.hadoop.hdfs.inotify.Event.MetadataUpdateEvent;
import org.apache.hadoop.hdfs.inotify.EventBatch;
import org.apache.hadoop.hdfs.inotify.MissingEventsException;
import org.apache.hadoop.hdfs.inotify.Event.CreateEvent.INodeType;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import org.apache.hadoop.hdfs.inotify.Event.CreateEvent.INodeType;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;

public class HdfsINotifyExample {

	private static long lastReadTxid;

	public static void main(String[] args) throws IOException, InterruptedException {

		if (args.length < 4) {
			System.err.println("Usage: HdfsINotifyExample hdfs_url aws_access_key_id secret_access_key sqs_url [txid]");
			System.exit(255);
		}
		lastReadTxid = 0;

		if (args.length > 4) {
			lastReadTxid = Long.parseLong(args[4]);
		}

		System.out.println("lastReadTxid = " + lastReadTxid);

		HdfsAdmin admin = new HdfsAdmin(URI.create(args[0]), new Configuration());


		AwsBasicCredentials awsCreds = AwsBasicCredentials.create(args[1], args[2]);
		SqsClient sqsClient = SqsClient.builder()
						.region(Region.US_EAST_1)
						.credentialsProvider(StaticCredentialsProvider.create(awsCreds)).build();
		while (true) {
			DFSInotifyEventInputStream eventStream = admin.getInotifyEventStream(lastReadTxid);
			theLoop(eventStream, sqsClient, args[3]);
			System.out.println("theLoop exited. Closing, sleeping, restarting...");
			try {
				Thread.sleep(60000);
			} catch (InterruptedException iex) {
			}
		}
	}

	private static void theLoop(DFSInotifyEventInputStream eventStream, SqsClient sqsClient, String qUrl)
				throws IOException, InterruptedException {
		String[] pathRv = new String[1];
		try {
			EventBatch batch = null;
			ArrayList<SendMessageBatchRequestEntry> entries = new ArrayList();
			boolean doBlockingPoll = false;
			while (true) {
				if (doBlockingPoll) {
					System.err.println("Entering poll with block");
					batch = eventStream.poll(60, TimeUnit.SECONDS);
					System.err.println("Finished poll with block");
				} else {
					System.err.println("Entering poll no block");
					batch = eventStream.poll();
				}
				if (batch == null) {
					System.err.println("returned batch is null");
					if (entries.size() > 0) {
						sendBatch(sqsClient, qUrl, entries);
						entries = new ArrayList();
					}
					doBlockingPoll = true;
				} else {
					System.err.println("returned batch is not null");
					long behind = eventStream.getTxidsBehindEstimate();
					System.err.println("getTxidsBehindEstimate=" + behind);
					doBlockingPoll = false;
					lastReadTxid = batch.getTxid();
					for (Event event : batch.getEvents()) {
						System.out.println("TxId = " + lastReadTxid);
						String js = formatToJson(event, pathRv);
						if (js != null) {
							System.out.println("JSON=" + js);
							entries.add(SendMessageBatchRequestEntry.builder().messageGroupId(pathRv[0]).id(String.valueOf(entries.size())).messageDeduplicationId(String.valueOf(lastReadTxid)).messageBody(js).build());
							if (entries.size() == 10) {
								sendBatch(sqsClient, qUrl, entries);
								entries = new ArrayList();
							}
						}
					}
				}
			}
		} catch (Exception mee) {
			System.err.println("Caught mee=" + mee);
		}
	}

	private static void sendBatch(SqsClient sqsClient, String qUrl, ArrayList entries) {
		System.out.println("Sending " + entries.size() + " events");
		SendMessageBatchRequest sendMessageBatchRequest = SendMessageBatchRequest.builder()
				.queueUrl(qUrl)
				.entries(entries)
			.build();
		sqsClient.sendMessageBatch(sendMessageBatchRequest);
	}

	private static String formatiNodeType(INodeType it) {
		if (it == org.apache.hadoop.hdfs.inotify.Event.CreateEvent.INodeType.FILE) {
			return "FILE";
		} else if (it == org.apache.hadoop.hdfs.inotify.Event.CreateEvent.INodeType.DIRECTORY) {
			return "DIRECTORY";
		} else if (it == org.apache.hadoop.hdfs.inotify.Event.CreateEvent.INodeType.SYMLINK) {
			return "SYMLINK";
		} else {
			return "UNKNOWN";
		}
	}

	private static String formatToJson(Event event, String[] pathRv) {
		System.out.println("event type = " + event.getEventType());
		String rv = null;
		switch (event.getEventType()) {
		case CREATE:
			CreateEvent createEvent = (CreateEvent) event;
			System.out.println("  " + createEvent);
			rv = "{\"type\": \"CREATE\","
			       + "\"path\": \"" + createEvent.getPath() + "\","
			       + "\"ctime\": \"" + createEvent.getCtime() + "\","
			       + "\"defaultBlockSize\": \"" + createEvent.getDefaultBlockSize() + "\","
			       + "\"groupName\": \"" + createEvent.getGroupName() + "\","
			       + "\"overwrite\": \"" + createEvent.getOverwrite() + "\","
			       + "\"owner\": \"" + createEvent.getOwnerName() + "\","
			       + "\"perms\": \"" + createEvent.getPerms() + "\","
			       + "\"replication\": \"" + createEvent.getReplication() + "\",";
			if (createEvent.getSymlinkTarget() != null &&createEvent.getSymlinkTarget().length() > 0)
			       rv += "\"symlinkTarget\": \"" + createEvent.getSymlinkTarget() + "\",";
		        rv += "\"inodeType\": \"" + formatiNodeType(createEvent.getiNodeType()) + "\"}";
			pathRv[0] = createEvent.getPath();
			return rv;
		case UNLINK:
			UnlinkEvent unlinkEvent = (UnlinkEvent) event;
			System.out.println("  " + unlinkEvent);
			rv = "{\"type\": \"UNLINK\","
			       + "\"path\": \"" + unlinkEvent.getPath() + "\","
			       + "\"timestamp\": \"" + unlinkEvent.getTimestamp() + "\"}";
			pathRv[0] = unlinkEvent.getPath();
			break;
		case APPEND:
			AppendEvent appendEvent = (AppendEvent) event;
			System.out.println("  " + appendEvent);
			rv = "{\"type\": \"APPEND\","
			       + "\"path\": \"" + appendEvent.getPath() + "\"}";
			pathRv[0] = appendEvent.getPath();
			break;
		case CLOSE:
			CloseEvent closeEvent = (CloseEvent) event;
			System.out.println("  " + closeEvent);
			rv = "{\"type\": \"CLOSE\","
			       + "\"path\": \"" + closeEvent.getPath() + "\","
			       + "\"timestamp\": \"" + closeEvent.getTimestamp() + "\","
			       + "\"fileSize\": \"" + closeEvent.getFileSize() + "\"}";
			pathRv[0] = closeEvent.getPath();
			break;
		case RENAME:
			RenameEvent renameEvent = (RenameEvent) event;
			System.out.println("  " + renameEvent);
			rv = "{\"type\": \"RENAME\","
			       + "\"dstPath\": \"" + renameEvent.getDstPath() + "\","
			       + "\"srcPath\": \"" + renameEvent.getSrcPath() + "\","
			       + "\"timestamp\": \"" + renameEvent.getTimestamp() + "\"}";
			pathRv[0] = renameEvent.getSrcPath();
			break;
		case METADATA:
			MetadataUpdateEvent metadataEvent = (MetadataUpdateEvent) event;
			System.out.println("  " + metadataEvent);
			rv = metadataEventToJson(metadataEvent, pathRv);
			break;
		default:
			System.out.println("WARNING: Unknown event type");
			break;
		}
		return rv;
	}

	private static String metadataEventToJson(MetadataUpdateEvent metadataEvent, String[] pathRv) {
		if (metadataEvent.getMetadataType() == MetadataUpdateEvent.MetadataType.OWNER) {
			pathRv[0] = metadataEvent.getPath();
			return "{\"type\": \"METADATA\","
			       + "\"path\": \"" + metadataEvent.getPath() + "\","
			       + "\"owner\": \"" + metadataEvent.getOwnerName() + "\","
			       + "\"metadataType\": \"" + metadataEvent.getMetadataType() + "\"}";
		} else if (metadataEvent.getMetadataType() == MetadataUpdateEvent.MetadataType.TIMES) {
			pathRv[0] = metadataEvent.getPath();
			return "{\"type\": \"METADATA\","
			       + "\"path\": \"" + metadataEvent.getPath() + "\","
			       + "\"mtime\": \"" + metadataEvent.getMtime() + "\","
			       + "\"atime\": \"" + metadataEvent.getAtime() + "\","
			       + "\"metadataType\": \"" + metadataEvent.getMetadataType() + "\"}";
		} else if (metadataEvent.getMetadataType() == MetadataUpdateEvent.MetadataType.REPLICATION) {
			pathRv[0] = metadataEvent.getPath();
			return "{\"type\": \"METADATA\","
			       + "\"path\": \"" + metadataEvent.getPath() + "\","
			       + "\"replication\": \"" + metadataEvent.getReplication() + "\","
			       + "\"metadataType\": \"" + metadataEvent.getMetadataType() + "\"}";
		} else if (metadataEvent.getMetadataType() == MetadataUpdateEvent.MetadataType.PERMS) {
			pathRv[0] = metadataEvent.getPath();
			return "{\"type\": \"METADATA\","
			       + "\"path\": \"" + metadataEvent.getPath() + "\","
			       + "\"perms\": \"" + metadataEvent.getPerms() + "\","
			       + "\"metadataType\": \"" + metadataEvent.getMetadataType() + "\"}";
		} else if (metadataEvent.getMetadataType() == MetadataUpdateEvent.MetadataType.XATTRS) {
			pathRv[0] = metadataEvent.getPath();
			return "{\"type\": \"METADATA\","
			       + "\"path\": \"" + metadataEvent.getPath() + "\","
			       + "\"xattrs\": \"" + metadataEvent.getxAttrs() + "\","
			       + "\"isxattrsRemoved\": \"" + metadataEvent.isxAttrsRemoved() + "\","
			       + "\"metadataType\": \"" + metadataEvent.getMetadataType() + "\"}";
		} else {
			pathRv[0] = metadataEvent.getPath();
			return "{\"type\": \"METADATA\","
			       + "\"path\": \"" + metadataEvent.getPath() + "\","
			       + "\"metadataType\": \"" + metadataEvent.getMetadataType() + "\"}";
		}
	}
}


/*
 * Copyright 1&1 Internet AG, https://github.com/1and1/
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.oneandone.incubator.neo.http.sink;

import java.io.ByteArrayOutputStream;



import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Closeables;

import jersey.repackaged.com.google.common.collect.Lists;
import net.oneandone.incubator.neo.collect.Immutables;
import net.oneandone.incubator.neo.http.sink.HttpSink.Method;
import net.oneandone.incubator.neo.http.sink.HttpQueryExecutor.QueryResponse;



/**
 * Persistent submission task which lives in main memory and on disc
 */
class PersistentSubmission extends TransientSubmission {
    private static final Logger LOG = LoggerFactory.getLogger(PersistentSubmission.class);

    private final SubmissionDir submissionDir;
    private final CompletableFuture<PersistentSubmissionTask> initialTaskFuture;

    
    /**
     * @param submissionMonitor the submission monitor
     * @param id                the id 
     * @param target            the target uri
     * @param method            the method
     * @param entity            the entity
     * @param rejectStatusList  the reject status list
     * @param processDelays     the process delays
     * @param submissionsStore  the submissions store 
     */
    public PersistentSubmission(final SubmissionMonitor submissionMonitor,
		    			        final String id,
    					  		final URI target,
    					  		final Method method,
    					  		final Entity<?> entity, 
    					  		final ImmutableSet<Integer> rejectStatusList,
    					  		final ImmutableList<Duration> processDelays,
    					  		final SubmissionsStore submissionsStore) {
    	this(submissionMonitor,
    	     id, 
    	     target,
    	     method, 
    	     entity,
    	     rejectStatusList, 
    	     processDelays, 
    	     ImmutableList.of(),
    	     ImmutableList.of(),
    	     submissionsStore);
    }
    
	
    /**
     * @param submissionMonitor the submission monitor
     * @param id                the id 
     * @param target            the target uri
     * @param method            the method
     * @param entity            the entity
     * @param rejectStatusList  the reject status list
     * @param processDelays     the process delays
     * @param lastTrials        the date of the last trials
     * @param actionLog         the action log 
     * @param submissionsStore  the submissions store 
     */
    private PersistentSubmission(final SubmissionMonitor submissionMonitor,
		     			         final String id,
    					  		 final URI target,
    					  		 final Method method,
    					  		 final Entity<?> entity, 
    					  		 final ImmutableSet<Integer> rejectStatusList,
    					  		 final ImmutableList<Duration> processDelays,
    					  		 final ImmutableList<Instant> lastTrials,
    					  		 final ImmutableList<String> actionLog,
    					  		 final SubmissionsStore submissionsStore) {
    	this(submissionMonitor,
    		 id,
    		 target, 
    		 method,
    		 entity,
    		 rejectStatusList, 
    		 processDelays,
    		 lastTrials,
    		 actionLog,
    		 submissionsStore.openSubmissionDir(id));           
    }

    /**
     * @param submissionMonitor the submission monitor
     * @param id                the id 
     * @param target            the target uri
     * @param method            the method
     * @param entity            the entity
     * @param rejectStatusList  the reject status list
     * @param processDelays     the process delays
     * @param lastTrials        the date of the last trials
     * @param actionLog         the action log  
     * @param submissionDir     the submission dir 
     */
    private PersistentSubmission(final SubmissionMonitor submissionMonitor,
		      				     final String id,
		      				     final URI target,
		      				     final Method method,
		      				     final Entity<?> entity, 
		      				     final ImmutableSet<Integer> rejectStatusList,
		      				     final ImmutableList<Duration> processDelays,
		      				     final ImmutableList<Instant> lastTrials,
		      				     final ImmutableList<String> actionLog,
		      				     final SubmissionDir submissionDir) {
    	super(submissionMonitor, id, target, method, entity, rejectStatusList, processDelays, lastTrials, actionLog);
    	this.submissionDir = submissionDir;
    	this.initialTaskFuture = CompletableFuture.supplyAsync(() -> new PersistentSubmissionTask(getLastTrials().size()));
    }
    
    /**
     * @return the submission dir
     */
    public SubmissionDir getSubmissionDir() {
    	return submissionDir;
    }

    @Override
    protected void onReleased() {
    	submissionDir.close();
    }
    

	@Override
	public CompletableFuture<Submission> processAsync(final HttpQueryExecutor executor) {
		return initialTaskFuture.thenCompose(task -> task.processAsync(executor));
    }    
    
    /**
     * loads a persistent task from disc 
     * 
     * @param submissionMonitor the submission monitor
     * @param submissionDir     the submission dir 
     * @param submissionFile    the submission task file
     * @return the persistent submission
     */
    static final PersistentSubmission load(final SubmissionMonitor submissionMonitor,
		    						       final SubmissionDir submissionDir, 
		    							   final File submissionFile) {
    	final Chunk chunk = submissionDir.loadFromDisc(submissionFile);
        return new PersistentSubmission(submissionMonitor, 
        		  			            chunk.read("id"), 
        		  			            chunk.readURI("target"),
        		  			            chunk.readMethod("method"),
        		  			            chunk.readEntity("data"),
        		  			            chunk.readIntegerSet("rejectStatusList"),
        		  			            chunk.readDurationList("retries"),
        		  			            chunk.readInstantList("lastTrials"),
        		  			            chunk.readTextList("actionLog"),
        		  			            submissionDir);
    }
    
	
    /**
     * Persistent submission task
     */
	final class PersistentSubmissionTask extends TransientSubmissionTask {
	    private final File submissionFile;
	    
	    /**
	     * constructor
	     * @param numTrials    the current num of retry 
	     */
	    private PersistentSubmissionTask(final int numTrials) {
	    	super(numTrials);
	        this.submissionFile = saveOnDisc();  // saves the task on disc by creating it
	    }
	    
	    /**
	     * constructor
	     * @param numTrials      the current num of retry 
	     * @param submissionFile the submission task file
	     */
	    private PersistentSubmissionTask(final int numTrials, final File submissionFile) {
	    	super(numTrials);
	        this.submissionFile = submissionFile;
	    }
	    
	    protected TransientSubmissionTask newTask(final int numTrials) {
	    	return new PersistentSubmissionTask(numTrials);
	    }
	    
	    private final File saveOnDisc() {
	    	return submissionDir.saveToDisc(Chunk.newChunk()
	    								         .with("id", getId())
	    								         .with("method", getMethod())
	    								         .with("target", getTarget())
	    								         .with("data", getEntity())
	    								         .withDurationList("retries", getProcessDelays())
	    								         .withInstantList("lastTrials", getLastTrials())
	    								         .withTextList("actionLog", getActionLog())
	    								         .withIntegerList("rejectStatusList", getRejectStatusList()));
	    }    
	    
	    /**
	     * @return submission task file
	     */
	    public File getFile() {
	    	return submissionFile;
	    }
	    
	    @Override
	    protected void onSuccess(final String msg) {
	    	super.onSuccess(msg);
	    	submissionDir.delete();
	    }
	    
	    @Override
	    protected RuntimeException onDiscard(String msg, QueryResponse response) {
	    	final RuntimeException error = super.onDiscard(msg, response);
	    	submissionDir.delete();
	    	return error; 
	    }
	}
	
	
	/**
	 * The store used to persist submissions 
	 */
	static final class SubmissionsStore {
		private final File submissionsStoreDir;
		
		/**
		 * @param persistencyDir  the persistency dir 
		 * @param target          the target uri
		 * @param method          the method
		 */
		public SubmissionsStore(final File persistencyDir, final URI target, final Method method) {
			submissionsStoreDir = new File(persistencyDir, method + "_" + Base64.getEncoder()
																			    .encodeToString(target.toString()
																			    					  .getBytes(Charsets.UTF_8))
																			    					   .replace("=", ""));
	    	if (!submissionsStoreDir.exists()) {
	    		submissionsStoreDir.mkdirs();
	    	}           
		}
		
		/**
		 * @return the submissions dir
		 */
		public File asFile() {
			return submissionsStoreDir;
		}
		
		/**
		 * opens a dedicated submission dir
		 * @param id  the submission id 
		 * @return the opened submission dir 
		 */
		public SubmissionDir openSubmissionDir(String id) {
			return new SubmissionDir(submissionsStoreDir, "dir_" + id);
		}
		
		/**
		 * scans the dir for unprocessed submissions (dirs)
		 * @return the unprocessed submission dirs 
		 */
		public ImmutableList<SubmissionDir> scanUnprocessedSubmissionDirs() {
	    	LOG.debug("scanning " + submissionsStoreDir + " for unprocessed submission dirs");

	    	final List<SubmissionDir> dirs = Lists.newArrayList();
	    	for (File file : submissionsStoreDir.listFiles()) {
	    		try {
	    			dirs.add(new SubmissionDir(file));
	    		} catch (RuntimeException e) { 
	    			LOG.info("persistent submission dir " + file + " can not be opened (may be locked or corrupt)");
	    		}
	    	}
	    	
	    	return ImmutableList.copyOf(dirs);
		}
	}

	/**
	 * Submission dir used to store a dedicated submission
	 */
	static final class SubmissionDir {
        private static final String LOGFILENAME = "submission.lock";
        private static final String DELETED_SUFFIX = ".deleted";
	    private static final String SUBMISSION_SUFFIX = ".properties";
	    private static final String TEMP_SUFFIX = ".temp";

	    private final File submissionDir;
	    private final File deleteMarkerFile;
	    private final File lockfile;
    	private final FileChannel fc; 

    	/**
    	 * @param submissionsDir the parent dir
    	 * @param id             the submission id
    	 */
    	public SubmissionDir(File submissionsStoreDir, String id) {
    		this(new File(submissionsStoreDir, id));
    	}
    	
		private SubmissionDir(File submissionDir) {
			this.deleteMarkerFile = new File(submissionDir, "submission" + DELETED_SUFFIX);
			this.submissionDir = submissionDir;
    		if (!submissionDir.exists()) {
    			submissionDir.mkdirs();
    		}
    		
    		// First, try to open the lock. 
    		// As long the submission dir is open it will be locked 
    		this.lockfile = new File(submissionDir, LOGFILENAME);
    		try {
	    		if (lockfile.createNewFile()) {
			    	fc = FileChannel.open(lockfile.toPath(), StandardOpenOption.WRITE);
			    	fc.lock();
			    	fc.write(ByteBuffer.wrap("locked".getBytes(Charsets.UTF_8)));
	    		} else {
	    			throw new RuntimeException("submission dir lockfile " + lockfile.getAbsolutePath() + " already exists");
	    		}
    		} catch (IOException ioe) {
    			throw new RuntimeException(ioe);
    		}
    		
    		// if submission is expired, submission dir will be deleted  
    		if (isExpired()) {
    			delete();
    			throw new RuntimeException("expired submission dir " + getId() + " found " + submissionDir.getAbsolutePath());
    		}
    		
    		LOG.debug("submission dir " + submissionDir.getAbsolutePath() + " opened and locked");
		}
		
    	private String getId() {
    		return submissionDir.getName();
    	}
		
		/**
		 * @return the submission dir for a dedicated submission
		 */
		public File asFile() {
			return submissionDir;
		}
		
		/**
		 * @param chunk  the chunk to save 
		 */
	    public final File saveToDisc(final Chunk chunk) {
	        final File tempFile = new File(submissionDir, "task_" + UUID.randomUUID().toString() + TEMP_SUFFIX);
	        try {
	            FileOutputStream os = null;
	            try {
	                // write the new submission file as e temp file
	                os = new FileOutputStream(tempFile);
	                
	                chunk.writeTo(os, "submission state");
	                os.close();
	                
	                // and commit it by renaming (this renaming approach avoids "half-written" files)
	                final File submissionFile = new File(submissionDir, Instant.now().toEpochMilli() + SUBMISSION_SUFFIX);
	                submissionFile.createNewFile();
	                
	                java.nio.file.Files.move(tempFile.toPath(), submissionFile.toPath(), StandardCopyOption.ATOMIC_MOVE);
	                LOG.debug("submission task file " + submissionFile.getAbsolutePath() + " saved on disc");
	                
	                return submissionFile;
	            } finally {
	                Closeables.close(os, true);  // close os in any case
	            }
	        } catch (final IOException ioe) {
	            LOG.debug("saving submission " + getId() + " failed", ioe);
	            throw new RuntimeException(ioe);
	        }
	    }    
	    
	    public Chunk loadFromDisc(final File submissionFile) {
	        FileInputStream fis = null;
	        try {
	            fis = new FileInputStream(submissionFile);
	            return Chunk.newChunk(fis);
	        } catch (final IOException ioe) {
	            LOG.debug("loading submission task file " + submissionFile.getAbsolutePath() + " failed", ioe);
	            throw new RuntimeException(ioe);
	        } finally {
	            Closeables.closeQuietly(fis);
	        }
	    }
	
    	public boolean isExpired() {
	        return deleteMarkerFile.exists();
    	}
    	
    	public void delete() {
    		// write deleted marker
	        try {
	            deleteMarkerFile.createNewFile();
	        } catch (IOException ignore) { }
	        
	        // try to deleted all file (may fail for exceptional reason)
	        boolean filesDeleted = true;
	        for (File file : submissionDir.listFiles()) {
	            if (!file.getAbsolutePath().equals(deleteMarkerFile.getAbsolutePath())) {
	                filesDeleted = filesDeleted && file.delete();
	            }
	        }
	        
	        // if all files (without deleted marker) are deleted, the dir will be removed  
	        if (filesDeleted) {
	        	deleteMarkerFile.delete();  
	        	close();
	        	
	        	if (submissionDir.delete()) {
	    			LOG.debug("submission dir " + submissionDir.getAbsolutePath() + " removed");                            
	        	}
	        }        
    	}
    	
    	void close() {
    		try {
    			fc.close();
    			lockfile.delete();
    			LOG.debug("submission dir " + submissionDir.getAbsolutePath() + " unlocked and close");
    		} catch (IOException ignore) { }
    	}
    	
    	public Optional<File> getNewestSubmissionFile() {
    		// submission already deleted?
	        if (deleteMarkerFile.exists()) {
	            return Optional.empty();
	            
	        // ..no , still active 
	        } else {
	            Optional<File> submissionFile = Optional.empty();
	            Instant newest = Instant.ofEpochMilli(0); 
	            
	            // find newest file
	            for (File file : submissionDir.listFiles()) {
	                final String name = file.getName(); 
	                if (name.endsWith(SUBMISSION_SUFFIX)) {
	                    Instant time = Instant.ofEpochMilli(Long.parseLong(name.substring(0, SUBMISSION_SUFFIX.length())));
	                    if (time.isAfter(newest)) {
	                        newest = time;
	                        submissionFile = Optional.of(file);
	                    }
	                }
	            }
	            
	            return submissionFile;
	        }
    	} 
    	
    	@Override
    	public String toString() {
    		return submissionDir.getAbsolutePath();
    	}
	}

    
	private static final class Chunk {
		private final ImmutableMap<Object, Object> data;

		private Chunk(ImmutableMap<Object, Object> data) {
			this.data = data;
		}

		public static Chunk newChunk() {
			return new Chunk(ImmutableMap.of());
		}

		public static Chunk newChunk(InputStream is) throws IOException {
            final Properties props = new Properties();
            props.load(is);
			return new Chunk(ImmutableMap.copyOf(props));
		}
		
		public void writeTo(OutputStream os, String comments) throws IOException {
			Properties props = new Properties();
			props.putAll(data);
			props.store(os, comments);
		}
		
		public Chunk with(String name, String value) {
			return new Chunk(Immutables.join(data, name, value));
		}
		
		public Chunk withDurationList(String name, ImmutableList<Duration> value) {
			return with(name, Joiner.on("&")
					   			    .join(value.stream()
					   			    .map(duration -> duration.toMillis())
					   			    .collect(Immutables.toList())));
		}
		
		public Chunk withInstantList(String name, ImmutableList<Instant> value) {
			return with(name, Joiner.on("&").join(value));
		}

		public Chunk withTextList(String name, ImmutableList<String> value) {
			return with(name, Joiner.on(" & ").join(value));
		}

		public Chunk withIntegerList(String name, ImmutableSet<Integer> value) {
			return with(name, Joiner.on("&").join(value));
		}
		
		public Chunk with(String name, URI value) {
			return with(name, value.toString());
		}
		
		public Chunk with(String name, Method value) {
			return with(name, value.toString());
		}
		
		public Chunk with(String name, Entity<?> value) {
			return with(name, EntitySerializer.serialize(value));
		}
		
		public String read(String name) {
			return (String) data.get(name);
		}

		public URI readURI(String name) {
			return URI.create(read(name));
		}

		public Method readMethod(String name) {
			return Method.valueOf(read(name)); 
		}
		
		public Entity<?> readEntity(String name) {
			return EntitySerializer.deserialize(read(name));
		}
		
		public ImmutableList<Duration> readDurationList(String name) {
			String row = read(name);
			return Strings.isNullOrEmpty(row) ? ImmutableList.<Duration>of()  
											  : Splitter.on("&")
											  			.trimResults()	
											  			.splitToList(row)
											  			.stream()
											  			.map(value -> Duration.ofMillis(Long.parseLong(value)))
											  			.collect(Immutables.toList());
		}
		
		public ImmutableList<Instant> readInstantList(String name) {
			String row = read(name);
			return Strings.isNullOrEmpty(row) ? ImmutableList.<Instant>of()  
											  : Splitter.on("&")
											  			.trimResults()	
											  			.splitToList(row)
											  			.stream()
											  			.map(value -> Instant.parse(value))
											  			.collect(Immutables.toList());
		}
		
		public ImmutableList<String> readTextList(String name) {
			String row = read(name);
			return Strings.isNullOrEmpty(row) ? ImmutableList.<String>of()  
											  : Splitter.on("&")
											  			.trimResults()	
											  			.splitToList(row)
											  			.stream()
											  			.map(value -> value)
											  			.collect(Immutables.toList());
		}
		
		public ImmutableSet<Integer> readIntegerSet(String name) {
			return Splitter.on(" & ")
						   .trimResults()
						   .splitToList(read(name))
						   .stream()
						   .map(status -> Integer.parseInt(status))
						   .collect(Immutables.toSet());
		}
		
	    private static final class EntitySerializer {
	    	private static final String SEPARATOR = "#";
	    	
	    	private EntitySerializer() { }
	    	   
	    	public static String serialize(Entity<?> entity) {
	    		final ByteArrayOutputStream bos = new ByteArrayOutputStream();
	    		try {
					new ObjectMapper().writeValue(bos, entity.getEntity());
		    		bos.flush();
		    		return entity.getMediaType().toString() + 
		    			   SEPARATOR +
		    			   Base64.getEncoder().encodeToString(bos.toByteArray());
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
	    	}
	    	
	    	public static Entity<?> deserialize(String serialized) {
	    		final int idx = serialized.indexOf("#");
	    		final String mediaType = serialized.substring(0, idx);
				final byte[] data = Base64.getDecoder().decode(serialized.substring(idx + 1, serialized.length())); 
	    		return Entity.entity(data, MediaType.valueOf(mediaType)); 
	    	}
	    }
	}
}  
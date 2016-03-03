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
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Closeables;

import jersey.repackaged.com.google.common.collect.Lists;
import net.oneandone.incubator.neo.collect.Immutables;
import net.oneandone.incubator.neo.http.sink.HttpSink.Method;



class PersistentSubmission extends TransientSubmission {
    private static final Logger LOG = LoggerFactory.getLogger(PersistentSubmission.class);

    private final SubmissionDir submissionDir;
	
    public PersistentSubmission(final String id,
    					  		final URI target,
    					  		final Method method,
    					  		final Entity<?> entity, 
    					  		final ImmutableSet<Integer> rejectStatusList,
    					  		final ImmutableList<Duration> processDelays,
    					  		final SubmissionsDir submissionsDir) {
    	this(id, target, method, entity, rejectStatusList, processDelays, submissionsDir.open(id));           
    }

    public PersistentSubmission(final String id,
	  							final URI target,
	  							final Method method,
	  							final Entity<?> entity, 
	  							final ImmutableSet<Integer> rejectStatusList,
	  							final ImmutableList<Duration> processDelays,
	  							final SubmissionDir submissionDir) {
    	super(id, target, method, entity, rejectStatusList, processDelays);
    	this.submissionDir = submissionDir;
    }
    
    public SubmissionDir getSubmissionDir() {
    	return submissionDir;
    }
    
    CompletableFuture<SubmissionTask> newSubmissionTaskAsync() {
    	return CompletableFuture.supplyAsync(() -> new PersistentSubmissionTask()); 
    }
   
    static final PersistentSubmissionTask load(final SubmissionDir submissionDir, final File submissionFile) {
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(submissionFile);
            final Properties props = new Properties();
            props.load(fis);
            
            final String retries = props.getProperty("retries");
            final PersistentSubmission submission =  new PersistentSubmission(props.getProperty("id"), 
            																  URI.create(props.getProperty("target")),
            																  Method.valueOf(props.getProperty("method")),
            																  EntitySerializer.deserialize(props.getProperty("data")),
            																  Splitter.on("&")
					   											  	      			  .trimResults()
					   											  	      			  .splitToList(props.getProperty("rejectStatusList"))
					   											  	      			  .stream()
					   											  	      			  .map(status -> Integer.parseInt(status))
					   											  	      			  .collect(Immutables.toSet()),
					   											  	      	  Strings.isNullOrEmpty(retries) ? ImmutableList.<Duration>of()  
					   													  						 		     : Splitter.on("&")
					   													  						 		               .trimResults()	
					   													  						 		               .splitToList(retries)
					   													  						 		               .stream()
					   													  						 		               .map(millis -> Duration.ofMillis(Long.parseLong(millis)))
					   													  						 		               .collect(Immutables.toList()),
					   													  	  submissionDir);
            
            return submission.new PersistentSubmissionTask(Integer.parseInt(props.getProperty("numTrials")),
            											   Instant.parse(props.getProperty("dataLastTrial")));
        } catch (final IOException ioe) {
            LOG.debug("loading persistent submission " + submissionFile.getAbsolutePath() + " failed", ioe);
            throw new RuntimeException(ioe);
        } finally {
            Closeables.closeQuietly(fis);
        }
    }
    
	
	final class PersistentSubmissionTask extends TransientSubmissionTask {
	    private final File submissionFile;
	    
	    private PersistentSubmissionTask() {
	    	this(0,                                     // no trials performed yet
		    	 Instant.now());                        // last trial time is now (time starting point is now) 
	    }
	    
	    private PersistentSubmissionTask(final int numRetries, final Instant dataLastTrial) {
	        super(numRetries, dataLastTrial);
	        this.submissionFile = save();
	    }
	    
	    private final File save() {
	        final File tempFile = submissionDir.newTempTaskFile();
	        try {
	            FileOutputStream os = null;
	            try {
	                // write the new cache file
	                os = new FileOutputStream(tempFile);
	                
	                Properties props = new Properties();
	                props.setProperty("id", getId());
	                props.setProperty("method", getMethod().toString());
	                props.setProperty("target", getTarget().toString());
	                props.setProperty("data", EntitySerializer.serialize(getEntity()));
	                props.setProperty("retries", Joiner.on("&")
	                                                   .join(getProcessDelays().stream()
	                                                                           .map(duration -> duration.toMillis())
	                                                                           .collect(Immutables.toList())));
	                props.setProperty("numTrials", Integer.toString(numTrials));
	                props.setProperty("dataLastTrial", dateLastTrial.toString());
	                props.setProperty("rejectStatusList", Joiner.on("&").join(getRejectStatusList()));
	
	                props.store(os, "submission state");
	                os.close();
	                
	
	                // and commit it (this renaming approach avoids "half-written" files)
	                final File submissionFile = submissionDir.newTaskFile();
	                submissionFile.createNewFile();
	                java.nio.file.Files.move(tempFile.toPath(), submissionFile.toPath(), StandardCopyOption.ATOMIC_MOVE);
	                LOG.debug("persistent query " + getId() + " saved on disc " + submissionFile.getAbsolutePath());
	                
	                return submissionFile;
	            } finally {
	                Closeables.close(os, true);  // close os in any case
	            }
	
	        } catch (final IOException ioe) {
	            LOG.debug("saving persistent submission " + getId() + " failed", ioe);
	            throw new RuntimeException(ioe);
	        }
	    }    
	    
	    public File getFile() {
	    	return submissionFile;
	    }
	    
	    @Override
	    protected void terminate() {
	    	super.terminate();
	    	submissionDir.delete();
	    }
	    
	    @Override
	    public void release() {
	    	super.release();
	    	submissionDir.close();
	    }
	
	    /*
	    private void deletePersistentSubmission() {
	        // write deleted marker
	        File deletedFile = newDeleteMarkerFile(submissionDir);
	        try {
	            deletedFile.createNewFile();
	        } catch (IOException ignore) { }
	        
	        // try to deleted all file (may fail for exceptional reason)
	        boolean filesDeleted = true;
	        for (File file : submissionDir.listFiles()) {
	            if (!file.getAbsolutePath().equals(deletedFile.getAbsolutePath())) {
	                filesDeleted = filesDeleted && file.delete();
	            }
	        }
	        
	        if (filesDeleted) {
	        	release();
	        	deletedFile.delete();
	        	if (submissionDir.delete()) {
	        		LOG.debug("persistent query " + getId() + " deleted " + submissionDir.getAbsolutePath());                            
	        	}
	        }        
	    }
	     */
	 
	    @Override
	    protected SubmissionTask copySubmissionTask(final int numTrials, final Instant dateLastTrial) {
	        return new PersistentSubmissionTask(numTrials,  dateLastTrial);
	    }
	  
	    /*
	    public Optional<File> getFile() {
	        return getFile(submissionDir);
	    }
	    
	    private Optional<File> getFile(final File submissionDir) {
	        if (newDeleteMarkerFile(submissionDir).exists()) {
	            return Optional.empty();
	        } else {
	            Optional<File> submissionFile = Optional.empty();
	            Instant newest = Instant.ofEpochMilli(0); 
	            
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
	    }*/
	}
	
	
	
	static final class SubmissionsDir {
		private final File submissionsDir;
		
		public SubmissionsDir(final File persistencyDir, final URI target, final Method method) {
			submissionsDir = new File(persistencyDir, method + "_" + Base64.getEncoder().encodeToString(target.toString().getBytes(Charsets.UTF_8)).replace("=", ""));
	    	if (!submissionsDir.exists()) {
	    		submissionsDir.mkdirs();
	    	}           
		}
		
		public File getDir() {
			return submissionsDir;
		}
		
		public SubmissionDir open(String id) {
			return new SubmissionDir(submissionsDir, id);
		}
		
		public ImmutableList<SubmissionDir> scanUnprocessedSubmissionDirs() {
	    	LOG.debug("scanning " + submissionsDir + " for unprocessed submissions");

	    	final List<SubmissionDir> dirs = Lists.newArrayList();
	    	for (File file : submissionsDir.listFiles()) {
	    		try {
	    			dirs.add(new SubmissionDir(file));
	    		} catch (RuntimeException igmore) { }
	    	}
	    	
	    	return ImmutableList.copyOf(dirs);
		}
	}

	
	static final class SubmissionDir {
        private static final String LOGFILENAME = "submission.lock";
        private static final String DELETED_SUFFIX = ".deleted";
	    private static final String SUBMISSION_SUFFIX = ".properties";
	    private static final String TEMP_SUFFIX = ".temp";


		private final File submissionDir;
    	private final File lockfile;
    	private final FileChannel fc; 

    	public SubmissionDir(File submissionsDir, String id) {
    		this(new File(submissionsDir, id));
    	}
    		
		
		private SubmissionDir(File submissionDir) {
			this.submissionDir = submissionDir;
    		if (!submissionDir.exists()) {
    			submissionDir.mkdirs();
    		}
    		
    		this.lockfile = new File(submissionDir, LOGFILENAME);
    		
    		try {
	    		if (lockfile.createNewFile()) {
			    	fc = FileChannel.open(lockfile.toPath(), StandardOpenOption.WRITE);
			    	fc.lock();
			    	fc.write(ByteBuffer.wrap("locked".getBytes(Charsets.UTF_8)));
	    		} else {
	    			throw new RuntimeException("lockfile " + lockfile.getAbsolutePath() + " already exists");
	    		}
    		} catch (IOException ioe) {
    			throw new RuntimeException(ioe);
    		}
		}
		
		public File getDir() {
			return submissionDir;
		}
		
		public File newTempTaskFile() {
			return new File(submissionDir, "task_" + UUID.randomUUID().toString() + TEMP_SUFFIX);
		}

		public File newTaskFile() {
			return new File(submissionDir, Instant.now().toEpochMilli() + SUBMISSION_SUFFIX);
		}
		
    	public void close() {
    		try {
    			fc.close();
    			lockfile.delete();
    			LOG.debug("persistent submission " + getId() + " released " + submissionDir.getAbsolutePath());
    		} catch (IOException ignore) { }
    	}
    	
    	public void delete() {
    		// write deleted marker
	        File deletedFile = newDeleteMarkerFile(submissionDir);
	        try {
	            deletedFile.createNewFile();
	        } catch (IOException ignore) { }
	        
	        // try to deleted all file (may fail for exceptional reason)
	        boolean filesDeleted = true;
	        for (File file : submissionDir.listFiles()) {
	            if (!file.getAbsolutePath().equals(deletedFile.getAbsolutePath())) {
	                filesDeleted = filesDeleted && file.delete();
	            }
	        }
	        
	        if (filesDeleted) {
	        	close();
	        	deletedFile.delete();
	        	if (submissionDir.delete()) {
	        		LOG.debug("persistent submission " + getId() + " deleted " + submissionDir.getAbsolutePath());                            
	        	}
	        }        
    	}
    	
    	public Optional<File> getNewestSubmissionFile() {
	        if (newDeleteMarkerFile(submissionDir).exists()) {
	            return Optional.empty();
	        } else {
	            Optional<File> submissionFile = Optional.empty();
	            Instant newest = Instant.ofEpochMilli(0); 
	            
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
    	
    	private final File newDeleteMarkerFile(final File submissionDir) {
    		return new File(submissionDir, "submission" + DELETED_SUFFIX);
  	    }
    	
    	private String getId() {
    		return submissionDir.getName();
    	}
    	
    	@Override
    	public String toString() {
    		return submissionDir.getAbsolutePath();
    	}
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
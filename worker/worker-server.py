import redis
import jsonpickle
import os
import sys
import platform
import base64
import hashlib
import tempfile
import shutil

# Redis configuration
redisHost = os.getenv("REDIS_HOST") or "localhost"
redisPort = int(os.getenv("REDIS_PORT") or 6379)

# Logging
infoKey = f"{platform.node()}.worker.info"
debugKey = f"{platform.node()}.worker.debug"

def log_debug(message):
    print("DEBUG:", message, file=sys.stdout)
    try:
        r = redis.StrictRedis(host=redisHost, port=redisPort, db=0, decode_responses=False)
        r.lpush("logging", f"{debugKey}:{message}")
    except:
        pass

def log_info(message):
    print("INFO:", message, file=sys.stdout)
    try:
        r = redis.StrictRedis(host=redisHost, port=redisPort, db=0, decode_responses=False)
        r.lpush("logging", f"{infoKey}:{message}")
    except:
        pass

def process_job(job):
    try:
        songhash = job['songhash']
        mp3_base64 = job['mp3']
        
        log_info(f"Processing job {songhash}")
        
        # Decode MP3
        mp3_data = base64.b64decode(mp3_base64)
        
        # Create temp directory
        temp_dir = tempfile.mkdtemp()
        input_file = os.path.join(temp_dir, f"{songhash}.mp3")
        
        # Write MP3 to file
        with open(input_file, 'wb') as f:
            f.write(mp3_data)
        
        log_info(f"Saved MP3 to {input_file}")
        
        # Run Demucs (simplified for now - just log)
        log_info(f"Would run: python -m demucs.separate --mp3 --out {temp_dir}/output {input_file}")
        log_info(f"Job {songhash} completed (simulation)")
        
        # Cleanup
        shutil.rmtree(temp_dir)
        
        return True
        
    except Exception as e:
        log_debug(f"Error processing job: {str(e)}")
        return False

if __name__ == "__main__":
    log_info("Worker starting")
    
    redisClient = redis.StrictRedis(host=redisHost, port=redisPort, db=0, decode_responses=False)
    
    while True:
        try:
            # Block and wait for job
            work = redisClient.blpop("toWorker", timeout=0)
            
            if work:
                job_data = work[1]
                job = jsonpickle.decode(job_data.decode('utf-8'))
                
                log_info(f"Received job: {job.get('songhash', 'unknown')}")
                
                process_job(job)
                
        except KeyboardInterrupt:
            log_info("Worker shutting down")
            break
        except Exception as e:
            log_debug(f"Worker error: {str(e)}")
            continue

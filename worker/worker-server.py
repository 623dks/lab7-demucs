import redis
import jsonpickle
import os
import sys
import platform
import base64
import tempfile
import shutil
import subprocess
from minio import Minio

# Configuration
redisHost = os.getenv("REDIS_HOST") or "localhost"
redisPort = int(os.getenv("REDIS_PORT") or 6379)
minioHost = os.getenv("MINIO_HOST") or "localhost:9000"
minioUser = os.getenv("MINIO_USER") or "rootuser"
minioPass = os.getenv("MINIO_PASS") or "rootpass123"

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
        
        # Create temp directories
        temp_dir = tempfile.mkdtemp()
        input_file = os.path.join(temp_dir, f"{songhash}.mp3")
        output_dir = os.path.join(temp_dir, "output")
        
        # Write MP3 to file
        with open(input_file, 'wb') as f:
            f.write(mp3_data)
        
        log_info(f"Saved MP3 to {input_file}")
        
        # Run Demucs using subprocess instead of os.system
        cmd = [
            "python", "-m", "demucs.separate",
            "-n", "htdemucs",
            "--mp3",
            "--out", output_dir,
            input_file
        ]
        
        log_info(f"Running: {' '.join(cmd)}")
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            log_debug(f"Demucs failed with code {result.returncode}")
            log_debug(f"STDERR: {result.stderr}")
            log_debug(f"STDOUT: {result.stdout}")
            shutil.rmtree(temp_dir)
            return False
        
        log_info(f"Demucs STDOUT: {result.stdout}")


        
        # Upload results to MinIO
        minio_client = Minio(
            minioHost,
            access_key=minioUser,
            secret_key=minioPass,
            secure=False
        )
        
        # Find separated tracks
        separated_dir = os.path.join(output_dir, "htdemucs", songhash)
        tracks = ["bass.mp3", "drums.mp3", "vocals.mp3", "other.mp3"]
        
        for track in tracks:
            track_path = os.path.join(separated_dir, track)
            if os.path.exists(track_path):
                object_name = f"{songhash}-{track}"
                minio_client.fput_object("output", object_name, track_path)
                log_info(f"Uploaded {object_name} to MinIO")
        
        # Cleanup
        shutil.rmtree(temp_dir)
        
        log_info(f"Job {songhash} completed successfully")
        return True
        
    except Exception as e:
        log_debug(f"Error processing job: {str(e)}")
        return False

if __name__ == "__main__":
    log_info("Worker starting")
    
    redisClient = redis.StrictRedis(host=redisHost, port=redisPort, db=0, decode_responses=False)
    
    while True:
        try:
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
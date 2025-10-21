from flask import Flask, request, jsonify, send_file
import redis
import hashlib
import base64
import jsonpickle
import os
import sys
import platform

app = Flask(__name__)

# Redis configuration
redisHost = os.getenv("REDIS_HOST") or "localhost"
redisPort = int(os.getenv("REDIS_PORT") or 6379)

# Logging keys
infoKey = f"{platform.node()}.rest.info"
debugKey = f"{platform.node()}.rest.debug"

def log_debug(message):
    print("DEBUG:", message, file=sys.stdout)
    try:
        r = redis.StrictRedis(host=redisHost, port=redisPort, db=0, decode_responses=False)
        r.lpush("logging", f"{debugKey}:{message}")
    except Exception as e:
        print(f"Redis logging error: {e}")

def log_info(message):
    print("INFO:", message, file=sys.stdout)
    try:
        r = redis.StrictRedis(host=redisHost, port=redisPort, db=0, decode_responses=False)
        r.lpush("logging", f"{infoKey}:{message}")
    except Exception as e:
        print(f"Redis logging error: {e}")

# Health check endpoint
@app.route("/", methods=["GET"])
def health_check():
    return "<h1>Music Separation REST API</h1><p>Use /apiv1/separate to upload MP3</p>"

# POST /apiv1/separate - Upload MP3 for separation
@app.route('/apiv1/separate', methods=['POST'])
def separate():
    try:
        data = request.get_json(force=True)
        mp3_data = base64.b64decode(data['mp3'])
        songhash = hashlib.sha256(mp3_data).hexdigest()[:56]
        
        log_info(f"Received separation request for song {songhash}")
        
        callback = data.get('callback', None)
        
        job = {
            'songhash': songhash,
            'mp3': data['mp3'],
            'callback': callback
        }
        
        redisClient = redis.StrictRedis(host=redisHost, port=redisPort, db=0, decode_responses=False)
        redisClient.lpush('toWorker', jsonpickle.encode(job))
        
        log_info(f"Job {songhash} queued to worker")
        
        return jsonify({
            'hash': songhash,
            'reason': 'Song enqueued for separation'
        }), 200
        
    except Exception as e:
        log_debug(f"Error in separate endpoint: {str(e)}")
        return jsonify({'error': str(e)}), 400

# GET /apiv1/queue - Show queued jobs
@app.route('/apiv1/queue', methods=['GET'])
def get_queue():
    try:
        redisClient = redis.StrictRedis(host=redisHost, port=redisPort, db=0, decode_responses=True)
        
        queue_length = redisClient.llen('toWorker')
        queue_items = []
        
        for i in range(queue_length):
            item = redisClient.lindex('toWorker', i)
            if item:
                job = jsonpickle.decode(item)
                queue_items.append(job.get('songhash', 'unknown'))
        
        log_info(f"Queue status requested: {len(queue_items)} items")
        
        return jsonify({
            'queue': queue_items
        }), 200
        
    except Exception as e:
        log_debug(f"Error in queue endpoint: {str(e)}")
        return jsonify({'error': str(e)}), 400

# GET /apiv1/track/<songhash>/<track> - Download separated track
@app.route('/apiv1/track/<songhash>/<track>', methods=['GET'])
def get_track(songhash, track):
    try:
        log_info(f"Track download requested: {songhash}/{track}")
        return jsonify({'error': 'MinIO not configured yet'}), 501
        
    except Exception as e:
        log_debug(f"Error in track endpoint: {str(e)}")
        return jsonify({'error': str(e)}), 400

# GET /apiv1/remove/<songhash> - Remove tracks
@app.route('/apiv1/remove/<songhash>', methods=['GET'])
def remove_track(songhash):
    try:
        log_info(f"Track removal requested: {songhash}")
        return jsonify({'message': f'Track {songhash} removal queued'}), 200
        
    except Exception as e:
        log_debug(f"Error in remove endpoint: {str(e)}")
        return jsonify({'error': str(e)}), 400

if __name__ == "__main__":
    log_info("REST Server starting")
    app.run(host="0.0.0.0", port=5000, debug=True)
import subprocess
import time
import sys

# The name of your main bot file
BOT_SCRIPT = "main.py"
# How often to check GitHub for updates (in seconds)
CHECK_INTERVAL = 60 

def get_commit_hashes():
    """Fetches the latest remote code and compares commit hashes."""
    subprocess.run(["git", "fetch"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    local_hash = subprocess.check_output(["git", "rev-parse", "HEAD"]).strip()
    remote_hash = subprocess.check_output(["git", "rev-parse", "@{u}"]).strip()
    return local_hash, remote_hash

def start_bot():
    print("🚀 Starting the bot...")
    return subprocess.Popen([sys.executable, BOT_SCRIPT])

def main():
    bot_process = start_bot()
    
    try:
        while True:
            time.sleep(CHECK_INTERVAL)
            
            try:
                local_hash, remote_hash = get_commit_hashes()
                
                if local_hash != remote_hash:
                    print("\n📥 New code detected on GitHub! Pulling updates...")
                    subprocess.run(["git", "pull"], stdout=subprocess.PIPE)
                    
                    print("🛑 Stopping current bot...")
                    bot_process.terminate()
                    bot_process.wait() # Wait for it to safely close
                    
                    bot_process = start_bot()
                    print("✅ Bot restarted with new code!\n")
            except Exception as e:
                print(f"⚠️ Error checking for updates: {e}")
                
    except KeyboardInterrupt:
        print("\n🛑 Shutting down launcher and bot...")
        bot_process.terminate()

if __name__ == "__main__":
    main()
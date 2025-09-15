# project_config.py

from dotenv import load_dotenv
import os
import socket

# Get root directory of this file
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

# === Detect environment ===
# Option 1: Check environment variable
env_mode = os.getenv("ENVIRONMENT")

if env_mode == "production":
    dotenv_path = os.path.join(ROOT_DIR, ".env")
    load_dotenv(dotenv_path)
    print("🔧 ENV=production: Using .env (localhost)")

elif env_mode == "development":
    dotenv_path = os.path.join(ROOT_DIR, ".env.local")
    if os.path.exists(dotenv_path):
        load_dotenv(dotenv_path)
        print("💻 ENV=development: Using .env.local (remote IP)")
    else:
        raise FileNotFoundError(f"❌ Forced development but {dotenv_path} not found!")

else:
    # Auto-detect: check if we're on the DB server by comparing IPs
    try:
        # Resolve DB_HOST from .env first to see what it is
        temp_env = os.path.join(ROOT_DIR, ".env")
        load_dotenv(temp_env, override=False)  # Load just to read DB_HOST
        db_host = os.getenv("DB_HOST")

        # Get our own outward IP (optional) or just check if DB_HOST is localhost
        if db_host in ("localhost", "127.0.0.1", "0.0.0.0"):
            # If .env says localhost, assume we're on the same machine
            local_env = os.path.join(ROOT_DIR, ".env.local")
            if os.path.exists(local_env):
                # We are on server, but someone put .env.local — warn
                print("⚠️ .env.local exists but DB_HOST=localhost → likely on server")
                print("   Using .env (localhost) for safety")
                load_dotenv(os.path.join(ROOT_DIR, ".env"))
            else:
                load_dotenv(temp_env)
                print("🚀 Using .env → Production mode (localhost)")
        else:
            # DB_HOST is a remote IP → we must be on a local machine
            local_env = os.path.join(ROOT_DIR, ".env.local")
            if os.path.exists(local_env):
                load_dotenv(local_env)
                print("💡 Using .env.local → Development mode (remote IP)")
            else:
                raise FileNotFoundError("❌ Not in production and .env.local not found!")
    except Exception as e:
        raise RuntimeError(f"Failed to auto-detect environment: {e}")

# === Read DB settings ===
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

if not all([DB_HOST, DB_NAME, DB_USER, DB_PASSWORD]):
    raise EnvironmentError("❌ Missing required DB credentials in environment!")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


holiday_list = ['2024-02-21', 
'2024-03-26', 
'2024-04-09',
 '2024-04-10', 
 '2024-04-11',
 '2024-04-12',
 '2024-04-13',
 '2024-04-14',
 '2024-05-01',
 '2024-06-16',
 '2024-06-17',
 '2024-06-18',
 '2024-06-19',
 '2024-06-20',
 '2024-08-15',
 '2024-12-16',
]

def holiday ():
    return holiday_list
import os
import re
import json
import base64
import asyncio
import logging
import secrets
import datetime
from typing import Optional, Dict, Any, List
from zoneinfo import ZoneInfo

import aiohttp
from fastapi import FastAPI, Request, HTTPException, Depends, Query, Form, Cookie, Path
from fastapi.responses import JSONResponse, RedirectResponse, HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from pyrogram import Client, filters
from pyrogram.types import Message, InputMediaAudio, InputMediaVideo
from pyrogram.enums import ParseMode
from motor.motor_asyncio import AsyncIOMotorClient
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
import humanize

# -------------------
# Basic logger setup
# -------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
logger = logging.getLogger("yt_api")

# -------------------
# Config (from env) - NO HARDCODED CREDENTIALS
# -------------------
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
CACHE_CHANNEL_ID = int(os.getenv("CACHE_CHANNEL_ID"))
MONGO_DB_URI = os.getenv("MONGO_DB_URI")
ADMIN_SECRET = os.getenv("ADMIN_SECRET")
DEFAULT_VIDEO_QUALITY = os.getenv("DEFAULT_VIDEO_QUALITY", "720")
DEFAULT_AUDIO_QUALITY = os.getenv("DEFAULT_AUDIO_QUALITY", "320")
DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR", "./downloads")
SAVETUBE_BASE = "https://media.savetube.me"
WEBSITE_URL = os.getenv("WEBSITE_URL", "https://chacheapi-21117ae61e3f.herokuapp.com")

# Validate required environment variables
if not all([API_ID, API_HASH, BOT_TOKEN, CACHE_CHANNEL_ID, MONGO_DB_URI, ADMIN_SECRET]):
    logger.error("Missing required environment variables")
    raise RuntimeError("Please set all required environment variables")

os.makedirs(DOWNLOAD_DIR, exist_ok=True)
os.makedirs("static", exist_ok=True)
os.makedirs("templates", exist_ok=True)

# -------------------
# MongoDB connection
# -------------------
try:
    mongo_client = AsyncIOMotorClient(MONGO_DB_URI)
    mongodb = mongo_client.yt_api
    logger.info("Connected to MongoDB successfully")
except Exception as e:
    logger.error(f"Failed to connect to MongoDB: {e}")
    raise

# -------------------
# FastAPI setup with templates
# -------------------
app = FastAPI(title="YouTube Downloader Pro - Premium 3D Admin Panel")
app.mount("/static", StaticFiles(directory="static"), name="static")
templates = Jinja2Templates(directory="templates")

# -------------------
# Pyrogram client
# -------------------
pyrogram_client = Client(
    "yt_cache_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    in_memory=True
)

# -------------------
# Constants and utilities
# -------------------
YT_RE = re.compile(
    r'(?:https?://)?(?:www\.)?(?:youtube\.com/(?:watch\?v=|embed/|shorts/)|youtu\.be/)?([0-9A-Za-z_-]{11})'
)

AES_HEX_KEY = "C5D58EF67A7584E4A29F6C35BBC4EB12"
AES_KEY_BYTES = bytes.fromhex(AES_HEX_KEY)

IN_MEMORY_CACHE: Dict[str, Dict[str, Any]] = {}
PROCESSING_SET = set()

# -------------------
# Authentication functions
# -------------------
async def get_current_user(api_key: str = Cookie(None)):
    if not api_key:
        return None
    user = await mongodb.apikeys.find_one({"key": api_key})
    return user

async def get_current_admin(api_key: str = Cookie(None)):
    if not api_key:
        return None
    admin = await mongodb.apikeys.find_one({"key": api_key, "is_admin": True})
    return admin

# -------------------
# Premium 3D HTML Templates
# -------------------
def create_premium_templates():
    # Base template with 3D effects and premium design
    with open("templates/base.html", "w") as f:
        f.write("""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{% block title %}YouTube Downloader Pro{% endblock %}</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <link href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css" rel="stylesheet">
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap" rel="stylesheet">
    <link href="https://unpkg.com/aos@2.3.1/dist/aos.css" rel="stylesheet">
    <style>
        :root {
            --primary: #6366f1;
            --primary-dark: #4f46e5;
            --secondary: #10b981;
            --dark: #1f2937;
            --light: #f9fafb;
            --gradient: linear-gradient(135deg, #6366f1 0%, #8b5cf6 100%);
        }
        
        body {
            font-family: 'Inter', sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            color: white;
        }
        
        .glass-card {
            background: rgba(255, 255, 255, 0.1);
            backdrop-filter: blur(10px);
            border-radius: 20px;
            border: 1px solid rgba(255, 255, 255, 0.2);
            box-shadow: 0 8px 32px rgba(0, 0, 0, 0.1);
        }
        
        .nav-glass {
            background: rgba(255, 255, 255, 0.1);
            backdrop-filter: blur(15px);
            border-bottom: 1px solid rgba(255, 255, 255, 0.2);
        }
        
        .btn-premium {
            background: var(--gradient);
            border: none;
            border-radius: 12px;
            padding: 12px 30px;
            font-weight: 600;
            transition: all 0.3s ease;
            box-shadow: 0 4px 15px rgba(99, 102, 241, 0.3);
        }
        
        .btn-premium:hover {
            transform: translateY(-2px);
            box-shadow: 0 8px 25px rgba(99, 102, 241, 0.4);
        }
        
        .stat-card {
            background: rgba(255, 255, 255, 0.15);
            border-radius: 16px;
            padding: 25px;
            transition: all 0.3s ease;
        }
        
        .stat-card:hover {
            transform: translateY(-5px);
            background: rgba(255, 255, 255, 0.2);
        }
        
        .feature-icon {
            width: 60px;
            height: 60px;
            background: var(--gradient);
            border-radius: 50%;
            display: flex;
            align-items: center;
            justify-content: center;
            margin-bottom: 20px;
            font-size: 24px;
        }
        
        .download-btn {
            background: var(--gradient);
            padding: 15px 40px;
            border-radius: 50px;
            font-weight: 600;
            font-size: 18px;
            transition: all 0.3s ease;
        }
        
        .download-btn:hover {
            transform: scale(1.05);
            box-shadow: 0 10px 30px rgba(99, 102, 241, 0.4);
        }
        
        .3d-text {
            text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
            font-weight: 700;
        }
    </style>
</head>
<body>
    <nav class="navbar navbar-expand-lg navbar-dark nav-glass">
        <div class="container">
            <a class="navbar-brand fw-bold" href="/">
                <i class="fab fa-youtube me-2"></i>YouTube Downloader Pro
            </a>
            <div class="navbar-nav ms-auto">
                {% if user %}
                <a class="nav-link" href="/dashboard"><i class="fas fa-tachometer-alt me-1"></i>Dashboard</a>
                {% if user.is_admin %}
                <a class="nav-link" href="/admin"><i class="fas fa-crown me-1"></i>Admin Panel</a>
                {% endif %}
                <a class="nav-link" href="/logout"><i class="fas fa-sign-out-alt me-1"></i>Logout</a>
                {% else %}
                <a class="nav-link" href="/login"><i class="fas fa-sign-in-alt me-1"></i>Login</a>
                <a class="nav-link" href="/register"><i class="fas fa-user-plus me-1"></i>Register</a>
                {% endif %}
            </div>
        </div>
    </nav>

    {% block content %}{% endblock %}

    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
    <script src="https://unpkg.com/aos@2.3.1/dist/aos.js"></script>
    <script>
        AOS.init();
        // Real-time stats update
        function updateRealTimeStats() {
            fetch('/api/stats')
                .then(response => response.json())
                .then(data => {
                    if (data.total_downloads) {
                        document.getElementById('total-downloads').textContent = data.total_downloads.toLocaleString();
                    }
                    if (data.today_usage) {
                        document.getElementById('today-usage').textContent = data.today_usage.toLocaleString();
                    }
                });
        }
        setInterval(updateRealTimeStats, 30000);
    </script>
</body>
</html>""")

    # Home page with premium design
    with open("templates/index.html", "w") as f:
        f.write("""{% extends "base.html" %}

{% block content %}
<div class="container py-5">
    <div class="text-center mb-5" data-aos="fade-up">
        <h1 class="display-3 fw-bold mb-3 3d-text">YouTube Downloader Pro</h1>
        <p class="lead mb-4">Download YouTube videos and audio in highest quality with lightning speed</p>
        
        <div class="row mb-5">
            <div class="col-md-3" data-aos="fade-up" data-aos-delay="100">
                <div class="feature-icon mx-auto">
                    <i class="fas fa-bolt"></i>
                </div>
                <h5>Lightning Fast</h5>
                <p class="text-muted">Instant downloads with our premium servers</p>
            </div>
            <div class="col-md-3" data-aos="fade-up" data-aos-delay="200">
                <div class="feature-icon mx-auto">
                    <i class="fas fa-hd"></i>
                </div>
                <h5>Highest Quality</h5>
                <p class="text-muted">4K video & 320kbps audio quality</p>
            </div>
            <div class="col-md-3" data-aos="fade-up" data-aos-delay="300">
                <div class="feature-icon mx-auto">
                    <i class="fas fa-infinity"></i>
                </div>
                <h5>Unlimited</h5>
                <p class="text-muted">No daily limits for premium users</p>
            </div>
            <div class="col-md-3" data-aos="fade-up" data-aos-delay="400">
                <div class="feature-icon mx-auto">
                    <i class="fas fa-shield-alt"></i>
                </div>
                <h5>Secure</h5>
                <p class="text-muted">Your data is always protected</p>
            </div>
        </div>
    </div>

    <div class="row justify-content-center">
        <div class="col-lg-8">
            <div class="glass-card p-5" data-aos="zoom-in">
                <h3 class="text-center mb-4"><i class="fas fa-download me-2"></i>Download Content</h3>
                
                <form id="downloadForm" class="row g-3">
                    <div class="col-12">
                        <label class="form-label">YouTube URL:</label>
                        <input type="url" class="form-control form-control-lg bg-dark text-white border-0" 
                               placeholder="https://www.youtube.com/watch?v=..." required
                               style="background: rgba(0,0,0,0.3) !important;">
                    </div>
                    
                    <div class="col-md-6">
                        <label class="form-label">Format:</label>
                        <select class="form-select form-select-lg bg-dark text-white border-0" id="format">
                            <option value="mp3">🎵 MP3 (Audio)</option>
                            <option value="mp4">🎥 MP4 (Video)</option>
                        </select>
                    </div>
                    <div class="col-md-6">
                        <label class="form-label">Quality:</label>
                        <select class="form-select form-select-lg bg-dark text-white border-0" id="quality">
                            <option value="max">🔥 Maximum Quality</option>
                            <option value="1080">🎬 1080p HD</option>
                            <option value="720">📺 720p HD</option>
                            <option value="480">📱 480p</option>
                        </select>
                    </div>

                    {% if user %}
                    <div class="col-12 text-center mt-4">
                        <button type="submit" class="download-btn">
                            <i class="fas fa-download me-2"></i>DOWNLOAD NOW
                        </button>
                    </div>
                    {% else %}
                    <div class="col-12">
                        <div class="alert alert-warning text-center">
                            <i class="fas fa-lock me-2"></i>Please <a href="/login" class="alert-link">login</a> to start downloading
                        </div>
                    </div>
                    {% endif %}
                </form>

                <div id="result" class="mt-4 text-center" style="display: none;">
                    <div class="spinner-border text-light" role="status">
                        <span class="visually-hidden">Loading...</span>
                    </div>
                    <p class="mt-2">Processing your request...</p>
                </div>
            </div>
        </div>
    </div>

    <div class="row mt-5">
        <div class="col-md-4" data-aos="fade-up">
            <div class="stat-card text-center">
                <i class="fas fa-download fa-2x mb-3"></i>
                <h3 id="total-downloads">{{ stats.total_downloads|default(0)|number_format }}</h3>
                <p>Total Downloads</p>
            </div>
        </div>
        <div class="col-md-4" data-aos="fade-up" data-aos-delay="200">
            <div class="stat-card text-center">
                <i class="fas fa-users fa-2x mb-3"></i>
                <h3>{{ stats.total_users|default(0)|number_format }}</h3>
                <p>Active Users</p>
            </div>
        </div>
        <div class="col-md-4" data-aos="fade-up" data-aos-delay="400">
            <div class="stat-card text-center">
                <i class="fas fa-today fa-2x mb-3"></i>
                <h3 id="today-usage">{{ stats.today_usage|default(0)|number_format }}</h3>
                <p>Today's Downloads</p>
            </div>
        </div>
    </div>
</div>
{% endblock %}""")

    # Admin Panel with 3D effects
    with open("templates/admin.html", "w") as f:
        f.write("""{% extends "base.html" %}

{% block content %}
<div class="container-fluid py-4">
    <div class="row">
        <div class="col-12">
            <div class="glass-card p-4 mb-4">
                <div class="d-flex justify-content-between align-items-center">
                    <h2 class="mb-0"><i class="fas fa-crown me-2"></i>Admin Dashboard</h2>
                    <span class="badge bg-premium">Premium Admin</span>
                </div>
            </div>
        </div>
    </div>

    <div class="row">
        <!-- Real-time Stats -->
        <div class="col-xl-3 col-md-6 mb-4">
            <div class="stat-card" data-aos="flip-left">
                <div class="d-flex align-items-center">
                    <div class="flex-shrink-0">
                        <i class="fas fa-download fa-2x text-primary"></i>
                    </div>
                    <div class="flex-grow-1 ms-3">
                        <h5 class="mb-0">{{ stats.today_usage|default(0)|number_format }}</h5>
                        <small class="text-muted">Today's Downloads</small>
                    </div>
                </div>
            </div>
        </div>

        <div class="col-xl-3 col-md-6 mb-4">
            <div class="stat-card" data-aos="flip-left" data-aos-delay="100">
                <div class="d-flex align-items-center">
                    <div class="flex-shrink-0">
                        <i class="fas fa-users fa-2x text-success"></i>
                    </div>
                    <div class="flex-grow-1 ms-3">
                        <h5 class="mb-0">{{ stats.total_users|default(0)|number_format }}</h5>
                        <small class="text-muted">Total Users</small>
                    </div>
                </div>
            </div>
        </div>

        <div class="col-xl-3 col-md-6 mb-4">
            <div class="stat-card" data-aos="flip-left" data-aos-delay="200">
                <div class="d-flex align-items-center">
                    <div class="flex-shrink-0">
                        <i class="fas fa-database fa-2x text-info"></i>
                    </div>
                    <div class="flex-grow-1 ms-3">
                        <h5 class="mb-0">{{ stats.total_cached|default(0)|number_format }}</h5>
                        <small class="text-muted">Cached Files</small>
                    </div>
                </div>
            </div>
        </div>

        <div class="col-xl-3 col-md-6 mb-4">
            <div class="stat-card" data-aos="flip-left" data-aos-delay="300">
                <div class="d-flex align-items-center">
                    <div class="flex-shrink-0">
                        <i class="fas fa-server fa-2x text-warning"></i>
                    </div>
                    <div class="flex-grow-1 ms-3">
                        <h5 class="mb-0">{{ stats.storage_used|default('0 MB') }}</h5>
                        <small class="text-muted">Storage Used</small>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <div class="row">
        <div class="col-lg-8 mb-4">
            <div class="glass-card p-4">
                <h5 class="mb-4"><i class="fas fa-chart-line me-2"></i>Usage Statistics</h5>
                <canvas id="usageChart" height="250"></canvas>
            </div>
        </div>

        <div class="col-lg-4 mb-4">
            <div class="glass-card p-4">
                <h5 class="mb-4"><i class="fas fa-plus-circle me-2"></i>Create API Key</h5>
                <form method="POST" action="/admin/create_key">
                    <div class="mb-3">
                        <input type="text" name="owner" class="form-control bg-dark text-white border-0" 
                               placeholder="Owner Name" required>
                    </div>
                    <div class="mb-3">
                        <input type="number" name="daily_limit" class="form-control bg-dark text-white border-0" 
                               placeholder="Daily Limit" value="1000" required>
                    </div>
                    <div class="mb-3">
                        <input type="number" name="days_valid" class="form-control bg-dark text-white border-0" 
                               placeholder="Days Valid" value="30" required>
                    </div>
                    <div class="mb-3 form-check">
                        <input type="checkbox" name="is_admin" class="form-check-input" id="isAdmin">
                        <label class="form-check-label" for="isAdmin">Admin Privileges</label>
                    </div>
                    <button type="submit" class="btn btn-premium w-100">
                        <i class="fas fa-key me-2"></i>Generate Key
                    </button>
                </form>
            </div>
        </div>
    </div>

    <div class="row">
        <div class="col-12">
            <div class="glass-card p-4">
                <h5 class="mb-4"><i class="fas fa-users-cog me-2"></i>User Management</h5>
                <div class="table-responsive">
                    <table class="table table-dark table-hover">
                        <thead>
                            <tr>
                                <th>User</th>
                                <th>API Key</th>
                                <th>Usage</th>
                                <th>Limit</th>
                                <th>Status</th>
                                <th>Actions</th>
                            </tr>
                        </thead>
                        <tbody>
                            {% for user in users %}
                            <tr>
                                <td>{{ user.owner }}</td>
                                <td><code>{{ user.key[:8] }}...</code></td>
                                <td>{{ user.used_today }}</td>
                                <td>{{ user.daily_limit }}</td>
                                <td>
                                    <span class="badge {% if user.used_today < user.daily_limit %}bg-success{% else %}bg-danger{% endif %}">
                                        {% if user.used_today < user.daily_limit %}Active{% else %}Limit Reached{% endif %}
                                    </span>
                                </td>
                                <td>
                                    <button class="btn btn-sm btn-outline-danger" onclick="deleteUser('{{ user.key }}')">
                                        <i class="fas fa-trash"></i>
                                    </button>
                                </td>
                            </tr>
                            {% endfor %}
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
    </div>
</div>

<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<script>
    // Usage Chart
    const ctx = document.getElementById('usageChart').getContext('2d');
    new Chart(ctx, {
        type: 'line',
        data: {
            labels: ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'],
            datasets: [{
                label: 'Downloads',
                data: [120, 190, 300, 500, 200, 300, 450],
                borderColor: '#6366f1',
                tension: 0.4,
                fill: true,
                backgroundColor: 'rgba(99, 102, 241, 0.2)'
            }]
        },
        options: {
            responsive: true,
            plugins: {
                legend: {
                    display: false
                }
            }
        }
    });

    function deleteUser(apiKey) {
        if (confirm('Are you sure you want to delete this user?')) {
            fetch('/admin/user/' + apiKey, { method: 'DELETE' })
                .then(response => response.json())
                .then(data => {
                    if (data.success) {
                        location.reload();
                    }
                });
        }
    }
</script>
{% endblock %}""")

# Create the templates
create_premium_templates()

# -------------------
# Helper functions
# -------------------
def ist_date_str() -> str:
    now_ist = datetime.datetime.now(ZoneInfo("Asia/Kolkata"))
    return now_ist.strftime("%Y-%m-%d")

def decrypt_savetube_data(b64_encrypted: str) -> dict:
    try:
        raw = base64.b64decode(b64_encrypted)
        iv = raw[:16]
        ciphertext = raw[16:]
        cipher = AES.new(AES_KEY_BYTES, AES.MODE_CBC, iv=iv)
        decrypted = cipher.decrypt(ciphertext)
        unpadded = unpad(decrypted, 16, style='pkcs7')
        return json.loads(unpadded.decode())
    except Exception as e:
        logger.error(f"Decryption failed: {e}")
        raise

def normalize_youtube_input(inp: str) -> str:
    inp = inp.strip()
    m = YT_RE.search(inp)
    if m:
        vidid = m.group(1)
        return f"https://www.youtube.com/watch?v={vidid}"
    if re.fullmatch(r"[0-9A-Za-z_-]{11}", inp):
        return f"https://www.youtube.com/watch?v={inp}"
    return inp

def extract_vidid(inp: str) -> Optional[str]:
    m = YT_RE.search(inp.strip())
    if m:
        return m.group(1)
    if re.fullmatch(r"[0-9A-Za-z_-]{11}", inp.strip()):
        return inp.strip()
    return None

def sanitize_filename(name: str) -> str:
    return re.sub(r'[^\w\-_\. ]', '_', name)

async def get_random_cdn(session: aiohttp.ClientSession) -> str:
    try:
        async with session.get(f"{SAVETUBE_BASE}/api/random-cdn", timeout=10) as r:
            js = await r.json()
            return js.get("cdn", "cdn1.savetube.me")
    except Exception:
        return "cdn1.savetube.me"

async def savetube_info(session: aiohttp.ClientSession, youtube_url: str) -> dict:
    cdn = await get_random_cdn(session)
    info_url = f"https://{cdn}/v2/info"
    
    try:
        async with session.post(info_url, json={"url": youtube_url}, timeout=15) as r:
            js = await r.json()
            if not js.get("status"):
                raise Exception(js.get("message", "Failed to fetch video info"))
            return decrypt_savetube_data(js["data"])
    except Exception as e:
        logger.error(f"Savetube info error: {e}")
        raise

async def savetube_download(session: aiohttp.ClientSession, key: str, quality: str, 
                           download_type: str = "video") -> str:
    cdn = await get_random_cdn(session)
    download_url = f"https://{cdn}/download"
    
    payload = {
        "downloadType": download_type,
        "quality": quality,
        "key": key
    }
    
    try:
        async with session.post(download_url, json=payload, timeout=20) as r:
            js = await r.json()
            if js.get("status") and js.get("data", {}).get("downloadUrl"):
                return js["data"]["downloadUrl"]
            raise Exception(js.get("message", "Failed to get download URL"))
    except Exception as e:
        logger.error(f"Savetube download error: {e}")
        raise

def choose_best_quality(decrypted: dict, media_type: str) -> str:
    if media_type == "video":
        formats = decrypted.get("formats", [])
        video_formats = [f for f in formats if f.get("type") == "video"]
        if video_formats:
            video_formats.sort(key=lambda x: x.get("height", 0), reverse=True)
            return str(video_formats[0].get("height", DEFAULT_VIDEO_QUALITY))
        return DEFAULT_VIDEO_QUALITY
    else:
        formats = decrypted.get("formats", [])
        audio_formats = [f for f in formats if f.get("type") == "audio"]
        if audio_formats:
            audio_formats.sort(key=lambda x: x.get("bitrate", 0), reverse=True)
            return str(audio_formats[0].get("bitrate", DEFAULT_AUDIO_QUALITY))
        return DEFAULT_AUDIO_QUALITY

async def get_cached_file(ytid: str, media_type: str) -> Optional[Dict]:
    cache_key = f"{ytid}:{media_type}"
    
    if cache_key in IN_MEMORY_CACHE:
        return IN_MEMORY_CACHE[cache_key]
    
    doc = await mongodb.cache.find_one({"ytid": ytid, "type": media_type})
    if doc:
        IN_MEMORY_CACHE[cache_key] = doc
        return doc
    
    return None

async def save_cache_record(ytid: str, media_type: str, file_id: str, 
                           chat_id: int, msg_id: int, file_name: str, meta: dict):
    doc = {
        "ytid": ytid,
        "type": media_type,
        "file_id": file_id,
        "chat_id": chat_id,
        "msg_id": msg_id,
        "file_name": file_name,
        "meta": meta,
        "cached_at": datetime.datetime.utcnow()
    }
    
    await mongodb.cache.update_one(
        {"ytid": ytid, "type": media_type},
        {"$set": doc},
        upsert=True
    )
    
    cache_key = f"{ytid}:{media_type}"
    IN_MEMORY_CACHE[cache_key] = doc
    return doc

async def check_api_key(key: str):
    if not key:
        raise HTTPException(status_code=401, detail="API key required")
    
    record = await mongodb.apikeys.find_one({"key": key})
    if not record:
        raise HTTPException(status_code=401, detail="Invalid API key")
    
    expiry = record.get("expiry_date")
    if expiry:
        if isinstance(expiry, str):
            expiry = datetime.datetime.fromisoformat(expiry)
        if expiry < datetime.datetime.utcnow():
            raise HTTPException(status_code=403, detail="API key expired")
    
    today = ist_date_str()
    if record.get("last_used_date") != today:
        await mongodb.apikeys.update_one(
            {"key": key},
            {"$set": {"used_today": 0, "last_used_date": today}}
        )
        record["used_today"] = 0
    
    used = record.get("used_today", 0)
    limit = record.get("daily_limit", 1000)
    
    if used >= limit:
        raise HTTPException(status_code=429, detail="Daily limit reached")
    
    await mongodb.apikeys.update_one(
        {"key": key},
        {"$inc": {"used_today": 1}}
    )
    
    return record

async def create_api_key(owner: str = "user", daily_limit: int = 1000, 
                        days_valid: int = 30, is_admin: bool = False):
    key = secrets.token_urlsafe(32)
    expiry = None
    
    if days_valid:
        expiry = (datetime.datetime.utcnow() + datetime.timedelta(days=days_valid))
    
    doc = {
        "key": key,
        "owner": owner,
        "daily_limit": daily_limit,
        "used_today": 0,
        "last_used_date": None,
        "expiry_date": expiry,
        "is_admin": is_admin,
        "created_at": datetime.datetime.utcnow()
    }
    
    await mongodb.apikeys.insert_one(doc)
    return doc

async def download_file(session: aiohttp.ClientSession, url: str, dest_path: str):
    async with session.get(url, timeout=300) as response:
        response.raise_for_status()
        total_size = int(response.headers.get('content-length', 0))
        
        with open(dest_path, 'wb') as f:
            downloaded = 0
            async for chunk in response.content.iter_chunked(1024 * 1024):
                f.write(chunk)
                downloaded += len(chunk)
                
                if total_size > 0:
                    percent = (downloaded / total_size) * 100
                    if int(percent) % 10 == 0:
                        logger.info(f"Downloaded {percent:.1f}% of {dest_path}")

async def upload_to_telegram(file_path: str, title: str, media_type: str):
    try:
        if media_type == "video":
            sent_message = await pyrogram_client.send_video(
                chat_id=CACHE_CHANNEL_ID,
                video=file_path,
                caption=title[:1000],
                parse_mode=ParseMode.HTML
            )
            file_id = sent_message.video.file_id
        else:
            sent_message = await pyrogram_client.send_audio(
                chat_id=CACHE_CHANNEL_ID,
                audio=file_path,
                caption=title[:1000],
                parse_mode=ParseMode.HTML,
                title=title[:64],
                performer="YouTube"
            )
            file_id = sent_message.audio.file_id
        
        return file_id, sent_message.id
    except Exception as e:
        logger.error(f"Telegram upload failed: {e}")
        raise

async def background_download_and_upload(ytid: str, media_type: str, 
                                        download_url: str, title: str, 
                                        quality: str, meta: dict):
    cache_key = f"{ytid}:{media_type}"
    
    if cache_key in PROCESSING_SET:
        logger.info(f"Already processing {cache_key}, skipping")
        return
    
    PROCESSING_SET.add(cache_key)
    
    ext = "mp4" if media_type == "video" else "mp3"
    safe_title = sanitize_filename(title)[:100]
    filename = f"{ytid}_{media_type}_{quality}.{ext}"
    filepath = os.path.join(DOWNLOAD_DIR, filename)
    
    session = aiohttp.ClientSession()
    
    try:
        logger.info(f"Downloading {ytid} to {filepath}")
        await download_file(session, download_url, filepath)
        
        logger.info(f"Uploading {filename} to Telegram")
        file_id, msg_id = await upload_to_telegram(filepath, title, media_type)
        
        await save_cache_record(
            ytid, media_type, file_id, 
            CACHE_CHANNEL_ID, msg_id, filename, 
            {"title": title, "quality": quality, **meta}
        )
        
        logger.info(f"Cached {cache_key} as file_id {file_id}")
        
    except Exception as e:
        logger.error(f"Background task failed for {cache_key}: {e}")
    finally:
        await session.close()
        
        try:
            if os.path.exists(filepath):
                os.remove(filepath)
        except Exception as e:
            logger.error(f"Failed to delete {filepath}: {e}")
        
        PROCESSING_SET.discard(cache_key)

async def get_telegram_download_url(file_id: str) -> str:
    try:
        file_info = await pyrogram_client.get_file(file_id)
        return f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file_info.file_path}"
    except Exception as e:
        logger.error(f"Failed to get Telegram download URL: {e}")
        try:
            async with aiohttp.ClientSession() as session:
                url = f"https://api.telegram.org/bot{BOT_TOKEN}/getFile?file_id={file_id}"
                async with session.get(url) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data.get("ok") and data.get("result", {}).get("file_path"):
                            file_path = data["result"]["file_path"]
                            return f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file_path}"
        except Exception as e2:
            logger.error(f"Fallback method also failed: {e2}")
        
        return f"https://api.telegram.org/bot{BOT_TOKEN}/getFile?file_id={file_id}"

# -------------------
# Website Routes
# -------------------
@app.get("/", response_class=HTMLResponse)
async def home_page(request: Request, user: dict = Depends(get_current_user)):
    # Get real stats
    total_downloads = await mongodb.cache.count_documents({})
    total_users = await mongodb.apikeys.count_documents({})
    today_usage = 0
    
    today = ist_date_str()
    users_today = await mongodb.apikeys.find({"last_used_date": today}).to_list(None)
    for u in users_today:
        today_usage += u.get("used_today", 0)
    
    return templates.TemplateResponse("index.html", {
        "request": request,
        "user": user,
        "stats": {
            "total_downloads": total_downloads,
            "total_users": total_users,
            "today_usage": today_usage
        }
    })

@app.get("/api/stats")
async def api_stats():
    total_downloads = await mongodb.cache.count_documents({})
    today_usage = 0
    
    today = ist_date_str()
    users_today = await mongodb.apikeys.find({"last_used_date": today}).to_list(None)
    for u in users_today:
        today_usage += u.get("used_today", 0)
    
    return {
        "total_downloads": total_downloads,
        "today_usage": today_usage
    }

@app.get("/admin", response_class=HTMLResponse)
async def admin_panel(request: Request, admin: dict = Depends(get_current_admin)):
    if not admin:
        return RedirectResponse(url="/login")
    
    # Get real statistics
    total_users = await mongodb.apikeys.count_documents({})
    total_cached = await mongodb.cache.count_documents({})
    today = ist_date_str()
    
    today_usage = 0
    users_today = await mongodb.apikeys.find({"last_used_date": today}).to_list(None)
    for user in users_today:
        today_usage += user.get("used_today", 0)
    
    # Calculate storage used (approximate)
    storage_used = total_cached * 10  # Approximate 10MB per file
    
    users = await mongodb.apikeys.find().sort("created_at", -1).limit(20).to_list(None)
    
    return templates.TemplateResponse("admin.html", {
        "request": request,
        "user": admin,
        "stats": {
            "total_users": total_users,
            "total_cached": total_cached,
            "today_usage": today_usage,
            "storage_used": humanize.naturalsize(storage_used * 1024 * 1024)
        },
        "users": users
    })

@app.post("/admin/create_key")
async def admin_create_key_api(request: Request, admin: dict = Depends(get_current_admin)):
    if not admin:
        raise HTTPException(status_code=401, detail="Admin access required")
    
    form = await request.form()
    owner = form.get("owner", "user")
    daily_limit = int(form.get("daily_limit", 1000))
    days_valid = int(form.get("days_valid", 30))
    is_admin = form.get("is_admin") == "on"
    
    key_data = await create_api_key(owner, daily_limit, days_valid, is_admin)
    
    return {"success": True, "key": key_data["key"]}

@app.delete("/admin/user/{api_key}")
async def delete_user_api(api_key: str, admin: dict = Depends(get_current_admin)):
    if not admin:
        raise HTTPException(status_code=401, detail="Admin access required")
    
    result = await mongodb.apikeys.delete_one({"key": api_key})
    if result.deleted_count:
        return {"success": True, "message": "User deleted successfully"}
    else:
        return {"success": False, "message": "User not found"}

# -------------------
# API endpoints
# -------------------
class ResultModel(BaseModel):
    status: bool
    creator: str = "@Nottyboyy"
    telegram: str = "https://t.me/ZeeMusicUpdate"
    result: Optional[dict] = None
    message: Optional[str] = None

@app.on_event("startup")
async def startup_event():
    await mongodb.cache.create_index("ytid")
    await mongodb.cache.create_index([("ytid", 1), ("type", 1)])
    await mongodb.apikeys.create_index("key", unique=True)
    
    await pyrogram_client.start()
    logger.info("Pyrogram client started")
    
    count = await mongodb.apikeys.count_documents({})
    if count == 0:
        key_data = await create_api_key("admin", 10000, 365, True)
        logger.info(f"Created default admin key: {key_data['key']}")

@app.on_event("shutdown")
async def shutdown_event():
    await pyrogram_client.stop()
    logger.info("Pyrogram client stopped")

@app.get("/ytmp4", response_model=ResultModel)
async def ytmp4(
    request: Request,
    url: str = Query(..., description="YouTube URL or video ID"),
    api_key: str = Query(..., description="Your API key"),
    quality: str = Query(None, description="Preferred quality (e.g., 720, 1080)")
):
    await check_api_key(api_key)
    
    yt_url = normalize_youtube_input(url)
    vidid = extract_vidid(url) or extract_vidid(yt_url)
    
    if not vidid:
        return JSONResponse(
            status_code=400,
            content={"status": False, "message": "Invalid YouTube URL or ID"}
        )
    
    cached = await get_cached_file(vidid, "video")
    if cached:
        telegram_url = await get_telegram_download_url(cached["file_id"])
        tlink = f"https://t.me/c/{str(cached['chat_id']).replace('-100', '')}/{cached['msg_id']}"
        
        return {
            "status": True,
            "result": {
                "title": cached["meta"].get("title", "Unknown"),
                "duration": cached["meta"].get("duration", "Unknown"),
                "quality": cached["meta"].get("quality", "Unknown"),
                "source": "telegram_cache",
                "url": telegram_url,
                "file_id": cached["file_id"],
                "telegram_msg": {
                    "chat_id": cached["chat_id"],
                    "msg_id": cached["msg_id"],
                    "tlink": tlink
                }
            }
        }
    
    session = aiohttp.ClientSession()
    
    try:
        decrypted = await savetube_info(session, yt_url)
        selected_quality = quality or choose_best_quality(decrypted, "video")
        download_url = await savetube_download(session, decrypted.get("key"), selected_quality, "video")
        
        response_data = {
            "status": True,
            "result": {
                "title": decrypted.get("title", "Unknown"),
                "duration": decrypted.get("durationLabel", "Unknown"),
                "quality": selected_quality,
                "source": "savetube",
                "url": download_url
            }
        }
        
        asyncio.create_task(
            background_download_and_upload(
                vidid, "video", download_url,
                decrypted.get("title", vidid),
                selected_quality,
                {
                    "duration": decrypted.get("durationLabel", "Unknown"),
                    "thumbnail": decrypted.get("thumbnail")
                }
            )
        )
        
        return response_data
        
    except Exception as e:
        logger.error(f"YTMP4 error: {e}")
        return JSONResponse(
            status_code=500,
            content={"status": False, "message": str(e)}
        )
    finally:
        await session.close()

@app.get("/ytmp3", response_model=ResultModel)
async def ytmp3(
    request: Request,
    url: str = Query(..., description="YouTube URL or video ID"),
    api_key: str = Query(..., description="Your API key"),
    quality: str = Query(None, description="Preferred quality (e.g., 128, 320)")
):
    await check_api_key(api_key)
    
    yt_url = normalize_youtube_input(url)
    vidid = extract_vidid(url) or extract_vidid(yt_url)
    
    if not vidid:
        return JSONResponse(
            status_code=400,
            content={"status": False, "message": "Invalid YouTube URL or ID"}
        )
    
    cached = await get_cached_file(vidid, "audio")
    if cached:
        telegram_url = await get_telegram_download_url(cached["file_id"])
        tlink = f"https://t.me/c/{str(cached['chat_id']).replace('-100', '')}/{cached['msg_id']}"
        
        return {
            "status": True,
            "result": {
                "title": cached["meta"].get("title", "Unknown"),
                "duration": cached["meta"].get("duration", "Unknown"),
                "quality": cached["meta"].get("quality", "Unknown"),
                "source": "telegram_cache",
                "url": telegram_url,
                "file_id": cached["file_id"],
                "telegram_msg": {
                    "chat_id": cached["chat_id"],
                    "msg_id": cached["msg_id"],
                    "tlink": tlink
                }
            }
        }
    
    session = aiohttp.ClientSession()
    
    try:
        decrypted = await savetube_info(session, yt_url)
        selected_quality = quality or choose_best_quality(decrypted, "audio")
        download_url = await savetube_download(session, decrypted.get("key"), selected_quality, "audio")
        
        response_data = {
            "status": True,
            "result": {
                "title": decrypted.get("title", "Unknown"),
                "duration": decrypted.get("durationLabel", "Unknown"),
                "quality": selected_quality,
                "source": "savetube",
                "url": download_url
            }
        }
        
        asyncio.create_task(
            background_download_and_upload(
                vidid, "audio", download_url,
                decrypted.get("title", vidid),
                selected_quality,
                {
                    "duration": decrypted.get("durationLabel", "Unknown"),
                    "thumbnail": decrypted.get("thumbnail")
                }
            )
        )
        
        return response_data
        
    except Exception as e:
        logger.error(f"YTMP3 error: {e}")
        return JSONResponse(
            status_code=500,
            content={"status": False, "message": str(e)}
        )
    finally:
        await session.close()

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)

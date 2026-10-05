import os
from pathlib import Path
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
TEMP_DIR = BASE_DIR / "temp"
TEMP_DIR.mkdir(exist_ok=True, parents=True)

MAX_INPUT_SIZE_MB = int(os.getenv("MAX_INPUT_SIZE_MB", "500"))
MAX_VIDEO_DURATION_SEC = int(os.getenv("MAX_VIDEO_DURATION_SEC", "300"))

SUPPORTED_EXTENSIONS = {".mp4", ".mov", ".mkv", ".avi", ".webm", ".flv", ".3gp", ".m4v"}

PRESETS = {
    "wa_1080p": {
        "name": "📱 WhatsApp Status 1080p (Anti-Pecah)",
        "desc": "Resolusi 1080x1920 @ 60fps CFR. Bitrate 2.8M (1:1 VBV) + Keyframe 1s agar WA tidak re-encode kasar.",
        "target_w": 1080,
        "target_h": 1920,
        "fps": 60,
        "crf": 18,
        "maxrate": "2800k",
        "bufsize": "2800k",
        "audio_bitrate": "128k",
        "preset": "fast",
        "threads": 2,
        "mode": "standard",
        "target": "whatsapp"
    },
    "wa_720p": {
        "name": "⚡ WhatsApp Status 720p (Super Smooth)",
        "desc": "Resolusi 720x1280 @ 60fps CFR. Bitrate 2.0M, ukuran file sangat hemat (<15MB), anti-lag.",
        "target_w": 720,
        "target_h": 1280,
        "fps": 60,
        "crf": 19,
        "maxrate": "2000k",
        "bufsize": "2000k",
        "audio_bitrate": "128k",
        "preset": "fast",
        "threads": 2,
        "mode": "standard",
        "target": "whatsapp"
    },
    "ig_story": {
        "name": "📸 Instagram Story / Reels HD",
        "desc": "Resolusi 1080x1920 @ 60fps CFR. Bitrate 7.5M, GOP 60 optimal untuk Instagram CDN.",
        "target_w": 1080,
        "target_h": 1920,
        "fps": 60,
        "crf": 18,
        "maxrate": "8000k",
        "bufsize": "16000k",
        "audio_bitrate": "192k",
        "preset": "slow",
        "threads": 2,
        "mode": "standard",
        "target": "instagram"
    },
    "iphone_hdr": {
        "name": "🍎 iPhone Pro HDR (Glossy & Cinematic)",
        "desc": "S-Curve Tone Mapping + Lanczos Spline + Skin-Safe Vibrance (Kinclong & Elegan).",
        "target_w": 1080,
        "target_h": 1920,
        "fps": 60,
        "crf": 17,
        "maxrate": "8000k",
        "bufsize": "16000k",
        "audio_bitrate": "192k",
        "preset": "slow",
        "threads": 2,
        "mode": "iphone_hdr",
        "target": "instagram"
    },
    "tiktok_4k": {
        "name": "🔥 TikTok 4K CC (Ultra Sharp & Contrast Pop)",
        "desc": "Adaptive Edge Sharpening (Unsharp 5:5:0.8) + Micro-Contrast + Denoise (Gaya 4K Remini).",
        "target_w": 1080,
        "target_h": 1920,
        "fps": 60,
        "crf": 17,
        "maxrate": "8500k",
        "bufsize": "17000k",
        "audio_bitrate": "192k",
        "preset": "slow",
        "threads": 2,
        "mode": "tiktok_4k",
        "target": "instagram"
    },
    "blur_bg_1080p": {
        "name": "🎨 Auto 9:16 + Blurred Background (Landscape/Kotak)",
        "desc": "Mengisi latar belakang hitam dengan video blur estetis agar rasio pas 9:16 di Story.",
        "target_w": 1080,
        "target_h": 1920,
        "fps": 60,
        "crf": 18,
        "maxrate": "4500k",
        "bufsize": "9000k",
        "audio_bitrate": "160k",
        "preset": "fast",
        "threads": 2,
        "mode": "blur_bg",
        "target": "universal"
    }
}

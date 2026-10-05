import os
import json
import asyncio
import re
import logging
from pathlib import Path
from typing import Dict, Any, Optional, Tuple

from config import PRESETS

logger = logging.getLogger("video_processor")


async def probe_video(file_path: Path) -> Dict[str, Any]:
    cmd = [
        "ffprobe",
        "-v", "quiet",
        "-print_format", "json",
        "-show_format",
        "-show_streams",
        str(file_path)
    ]
    
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await proc.communicate()
        
        if proc.returncode != 0:
            raise RuntimeError(f"ffprobe failed: {stderr.decode('utf-8', errors='ignore')}")
            
        data = json.loads(stdout.decode("utf-8"))
        
        v_stream = next((s for s in data.get("streams", []) if s.get("codec_type") == "video"), None)
        a_stream = next((s for s in data.get("streams", []) if s.get("codec_type") == "audio"), None)
        format_info = data.get("format", {})
        
        if not v_stream:
            raise ValueError("No video stream found in file")
            
        width = int(v_stream.get("width", 0))
        height = int(v_stream.get("height", 0))
        
        rotation = 0
        for side_data in v_stream.get("side_data_list", []):
            if "rotation" in side_data:
                rotation = int(side_data["rotation"])
        if "rotate" in v_stream.get("tags", {}):
            try:
                rotation = int(v_stream["tags"]["rotate"])
            except Exception:
                pass
            
        if abs(rotation) in (90, 270):
            width, height = height, width
            
        duration = float(format_info.get("duration", v_stream.get("duration", 0)))
        file_size_bytes = int(format_info.get("size", file_path.stat().st_size))
        file_size_mb = round(file_size_bytes / (1024 * 1024), 2)
        
        bitrate_bps = int(format_info.get("bit_rate", v_stream.get("bit_rate", 0) or 0))
        bitrate_kbps = round(bitrate_bps / 1000) if bitrate_bps else round((file_size_bytes * 8) / (duration * 1000)) if duration else 0
        
        r_fps = v_stream.get("r_frame_rate", "30/1")
        avg_fps = v_stream.get("avg_frame_rate", "30/1")
        
        def eval_fps(fps_str):
            try:
                if "/" in fps_str:
                    num, den = map(float, fps_str.split("/"))
                    return round(num / den, 2) if den else 30.0
                return float(fps_str)
            except Exception:
                return 30.0
                
        fps = eval_fps(r_fps)
        if fps > 120 or fps < 5:
            fps = eval_fps(avg_fps)
            
        aspect_ratio = f"{round(width/height, 2)}:1" if height else "9:16"
        is_vertical_9_16 = (width == 1080 and height == 1920) or (width == 720 and height == 1280) or (abs((width / (height or 1)) - (9/16)) < 0.05)
        
        return {
            "width": width,
            "height": height,
            "rotation": rotation,
            "duration": round(duration, 1),
            "file_size_mb": file_size_mb,
            "bitrate_kbps": bitrate_kbps,
            "fps": fps,
            "codec_video": v_stream.get("codec_name", "unknown"),
            "codec_audio": a_stream.get("codec_name", "none") if a_stream else "none",
            "has_audio": a_stream is not None,
            "aspect_ratio": aspect_ratio,
            "is_vertical_9_16": is_vertical_9_16
        }
    except Exception as e:
        logger.error(f"Error probing video {file_path}: {e}")
        raise


async def encode_story_video(
    input_path: Path,
    output_path: Path,
    preset_key: str,
    meta: Dict[str, Any]
) -> Tuple[bool, str]:
    preset_cfg = PRESETS.get(preset_key, PRESETS["wa_1080p"])
    
    target_w = preset_cfg["target_w"]
    target_h = preset_cfg["target_h"]
    target_fps = preset_cfg["fps"]
    mode = preset_cfg.get("mode", "standard")
    threads = preset_cfg.get("threads", 2)
    preset_speed = preset_cfg.get("preset", "fast")
    target_platform = preset_cfg.get("target", "whatsapp")
    
    cmd = [
        "ffmpeg",
        "-y",
        "-threads", str(threads),
        "-i", str(input_path)
    ]
    
    cfr_filter = f"fps=fps={target_fps}:round=near,setpts=N/({target_fps}*TB),format=yuv420p"
    
    if mode == "blur_bg":
        filter_complex = (
            f"[0:v]split=2[in1][in2];"
            f"[in1]scale={target_w}:{target_h}:flags=lanczos:force_original_aspect_ratio=increase,"
            f"crop={target_w}:{target_h},boxblur=25:5[bg];"
            f"[in2]scale={target_w}:{target_h}:flags=lanczos:force_original_aspect_ratio=decrease[fg];"
            f"[bg][fg]overlay=(W-w)/2:(H-h)/2,"
            f"{cfr_filter}[v_out]"
        )
        cmd.extend(["-filter_complex", filter_complex, "-map", "[v_out]"])
        if meta["has_audio"]:
            cmd.extend(["-map", "0:a?"])
    elif mode == "iphone_hdr":
        # iPhone Pro HDR: S-Curve Tone Curve + Subtle Lanczos Sharpness + Golden Vibrance
        vf = (
            f"scale={target_w}:{target_h}:flags=lanczos+accurate_rnd+full_chroma_int:force_original_aspect_ratio=decrease,"
            f"pad={target_w}:{target_h}:(ow-iw)/2:(oh-ih)/2:color=black,"
            f"hqdn3d=0.8:0.8:1.5:1.5,"
            f"unsharp=5:5:0.5:3:3:0.2,"
            f"curves=all='0/0 0.22/0.18 0.78/0.83 1/1',"
            f"eq=contrast=1.03:saturation=1.07:brightness=0.005,"
            f"{cfr_filter}"
        )
        cmd.extend(["-vf", vf])
    elif mode == "tiktok_4k":
        # TikTok 4K CC: High-Pass Unsharp Masking + Denoising + Crisp Edge Pop + Dynamic Contrast
        vf = (
            f"scale={target_w}:{target_h}:flags=lanczos+accurate_rnd+full_chroma_int:force_original_aspect_ratio=decrease,"
            f"pad={target_w}:{target_h}:(ow-iw)/2:(oh-ih)/2:color=black,"
            f"hqdn3d=1.2:1.2:2.2:2.2,"
            f"unsharp=5:5:0.85:3:3:0.45,"
            f"eq=contrast=1.07:saturation=1.12:brightness=0.01,"
            f"{cfr_filter}"
        )
        cmd.extend(["-vf", vf])
    else:
        # Standard Clean Full HD
        vf = (
            f"scale={target_w}:{target_h}:flags=lanczos:force_original_aspect_ratio=decrease,"
            f"pad={target_w}:{target_h}:(ow-iw)/2:(oh-ih)/2:color=black,"
            f"unsharp=3:3:0.3:3:3:0.1,"
            f"{cfr_filter}"
        )
        cmd.extend(["-vf", vf])
    
    level = "4.1" if target_platform == "whatsapp" else "4.2"
    
    cmd.extend([
        "-c:v", "libx264",
        "-preset", preset_speed,
        "-threads", str(threads),
        "-crf", str(preset_cfg.get("crf", 18)),
        "-maxrate", str(preset_cfg.get("maxrate", "2800k")),
        "-bufsize", str(preset_cfg.get("bufsize", "2800k")),
        "-profile:v", "high",
        "-level", level,
        "-pix_fmt", "yuv420p",
        "-color_primaries", "bt709",
        "-color_trc", "bt709",
        "-colorspace", "bt709",
        "-g", str(target_fps),
        "-keyint_min", str(int(target_fps / 2)),
        "-movflags", "+faststart"
    ])
    
    if meta["has_audio"]:
        cmd.extend([
            "-c:a", "aac",
            "-b:a", preset_cfg.get("audio_bitrate", "128k"),
            "-ar", "44100",
            "-ac", "2"
        ])
    else:
        cmd.append("-an")
        
    cmd.append(str(output_path))
    
    logger.info(f"Executing FFmpeg (preset={preset_speed}, threads={threads}): {' '.join(cmd)}")
    
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    
    stdout, stderr = await proc.communicate()
    
    if proc.returncode != 0:
        err_msg = stderr.decode("utf-8", errors="ignore")
        logger.error(f"FFmpeg encoding error: {err_msg}")
        return False, err_msg
        
    if not output_path.exists() or output_path.stat().st_size == 0:
        return False, "Output file was not generated or is empty"
        
    return True, "Success"

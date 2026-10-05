with open(r"e:\maplebot\hd_story_bot_build\bot_patched.py", "r", encoding="utf-8") as f:
    code = f.read()

# Make sure env vars are robust
code = code.replace(
    'BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]',
    'BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "8789659912:AAE9U4epvUzkuMgEMaH0eBS_cJS1USU-pGY")'
)
code = code.replace(
    'WORKER_BASE_URL = os.environ["WORKER_BASE_URL"].rstrip("/")',
    'WORKER_BASE_URL = os.environ.get("WORKER_BASE_URL", "https://tg-relay.sandhyarasmi.workers.dev").rstrip("/")'
)
code = code.replace(
    'WORKER_TOKEN = os.environ["WORKER_TOKEN"]',
    'WORKER_TOKEN = os.environ.get("WORKER_TOKEN", "default_token")'
)

with open(r"e:\maplebot\sandhyarasmi_homeserver_deploy\bot.py", "w", encoding="utf-8") as f:
    f.write(code)

print("bot.py ready in sandhyarasmi_homeserver_deploy!")

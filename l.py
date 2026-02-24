import asyncio
import os
import sys
import logging
import subprocess
import psutil
import aiosqlite as sqlite3 # <<< FIX: Import aiosqlite
import hashlib
import json
import zipfile
import shutil
import ast
from datetime import datetime
from pathlib import Path

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, FSInputFile
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.client.default import DefaultBotProperties
from aiogram.types import LabeledPrice # Import for payments

from aiohttp import web
# 'from dotenv import load_dotenv' has been removed as it's no longer needed

# --- Configuration & Setup ---

# load_dotenv() has been removed
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load variables (hardcoded from .env)
BOT_TOKEN = "8438635627:AAFTq-vg-vVuXK_RZ5chSPw6zFMAIhY_buk"
OWNER_ID_STR = "7337091751"
ADMIN_ID_STR = "7337091751"
OWNER_CONTACT_URL = "https://t.me/GIKSSEM7"
UPDATE_CHANNEL_URL = "https://t.me/giksxitvip"

if not BOT_TOKEN:
    logger.error("BOT_TOKEN is missing! Please check the hardcoded variables.")
    raise ValueError("BOT_TOKEN is required.")

if not OWNER_ID_STR or not ADMIN_ID_STR:
    logger.error("OWNER_ID or ADMIN_ID is missing! Please check the hardcoded variables.")
    raise ValueError("OWNER_ID and ADMIN_ID are required.")

try:
    OWNER_ID = int(OWNER_ID_STR)
    ADMIN_ID = int(ADMIN_ID_STR)
except ValueError:
    logger.error("OWNER_ID or ADMIN_ID must be valid integers!")
    raise

# --- Constants & Paths ---
BASE_DIR = Path(__file__).parent.absolute()
PROJECTS_DIR = BASE_DIR / 'projects'
DATA_DIR = BASE_DIR / 'data'
DATABASE_PATH = DATA_DIR / 'projects.db'

# Project Limits
FREE_TIER_SLOTS = 1
PREMIUM_TIER_SLOTS = 10 
SLOT_PRICE_STARS = 30 # Set your price in Stars

# Create directories
PROJECTS_DIR.mkdir(exist_ok=True)
DATA_DIR.mkdir(exist_ok=True)

try:
    STANDARD_LIBS = set(sys.stdlib_module_names)
    logger.info("Loaded {} standard library modules (Python 3.10+).".format(len(STANDARD_LIBS)))
except AttributeError:
    logger.warning("Using basic stdlib list. (Upgrade to Python 3.10+ for best results)")
    STANDARD_LIBS = {"os", "sys", "json", "asyncio", "sqlite3", "logging", "datetime", "pathlib", "hashlib", "zipfile", "re", "math", "collections", "time", "subprocess", "shutil", "ast"}

# --- Bot & Dispatcher ---
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# --- Global State ---
bot_scripts = {} 
admin_ids = {OWNER_ID, ADMIN_ID}
banned_users = set()
bot_locked = False
bot_stats = {}

# --- FSM States ---
class ProjectCreation(StatesGroup):
    awaiting_name = State()
    awaiting_file = State()

class ProjectManagement(StatesGroup):
    awaiting_run_command = State()
    awaiting_deps_install = State()
    awaiting_delete_confirm = State()

class AdminFSM(StatesGroup):
    awaiting_broadcast = State()
    awaiting_add_admin_id = State()
    awaiting_remove_admin_id = State()
    awaiting_ban_id = State()
    awaiting_ban_reason = State()
    awaiting_unban_id = State()

# --- Database Functions ---
# <<< FIX: All DB functions are now async
async def get_db_conn():
    """Helper to get a DB connection."""
    return await sqlite3.connect(DATABASE_PATH)
    
async def init_db():
    logger.info("Initializing database at: {}".format(DATABASE_PATH))
    try:
        conn = await get_db_conn()
        c = await conn.cursor()
        await c.execute('''CREATE TABLE IF NOT EXISTS projects (
                        project_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_id INTEGER NOT NULL,
                        project_name TEXT NOT NULL,
                        status TEXT DEFAULT 'stopped',
                        ram_tier TEXT DEFAULT 'free',
                        run_command TEXT,
                        main_file TEXT,
                        created_at TEXT,
                        UNIQUE(user_id, project_name)
                    )''')
        await c.execute('''CREATE TABLE IF NOT EXISTS users (
                        user_id INTEGER PRIMARY KEY,
                        join_date TEXT,
                        last_active TEXT,
                        is_banned BOOLEAN DEFAULT 0,
                        is_admin BOOLEAN DEFAULT 0,
                        purchased_slots INTEGER DEFAULT 0
                    )''')
        
        await c.execute('''CREATE TABLE IF NOT EXISTS bot_stats (
                        stat_name TEXT PRIMARY KEY,
                        stat_value INTEGER
                    )''')
        
        await c.execute('INSERT OR IGNORE INTO users (user_id, is_admin) VALUES (?, ?)', (OWNER_ID, 1))
        if ADMIN_ID != OWNER_ID:
            await c.execute('INSERT OR IGNORE INTO users (user_id, is_admin) VALUES (?, ?)', (ADMIN_ID, 1))

        for stat in ['total_uploads', 'total_downloads', 'total_runs']:
            await c.execute('INSERT OR IGNORE INTO bot_stats (stat_name, stat_value) VALUES (?, 0)', (stat,))

        await conn.commit()
        await conn.close()
        logger.info("Database initialized successfully.")
    except Exception as e:
        logger.error("Database initialization error: {}".format(e), exc_info=True)

async def migrate_db():
    """Applies database schema changes."""
    logger.info("Running database migrations...")
    try:
        conn = await get_db_conn()
        c = await conn.cursor()
        
        await c.execute("PRAGMA table_info(users)")
        columns = [row[1] for row in await c.fetchall()]
        
        if 'purchased_slots' not in columns:
            logger.info("Adding 'purchased_slots' column to 'users' table...")
            await c.execute("ALTER TABLE users ADD COLUMN purchased_slots INTEGER DEFAULT 0")
            logger.info("Column added successfully.")
        
        await conn.commit()
        await conn.close()
        logger.info("Database migrations complete.")
    except Exception as e:
        logger.error("Database migration error: {}".format(e), exc_info=True)

async def load_global_data():
    """Loads admins, banned users, and stats into memory."""
    logger.info("Loading global data from database...")
    try:
        conn = await get_db_conn()
        c = await conn.cursor()
        
        admin_ids.clear()
        await c.execute("SELECT user_id FROM users WHERE is_admin = 1")
        for (user_id,) in await c.fetchall():
            admin_ids.add(user_id)
        logger.info("Loaded {} admins.".format(len(admin_ids)))
            
        banned_users.clear()
        await c.execute("SELECT user_id FROM users WHERE is_banned = 1")
        for (user_id,) in await c.fetchall():
            banned_users.add(user_id)
        logger.info("Loaded {} banned users.".format(len(banned_users)))

        bot_stats.clear()
        await c.execute("SELECT stat_name, stat_value FROM bot_stats")
        for stat_name, stat_value in await c.fetchall():
            bot_stats[stat_name] = stat_value
        logger.info("Loaded bot stats: {}".format(bot_stats))
        
        await conn.close()
    except Exception as e:
        logger.error("Error loading global data: {}".format(e), exc_info=True)

async def get_user_slot_limit(user_id: int) -> int:
    """Gets a user's total slot limit (free + purchased)."""
    if user_id in admin_ids:
        return float('inf')
    
    conn = await get_db_conn()
    c = await conn.cursor()
    await c.execute("SELECT purchased_slots FROM users WHERE user_id = ?", (user_id,))
    result = await c.fetchone()
    await conn.close()
    
    purchased = result[0] if result else 0
    return FREE_TIER_SLOTS + purchased

async def get_user_projects(user_id: int) -> list:
    conn = await get_db_conn()
    conn.row_factory = sqlite3.Row
    c = await conn.cursor()
    await c.execute("SELECT project_name, status, ram_tier FROM projects WHERE user_id = ?", (user_id,))
    projects = await c.fetchall()
    await conn.close()
    return projects

async def get_project_details(user_id: int, project_name: str) -> sqlite3.Row | None:
    conn = await get_db_conn()
    conn.row_factory = sqlite3.Row
    c = await conn.cursor()
    await c.execute("SELECT * FROM projects WHERE user_id = ? AND project_name = ?", (user_id, project_name))
    project = await c.fetchone()
    await conn.close()
    return project

async def update_project_status(user_id: int, project_name: str, status: str):
    conn = await get_db_conn()
    c = await conn.cursor()
    await c.execute("UPDATE projects SET status = ? WHERE user_id = ? AND project_name = ?", (status, user_id, project_name))
    await conn.commit()
    await conn.close()
    logger.info("Updated status for {}_ {} to {}".format(user_id, project_name, status))

async def delete_project_from_db(user_id: int, project_name: str):
    conn = await get_db_conn()
    c = await conn.cursor()
    await c.execute("DELETE FROM projects WHERE user_id = ? AND project_name = ?", (user_id, project_name))
    await conn.commit()
    await conn.close()
    
async def update_user_activity(user_id: int):
    conn = await get_db_conn()
    c = await conn.cursor()
    now = datetime.now().isoformat()
    await c.execute('INSERT OR IGNORE INTO users (user_id, join_date, last_active) VALUES (?, ?, ?)', (user_id, now, now))
    await c.execute('UPDATE users SET last_active = ? WHERE user_id = ?', (now, user_id))
    await conn.commit()
    await conn.close()

def is_user_banned(user_id: int) -> bool:
    return user_id in banned_users

# --- Dependency Handling Helpers ---
async def create_venv_if_not_exists(user_folder: Path) -> tuple[Path | None, str | None]:
    venv_path = user_folder / "venv"
    if not venv_path.exists():
        logger.info("Creating venv for user {} at {}".format(user_folder.name, venv_path))
        try:
            proc = await asyncio.create_subprocess_exec(
                sys.executable, "-m", "venv", str(venv_path),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await proc.communicate()
            if proc.returncode != 0:
                error_msg = "Venv creation failed: {}".format(stderr.decode())
                return None, error_msg
            return venv_path, None
        except Exception as e:
            error_msg = "Exception creating venv: {}".format(e)
            return None, error_msg
    return venv_path, None

def get_venv_python(user_folder: Path) -> str:
    venv_path = user_folder / "venv"
    if sys.platform == "win32":
        python_exe = venv_path / "Scripts" / "python.exe"
    else:
        python_exe = venv_path / "bin" / "python"
    
    if python_exe.exists():
        return str(python_exe)
    
    return sys.executable

def parse_imports(file_content: str) -> set[str]:
    imports = set()
    try:
        tree = ast.parse(file_content)
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    imports.add(alias.name.split('.')[0])
            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    imports.add(node.module.split('.')[0])
    except SyntaxError as e:
        logger.warning("AST parse error: {}".format(e))
    return imports

async def install_dependencies(user_folder: Path, status_msg: types.Message, dependencies: set[str] = None, requirements_file: Path = None) -> str | None:
    venv_path, err = await create_venv_if_not_exists(user_folder)
    if err:
        return "Failed to create venv: {}".format(err)

    if sys.platform == "win32":
        pip_exe = venv_path / "Scripts" / "pip.exe"
    else:
        pip_exe = venv_path / "bin" / "pip"

    command = [str(pip_exe), "install", "--upgrade", "pip"]
    
    if requirements_file:
        command += ["-r", str(requirements_file)]
        install_msg = "⚙️ <b>Installing from requirements.txt...</b>"
    elif dependencies:
        command += list(dependencies)
        install_msg = "⚙️ <b>Installing dependencies...</b>\n<code>{}</code>".format(' '.join(dependencies))
    else:
        install_msg = "⚙️ <b>Verifying environment...</b>"

    await status_msg.edit_text("{}\n\n{}".format(status_msg.text, install_msg))
    
    proc = await asyncio.create_subprocess_exec(
        *command,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    stdout, stderr = await proc.communicate()
    
    if proc.returncode != 0:
        error_msg = "Dependency installation failed:\n<pre>{}</pre>".format(stderr.decode()[:1000])
        logger.error("Pip install failed for {}: {}".format(user_folder.name, stderr.decode()))
        return error_msg
        
    logger.info("Pip install successful for {}".format(user_folder.name))
    return None

# --- Project Process Management ---
async def cleanup_script(user_id: int, project_name: str, new_status: str = "stopped"):
    script_key = "{}_{}".format(user_id, project_name)
    process = bot_scripts.pop(script_key, None)
    
    if process:
        try:
            if process.poll() is None: 
                logger.warning("Process {} was still running during cleanup. Killing.".format(process.pid))
                parent = psutil.Process(process.pid)
                for child in parent.children(recursive=True):
                    child.kill()
                parent.kill()
        except Exception as e:
            logger.error("Error during cleanup kill: {}".format(e))
            
    await update_project_status(user_id, project_name, new_status)
    logger.info("Cleaned up script {}, set status to {}".format(script_key, new_status))

async def monitor_script(user_id: int, project_name: str):
    script_key = "{}_{}".format(user_id, project_name)
    
    if script_key not in bot_scripts:
        logger.warning("Monitor called for {}, but it's not in bot_scripts.".format(script_key))
        return
        
    process = bot_scripts[script_key]
    
    try:
        return_code = await asyncio.to_thread(process.wait)
        logger.info("Script {} (PID: {}) finished with code {}.".format(script_key, process.pid, return_code))
        
        if script_key in bot_scripts: 
            if return_code == 0:
                await bot.send_message(user_id, "✅ <b>Script Finished!</b>\nProject: <code>{}</code>".format(project_name))
                new_status = "stopped"
            else:
                await bot.send_message(user_id, "❌ <b>Script Crashed!</b>\nProject: <code>{}</code>\nCode: <code>{}</code>\n\nCheck logs for details.".format(project_name, return_code))
                new_status = "crashed"
            
            await cleanup_script(user_id, project_name, new_status=new_status)
        
    except Exception as e:
        logger.error("Error monitoring script {}: {}".format(script_key, e), exc_info=True)
        await cleanup_script(user_id, project_name, new_status="crashed")

async def start_project(user_id: int, project_name: str, message: types.Message):
    script_key = "{}_{}".format(user_id, project_name)
    if script_key in bot_scripts:
        await message.answer("Project is already running.")
        return

    project_details = await get_project_details(user_id, project_name)
    if not project_details:
        await message.answer("Error: Project not found.")
        return
    
    run_command = project_details['run_command']
    main_file = project_details['main_file']
    
    if not run_command:
        if main_file:
            run_command = "python {}".format(main_file)
        else:
            await message.answer("❌ No run command set. Please upload a file or set a command.")
            return

    user_folder = PROJECTS_DIR / str(user_id)
    project_folder = user_folder / project_name
    python_exe = get_venv_python(user_folder)
    log_file_path = project_folder / "deploy.log"
    
    command_parts = run_command.split()
    if command_parts[0].lower() in ["python", "python3"]:
        command = [python_exe] + command_parts[1:]
    else:
        await message.answer("❌ <b>Run Command Error!</b>\nYour run command must start with `python` or `python3`.")
        return
    
    try:
        log_file = open(log_file_path, 'w') # 'w' to clear log on start
        log_file.write("\n--- Starting process at {} ---\n".format(datetime.now()))
        log_file.write("Executing: {}\n".format(' '.join(command)))
        log_file.flush()
        
        process = subprocess.Popen(
            command,
            cwd=str(project_folder),
            stdout=log_file,
            stderr=log_file,
            close_fds=True
        )
        
        bot_scripts[script_key] = process
        await update_project_status(user_id, project_name, 'running')
        
        bot_stats['total_runs'] = bot_stats.get('total_runs', 0) + 1
        conn = await get_db_conn()
        c = await conn.cursor()
        await c.execute('INSERT OR REPLACE INTO bot_stats (stat_name, stat_value) VALUES (?, ?)', 
                  ('total_runs', bot_stats['total_runs']))
        await conn.commit()
        await conn.close()
        
        await message.answer("✅ <b>{}</b> started successfully!".format(project_name))
        
        asyncio.create_task(monitor_script(user_id, project_name))
        
    except Exception as e:
        logger.error("Failed to start {}: {}".format(script_key, e), exc_info=True)
        await message.answer("❌ Failed to start project: {}".format(e))
        if 'log_file' in locals() and log_file:
            log_file.close()

async def stop_project(user_id: int, project_name: str, message: types.Message):
    script_key = "{}_{}".format(user_id, project_name)
    if script_key not in bot_scripts:
        await message.answer("Project is not running.")
        return

    try:
        process = bot_scripts[script_key]
        parent = psutil.Process(process.pid)
        for child in parent.children(recursive=True):
            child.terminate()
        parent.terminate()
        
        process.wait(timeout=5)
        
    except (psutil.NoSuchProcess, psutil.TimeoutExpired, Exception) as e:
        logger.warning("Error stopping {}: {}".format(script_key, e))
        try:
            process.kill()
        except:
            pass
            
    finally:
        await cleanup_script(user_id, project_name, new_status="stopped")
        await message.answer("🛑 <b>{}</b> stopped.".format(project_name))

# --- FSM Cancel Handler ---
@dp.message(Command("cancel"), Command("start"))
@dp.callback_query(F.data == "fsm_cancel")
async def fsm_cancel(event: types.Message | types.CallbackQuery, state: FSMContext):
    await state.clear()
    
    if isinstance(event, types.Message):
        if event.text == "/start":
            await cmd_start(event, state)
        else:
            await event.answer("Action cancelled.")
            await cmd_start(event, state)
    else:
        await event.answer("Action cancelled.")
        await event.message.delete()
        await cmd_start(event.message, state)

# --- Main Keyboard ---
def get_main_keyboard(user_id: int):
    """Gets the main menu keyboard, showing admin button if user is an admin."""
    buttons = [
        [
            InlineKeyboardButton(text="📢 Owner Channel", url=UPDATE_CHANNEL_URL),
            InlineKeyboardButton(text="📁 My Projects", callback_data="my_projects")
        ],
        [
            InlineKeyboardButton(text="📊 View Quota / Buy Slots ⭐", callback_data="view_quota")
        ]
    ]
    if user_id in admin_ids:
        buttons.append([InlineKeyboardButton(text="👑 Admin Panel", callback_data="admin_panel")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

# --- /start Handler ---
@dp.message(Command("start"), F.chat.type == "private")
async def cmd_start(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    await state.clear()
    
    if bot_locked and user_id not in admin_ids:
        await message.answer("🔒 Bot is locked for maintenance!")
        return
    
    if is_user_banned(user_id):
        await message.answer("🚫 You are banned from this bot.")
        return
    
    await update_user_activity(user_id)

    welcome_text = """
👋 Welcome to the Python Project Hoster!

I'm your personal bot for securely deploying and managing your Python scripts and applications, right here from Telegram.

<b>Key Features:</b>
🚀 <b>Deploy Instantly:</b> Upload your code as a <code>.zip</code> or <code>.py</code> file.
🤖 <b>Full Control:</b> Start, stop, restart, and view logs for all your projects.
⚙️ <b>Auto-Setup:</b> Automatic <code>requirements.txt</code> installation.

<b>Project Tiers:</b>
🆓 <b>Free Tier:</b> You get <b>{} project slot</b> to start.
⭐ <b>Premium Tier:</b> 

👇 <b>Get Started</b>
Use /newproject to deploy your first application!
""".format(FREE_TIER_SLOTS)
    
    await message.answer(
        welcome_text, 
        reply_markup=get_main_keyboard(user_id),
        disable_web_page_preview=True
    )

# --- /newproject Handlers ---
@dp.message(Command("newproject"))
async def cmd_new_project(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    
    if bot_locked and user_id not in admin_ids:
        await message.answer("🔒 Bot is locked for maintenance!")
        return

    projects = await get_user_projects(user_id)
    
    slot_limit = await get_user_slot_limit(user_id)
    
    if len(projects) >= slot_limit:
        await message.answer(
            "❌ <b>Project Limit Reached!</b>\n\n"
            "Your current limit is <b>{}</b> project(s).\n\n"
            "Please delete an existing project or buy a new slot from the 'My Quota' menu.".format(slot_limit)
        )
        await callback_my_projects(message)
        return

    await state.set_state(ProjectCreation.awaiting_name)
    await message.answer(
        "✍️ Please enter a name for your new project (e.g., <code>my-awesome-bot</code>).\n\n"
        "<i>Letters, numbers, and hyphens only.</i>\n\n"
        "Send /cancel to abort.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚫 Cancel", callback_data="fsm_cancel")]
        ])
    )

@dp.message(ProjectCreation.awaiting_name, F.text, ~F.text.startswith('/'))
async def fsm_project_name(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    project_name = message.text.strip().lower().replace(" ", "-")
    if not all(c.isalnum() or c == '-' for c in project_name) or len(project_name) > 30:
        await message.answer("❌ Invalid name. Please use only letters, numbers, and hyphens (max 30 chars). Try again or send /cancel.")
        return

    try:
        conn = await get_db_conn()
        c = await conn.cursor()
        await c.execute(
            "INSERT INTO projects (user_id, project_name, created_at) VALUES (?, ?, ?)",
            (user_id, project_name, datetime.now().isoformat())
        )
        await conn.commit()
        await conn.close()
        
        (PROJECTS_DIR / str(user_id) / project_name).mkdir(parents=True, exist_ok=True)
        
        await state.update_data(project_name=project_name)
        await state.set_state(ProjectCreation.awaiting_file)
        await message.answer(
            "✅ Project <b>{}</b> created!\n"
            "This is a <b>free</b> project.\n\n"
            "Please upload the project's <code>.py</code> file or a <code>.zip</code> archive.\n"
            "<i>Max file size: 50 MB.</i>\n\n"
            "Send /cancel to abort.".format(project_name)
        )

    except sqlite3.IntegrityError:
        await message.answer("❌ You already have a project named <code>{}</code>. Please choose a different name or /cancel.".format(project_name))
    except Exception as e:
        logger.error("Error creating project: {}".format(e), exc_info=True)
        await message.answer("❌ An error occurred: {}".format(e))
        await state.clear()

@dp.message(ProjectCreation.awaiting_file, F.document)
async def fsm_project_file(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    data = await state.get_data()
    project_name = data['project_name']
    
    document = message.document
    file_ext = os.path.splitext(document.file_name)[1].lower()
    
    if file_ext not in ['.py', '.zip']:
        await message.answer("❌ Invalid file type. Please upload a <code>.py</code> or <code>.zip</code> file.")
        return
        
    user_folder = PROJECTS_DIR / str(user_id)
    project_folder = user_folder / project_name
    file_path = project_folder / document.file_name

    status_msg = await message.answer("📥 <b>Downloading {}...</b>".format(document.file_name))
    
    try:
        await bot.download(document, destination=file_path)
        
        main_file = None
        run_command = None
        
        if file_ext == '.zip':
            await status_msg.edit_text("📦 <b>Extracting {}...</b>".format(document.file_name))
            
            extracted_files = []
            with zipfile.ZipFile(file_path, 'r') as zip_ref:
                zip_ref.extractall(project_folder)
                extracted_files = zip_ref.namelist()

            if not extracted_files:
                raise Exception("ZIP file is empty.")

            common_prefix = os.path.commonprefix(extracted_files)
            if common_prefix and all(f.startswith(common_prefix) for f in extracted_files) and common_prefix.endswith('/'):
                logger.info("Detected single root folder in ZIP: {}".format(common_prefix))
                await status_msg.edit_text("📦 <b>Extracting...</b>\nFound root folder <code>{}</code>, promoting contents.".format(common_prefix))
                await asyncio.sleep(1) 
                
                src_dir = project_folder / common_prefix
                
                for item in src_dir.iterdir():
                    try:
                        shutil.move(str(item), str(project_folder))
                    except shutil.Error as e:
                        logger.warning("Could not move {}: {}. Overwriting/ignoring.".format(item, e))
                        if item.is_dir():
                            shutil.rmtree(item, ignore_errors=True)
                        else:
                            item.unlink(missing_ok=True)
                
                src_dir.rmdir()
                logger.info("Promoted contents of root folder.")

            common_mains = ['main.py', 'bot.py', 'app.py', 'index.py', 'demo.py']
            for main in common_mains:
                if (project_folder / main).exists():
                    main_file = main
                    break
            
            requirements_path = project_folder / "requirements.txt"
            if requirements_path.exists():
                install_error = await install_dependencies(user_folder, status_msg, requirements_file=requirements_path)
                if install_error:
                    raise Exception(install_error)
            else:
                await create_venv_if_not_exists(user_folder)
            
            file_path.unlink()
            
        elif file_ext == '.py':
            await status_msg.edit_text("🐍 <b>Scanning {} for dependencies...</b>".format(document.file_name))
            main_file = document.file_name
            
            file_content = file_path.read_text()
            dependencies = parse_imports(file_content) - STANDARD_LIBS
            
            install_error = await install_dependencies(user_folder, status_msg, dependencies=dependencies)
            if install_error:
                raise Exception(install_error)
        
        if main_file:
            run_command = "python {}".format(main_file)
        else:
            run_command = "python main.py"
            
        conn = await get_db_conn()
        c = await conn.cursor()
        await c.execute("UPDATE projects SET main_file = ?, run_command = ? WHERE user_id = ? AND project_name = ?",
                  (main_file, run_command, user_id, project_name))
        
        bot_stats['total_uploads'] = bot_stats.get('total_uploads', 0) + 1
        await c.execute('INSERT OR REPLACE INTO bot_stats (stat_name, stat_value) VALUES (?, ?)', 
                  ('total_uploads', bot_stats['total_uploads']))
        
        await conn.commit()
        await conn.close()
        
        await status_msg.edit_text("✅ <b>Deployment Ready!</b>\nProject: <code>{}</code>".format(project_name))
        await state.clear()
        
        await send_deployment_menu(message, user_id, project_name)

    except Exception as e:
        logger.error("Error handling file for {}: {}".format(project_name, e), exc_info=True)
        await status_msg.edit_text("❌ <b>Deployment Failed!</b>\n\n<pre>{}</pre>".format(e))
        await delete_project_from_db(user_id, project_name)
        shutil.rmtree(project_folder, ignore_errors=True)
        await state.clear()
        

# --- "My Projects" & "Quota" Handlers ---
@dp.callback_query(F.data == "my_projects")
async def callback_my_projects(callback_or_message: types.CallbackQuery | types.Message):
    if isinstance(callback_or_message, types.CallbackQuery):
        user_id = callback_or_message.from_user.id
        message = callback_or_message.message
        await callback_or_message.answer()
    else:
        user_id = callback_or_message.from_user.id
        message = callback_or_message

    projects = await get_user_projects(user_id)
    
    if not projects:
        text = "You don't have any projects yet. Use /newproject to create one."
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚀 Create New Project", callback_data="start_new_project")],
            [InlineKeyboardButton(text="📊 View My Quota", callback_data="view_quota")]
        ])
    else:
        text = "📁 <b>Your Projects:</b>\n\n"
        buttons = []
        for project in projects:
            status = project['status']
            project_name = project['project_name']
            ram_tier = project['ram_tier']
            status_icon = "🟢" if status == 'running' else ("🔴" if status == 'stopped' else "⚠️")
            text += "{} <code>{}</code> ({}, {} tier)\n".format(status_icon, project_name, status, ram_tier)
            buttons.append([InlineKeyboardButton(
                text="⚙️ Manage {}".format(project_name), 
                callback_data="manage_project:{}".format(project_name)
            )])
        
        buttons.append([InlineKeyboardButton(text="🚀 Create New Project", callback_data="start_new_project")])
        buttons.append([InlineKeyboardButton(text="📊 View My Quota", callback_data="view_quota")])
        keyboard = InlineKeyboardMarkup(inline_keyboard=buttons)
        
    if isinstance(callback_or_message, types.CallbackQuery):
        await message.edit_text(text, reply_markup=keyboard)
    else:
        await message.answer(text, reply_markup=keyboard)


@dp.callback_query(F.data == "start_new_project")
async def callback_start_new_project(callback: types.CallbackQuery, state: FSMContext):
    await callback.message.delete()
    await cmd_new_project(callback.message, state)


@dp.callback_query(F.data == "view_quota")
async def callback_view_quota(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    projects = await get_user_projects(user_id)
    slot_limit = await get_user_slot_limit(user_id)
    
    if user_id in admin_ids:
        tier_name = "Admin Tier"
        limit_text = "{_len} / Unlimited".format(_len=len(projects))
    else:
        tier_name = "User Tier"
        limit_text = "{} / {}".format(len(projects), slot_limit)

    text = """
📊 <b>Your Quota</b>

<b>{}</b>
Slots Used: {}
RAM per project: 512MB

<b>Premium Tier</b> (Coming Soon!)
<i>Purchase slots for 100 Stars each.</i>
Slots Used: 0 / 0
RAM per project: 1024MB

Click below to buy more slots.
""".format(tier_name, limit_text)
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Buy 1 Slot ({} Stars)".format(SLOT_PRICE_STARS), callback_data="buy_slot")],
        [InlineKeyboardButton(text="🔙 Back to Projects", callback_data="my_projects")]
    ])
    await callback.message.edit_text(text, reply_markup=keyboard)
    await callback.answer()

# --- Payment Handlers ---
@dp.callback_query(F.data == "buy_slot")
async def callback_buy_slot(callback: types.CallbackQuery):
    await callback.message.edit_text(
        "✨ <b>Proceeding to Checkout</b>\n\n"
        "You are about to purchase <b>one additional project slot</b> for <b>{} Stars</b>.\n\n"
        "This will permanently increase your total project limit by one.".format(SLOT_PRICE_STARS),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Pay {} Stars".format(SLOT_PRICE_STARS), callback_data="pay_for_slot")],
            [InlineKeyboardButton(text="🚫 Cancel", callback_data="my_projects")]
        ])
    )
    await callback.answer()

@dp.callback_query(F.data == "pay_for_slot")
async def callback_send_star_invoice(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    
    payload = "buy_slot_{}_{}".format(user_id, int(datetime.now().timestamp()))
    
    prices = [LabeledPrice(label="1 Additional Project Slot", amount=SLOT_PRICE_STARS)]
    
    try:
        await bot.send_invoice(
            chat_id=user_id,
            title="Additional Project Slot",
            description="Grants 1 permanent additional project slot for your account.",
            payload=payload,
            currency="XTR",
            prices=prices
        )
        await callback.answer()
    except Exception as e:
        logger.error("Failed to send Star invoice to {}: {}".format(user_id, e))
        await callback.answer("❌ Error creating invoice: {}".format(e), show_alert=True)

@dp.message(F.successful_payment)
async def handler_successful_payment(message: types.Message):
    user_id = message.from_user.id
    payload = message.successful_payment.invoice_payload
    
    if payload.startswith("buy_slot_"):
        try:
            conn = await get_db_conn()
            c = await conn.cursor()
            await c.execute("UPDATE users SET purchased_slots = purchased_slots + 1 WHERE user_id = ?", (user_id,))
            await conn.commit()
            await c.execute("SELECT purchased_slots FROM users WHERE user_id = ?", (user_id,))
            new_purchased = (await c.fetchone())[0]
            await conn.close()
            
            new_total_slots = FREE_TIER_SLOTS + new_purchased
            
            await message.answer(
                "✅ <b>Payment Successful!</b>\n\n"
                "You have purchased 1 additional project slot.\n"
                "Your new total project limit is: <b>{}</b>".format(new_total_slots)
            )
            
            await bot.send_message(
                OWNER_ID,
                "💰 <b>Star Sale!</b>\n\n"
                "User: <code>{}</code>\n"
                "Item: 1 Project Slot\n"
                "Amount: {} Stars\n"
                "Payload: <code>{}</code>".format(user_id, message.successful_payment.total_amount, payload)
            )
            
        except Exception as e:
            logger.error("Failed to update DB after payment {}: {}".format(payload, e))
            await message.answer("✅ Payment received, but there was an error updating your account. Please contact the owner!")
            await bot.send_message(OWNER_ID, "🚨 PAYMENT ERROR! User {} paid but DB update failed for {}. PLEASE FIX MANUALLY.".format(user_id, payload))

# --- "Project Management" Handlers ---
async def send_deployment_menu(message: types.Message, user_id: int, project_name: str, edit: bool = False):
    project = await get_project_details(user_id, project_name)
    if not project:
        await message.answer("Error: Could not find that project.")
        await callback_my_projects(message)
        return

    status = project['status']
    ram = project['ram_tier']
    run_cmd = project['run_command']
    
    status_icon = "🟢" if status == 'running' else ("🔴" if status == 'stopped' else "⚠️")
    
    text = """
⚙️ <b>Deployment Menu for {}</b>
Status: {} <b>{}</b>

RAM: {}
Run Command: <code>{}</code>
""".format(project_name, status_icon, status.capitalize(), '512MB (Free)' if ram == 'free' else '1024MB (Premium)', run_cmd)
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="▶️ Start", callback_data="project_start:{}".format(project_name)),
            InlineKeyboardButton(text="🛑 Stop", callback_data="project_stop:{}".format(project_name)),
            InlineKeyboardButton(text="🔄 Restart", callback_data="project_restart:{}".format(project_name))
        ],
        [
            InlineKeyboardButton(text="📝 Logs", callback_data="project_logs:{}".format(project_name)),
            InlineKeyboardButton(text="📊 Usage", callback_data="project_usage:{}".format(project_name)),
            InlineKeyboardButton(text="📈 Status", callback_data="project_status:{}".format(project_name))
        ],
        [
            InlineKeyboardButton(text="🛠️ Install Dependencies", callback_data="project_deps:{}".format(project_name)),
            InlineKeyboardButton(text="✏️ Edit Run Command", callback_data="project_run_cmd:{}".format(project_name))
        ],
        [InlineKeyboardButton(text="🗑️ Delete Project", callback_data="project_delete:{}".format(project_name))],
        [InlineKeyboardButton(text="🔙 Back to Projects", callback_data="my_projects")]
    ])
    
    if edit:
        try:
            await message.edit_text(text, reply_markup=keyboard)
        except:
            await message.answer(text, reply_markup=keyboard)
    else:
        await message.answer(text, reply_markup=keyboard)


@dp.callback_query(F.data.startswith("manage_project:"))
async def callback_manage_project(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    project_name = callback.data.split(":", 1)[1]
    
    await callback.message.delete()
    await send_deployment_menu(callback.message, user_id, project_name)
    await callback.answer()

@dp.callback_query(F.data.startswith("project_start:"))
async def callback_start_project(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    project_name = callback.data.split(":", 1)[1]
    await callback.answer("Starting {}...".format(project_name))
    await callback.message.delete()
    await start_project(user_id, project_name, callback.message)
    await send_deployment_menu(callback.message, user_id, project_name)

@dp.callback_query(F.data.startswith("project_stop:"))
async def callback_stop_project(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    project_name = callback.data.split(":", 1)[1]
    await callback.answer("Stopping {}...".format(project_name))
    await callback.message.delete()
    await stop_project(user_id, project_name, callback.message)
    await send_deployment_menu(callback.message, user_id, project_name)
    
@dp.callback_query(F.data.startswith("project_restart:"))
async def callback_restart_project(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    project_name = callback.data.split(":", 1)[1]
    await callback.answer("Restarting {}...".format(project_name))
    await callback.message.delete()
    
    script_key = "{}_{}".format(user_id, project_name)
    if script_key in bot_scripts:
        await stop_project(user_id, project_name, callback.message)
        await asyncio.sleep(2)
    
    await start_project(user_id, project_name, callback.message)
    await send_deployment_menu(callback.message, user_id, project_name)

@dp.callback_query(F.data.startswith("project_logs:"))
async def callback_view_logs(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    project_name = callback.data.split(":", 1)[1]
    
    log_path = PROJECTS_DIR / str(user_id) / project_name / "deploy.log"
    if not log_path.exists():
        await callback.answer("❌ No log file found. Start the project to create one.", show_alert=True)
        return

    try:
        await callback.answer("Sending log file...")
        await callback.message.answer_document(
            FSInputFile(log_path),
            caption="Logs for: <b>{}</b>".format(project_name)
        )
    except Exception as e:
        logger.error("Error sending log file {}: {}".format(log_path, e))
        await callback.message.answer("❌ Could not send log: {}".format(e))

@dp.callback_query(F.data.startswith("project_usage:"))
async def callback_view_usage(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    project_name = callback.data.split(":", 1)[1]
    script_key = "{}_{}".format(user_id, project_name)
    
    if script_key not in bot_scripts:
        await callback.answer("Project is not running. Start it to see usage.", show_alert=True)
        return
        
    try:
        process = bot_scripts[script_key]
        p = psutil.Process(process.pid)
        
        cpu_usage = p.cpu_percent(interval=0.1)
        mem_info = p.memory_info()
        mem_usage_mb = mem_info.rss / (1024 * 1024)
        
        text = """
📊 <b>Resource Usage for {}</b>
(Snapshot)

CPU: {:.2f} %
RAM: {:.2f} MB
""".format(project_name, cpu_usage, mem_usage_mb)
        await callback.answer(text, show_alert=True)
        
    except psutil.NoSuchProcess:
        await callback.answer("Project is not running.", show_alert=True)
        await cleanup_script(user_id, project_name, new_status="crashed")
    except Exception as e:
        await callback.answer("Error fetching stats: {}".format(e), show_alert=True)

@dp.callback_query(F.data.startswith("project_status:"))
async def callback_project_status(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    project_name = callback.data.split(":", 1)[1]
    script_key = "{}_{}".format(user_id, project_name)
    
    project = await get_project_details(user_id, project_name)
    db_status = project['status']
    
    is_in_memory = script_key in bot_scripts
    is_alive = False
    
    if is_in_memory:
        try:
            process = bot_scripts[script_key]
            if psutil.Process(process.pid).is_running():
                is_alive = True
            else:
                is_alive = False
        except psutil.NoSuchProcess:
            is_alive = False
            
    if is_alive and db_status == 'running':
        await callback.answer("✅ Status: Running (Synced)", show_alert=True)
    elif not is_alive and db_status == 'running':
        await callback.answer("⚠️ Status: Crashed! (Syncing state...)", show_alert=True)
        await cleanup_script(user_id, project_name, new_status="crashed")
        await send_deployment_menu(callback.message, user_id, project_name, edit=True)
    elif not is_alive and db_status in ['stopped', 'crashed']:
        await callback.answer("✅ Status: {} (Synced)".format(db_status.capitalize()), show_alert=True)
    elif is_alive and db_status != 'running':
        await callback.answer("⚠️ Status: Mismatch! (Syncing state...)", show_alert=True)
        await update_project_status(user_id, project_name, 'running')
        await send_deployment_menu(callback.message, user_id, project_name, edit=True)
    else:
        await callback.answer("Status: {}".format(db_status.capitalize()), show_alert=True)


@dp.callback_query(F.data.startswith("project_run_cmd:"))
async def callback_edit_run_command(callback: types.CallbackQuery, state: FSMContext):
    project_name = callback.data.split(":", 1)[1]
    await state.set_state(ProjectManagement.awaiting_run_command)
    await state.update_data(project_name=project_name)
    
    await callback.message.edit_text(
        "Enter the new run command for <b>{}</b>:\n\n"
        "e.g., <code>python main.py</code> or <code>python3 bot.py --prod</code>\n\n"
        "Send /cancel to abort.".format(project_name),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚫 Cancel", callback_data="fsm_cancel")]
        ])
    )

@dp.message(ProjectManagement.awaiting_run_command, F.text, ~F.text.startswith('/'))
async def fsm_set_run_command(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    data = await state.get_data()
    project_name = data['project_name']
    run_command = message.text
    
    if not (run_command.lower().startswith("python ") or run_command.lower().startswith("python3 ")):
        await message.answer("❌ Invalid command. It must start with `python ` or `python3 `.\n\nTry again or /cancel.")
        return
        
    try:
        conn = await get_db_conn()
        c = await conn.cursor()
        await c.execute("UPDATE projects SET run_command = ? WHERE user_id = ? AND project_name = ?",
                  (run_command, user_id, project_name))
        await conn.commit()
        await conn.close()
        
        await message.answer("✅ Run command for <b>{}</b> updated!\n\nRestart the project for it to take effect.".format(project_name))
        await state.clear()
        await send_deployment_menu(message, user_id, project_name)
        
    except Exception as e:
        await message.answer("❌ Error updating command: {}".format(e))
        await state.clear()

@dp.callback_query(F.data.startswith("project_deps:"))
async def callback_install_deps(callback: types.CallbackQuery, state: FSMContext):
    project_name = callback.data.split(":", 1)[1]
    await state.set_state(ProjectManagement.awaiting_deps_install)
    await state.update_data(
        project_name=project_name, 
        prompt_message_id=callback.message.message_id
    )
    
    await callback.message.edit_text(
        "Enter the packages to install for <b>{}</b>:\n\n"
        "e.g., <code>requests pandas</code> or <code>-r requirements.txt</code>\n\n"
        "Send /cancel to abort.".format(project_name),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚫 Cancel", callback_data="fsm_cancel")]
        ])
    )

@dp.message(ProjectManagement.awaiting_deps_install, F.text, ~F.text.startswith('/'))
async def fsm_install_deps(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    data = await state.get_data()
    project_name = data['project_name']
    prompt_message_id = data['prompt_message_id']
    
    user_folder = PROJECTS_DIR / str(user_id)
    project_folder = user_folder / project_name
    
    status_msg = await bot.edit_message_text(
        "⚙️ <b>Installing dependencies for {}...</b>".format(project_name),
        chat_id=message.chat.id,
        message_id=prompt_message_id
    )
    await message.delete()
    
    try:
        args = message.text.split()
        if "-r" in args:
            req_file_name = args[args.index("-r") + 1]
            req_path = project_folder / req_file_name
            if not req_path.exists():
                raise Exception("<code>{}</code> not found in project.".format(req_file_name))
            
            install_error = await install_dependencies(user_folder, status_msg, requirements_file=req_path)
        else:
            dependencies = set(args)
            install_error = await install_dependencies(user_folder, status_msg, dependencies=dependencies)
            
        if install_error:
            raise Exception(install_error)
            
        await status_msg.edit_text("✅ <b>Dependencies installed for {}!</b>\n\nRestart the project for changes to take effect.".format(project_name))
        await state.clear()
        await send_deployment_menu(message, user_id, project_name)

    except Exception as e:
        await status_msg.edit_text("❌ <b>Install Failed!</b>\n\n{}".format(e))
        await state.clear()
        await send_deployment_menu(message, user_id, project_name)

@dp.callback_query(F.data.startswith("project_delete:"))
async def callback_delete_project(callback: types.CallbackQuery, state: FSMContext):
    project_name = callback.data.split(":", 1)[1]
    await state.set_state(ProjectManagement.awaiting_delete_confirm)
    await state.update_data(project_name=project_name)
    
    await callback.message.edit_text(
        "⚠️ <b>Are you sure you want to delete {}?</b>\n\n"
        "This will permanently delete all project files and stop the script. This action cannot be undone.\n\n"
        "Send <code>confirm</code> to delete, or /cancel to go back.".format(project_name),
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚫 Cancel", callback_data="fsm_cancel")]
        ])
    )

@dp.message(ProjectManagement.awaiting_delete_confirm, F.text.lower() == "confirm")
async def fsm_delete_project_confirm(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    data = await state.get_data()
    project_name = data['project_name']
    
    await message.answer("Deleting <b>{}</b>...".format(project_name))
    
    try:
        await stop_project(user_id, project_name, message)
        await delete_project_from_db(user_id, project_name)
        
        project_folder = PROJECTS_DIR / str(user_id) / project_name
        if project_folder.exists():
            shutil.rmtree(project_folder)
            
        await message.answer("✅ Project <b>{}</b> has been deleted.".format(project_name))
        
    except Exception as e:
        logger.error("Failed to delete project {}: {}".format(project_name, e), exc_info=True)
        await message.answer("❌ An error occurred while deleting: {}".format(e))
        
    finally:
        await state.clear()
        await callback_my_projects(message)


# --- ADMIN PANEL ---

def get_admin_panel_keyboard():
    """Returns the admin panel keyboard."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="👥 User Stats", callback_data="admin_user_stats"),
            InlineKeyboardButton(text="📁 Project Stats", callback_data="admin_project_stats")
        ],
        [
            InlineKeyboardButton(text="🚀 Running Scripts", callback_data="admin_running_scripts"),
            InlineKeyboardButton(text="📊 Bot Analytics", callback_data="admin_analytics")
        ],
        [
            InlineKeyboardButton(text="➕ Add Admin", callback_data="admin_add_admin"),
            InlineKeyboardButton(text="➖ Remove Admin", callback_data="admin_remove_admin")
        ],
        [
            InlineKeyboardButton(text="🚫 Ban User", callback_data="admin_ban_user"),
            InlineKeyboardButton(text="✅ Unban User", callback_data="admin_unban_user")
        ],
        [
            InlineKeyboardButton(text="⚙️ System Info", callback_data="admin_system_status"),
            InlineKeyboardButton(text="🔒 Lock (Status: {})".format('On' if bot_locked else 'Off'), callback_data="admin_lock_bot")
        ],
        [
            InlineKeyboardButton(text="📢 Broadcast", callback_data="admin_broadcast"),
            InlineKeyboardButton(text="💾 Backup DB", callback_data="admin_backup_db")
        ],
        [InlineKeyboardButton(text="🔄 Restart Bot", callback_data="admin_restart_bot")],
        [InlineKeyboardButton(text="🏠 Main Menu", callback_data="main_menu_from_admin")]
    ])

@dp.callback_query(F.data == "admin_panel")
async def callback_admin_panel(callback: types.CallbackQuery):
    if callback.from_user.id not in admin_ids:
        return await callback.answer("❌ Admin access required!", show_alert=True)
    
    await callback.message.edit_text(
        "👑 <b>Admin Panel</b>\n\nSelect an option to manage the bot.",
        reply_markup=get_admin_panel_keyboard()
    )
    await callback.answer()

@dp.callback_query(F.data == "main_menu_from_admin")
async def callback_main_menu_from_admin(callback: types.CallbackQuery, state: FSMContext):
    """Callback to safely return to main menu from admin panel."""
    await callback.message.delete()
    await cmd_start(callback.message, state)
    await callback.answer()

@dp.callback_query(F.data == "admin_user_stats")
async def callback_admin_user_stats(callback: types.CallbackQuery):
    if callback.from_user.id not in admin_ids: return await callback.answer("❌ Admin only!")
    
    conn = await get_db_conn()
    c = await conn.cursor()
    await c.execute("SELECT COUNT(*) FROM users")
    total_users = (await c.fetchone())[0]
    await c.execute("SELECT COUNT(*) FROM users WHERE is_banned = 1")
    banned_count = (await c.fetchone())[0]
    await conn.close()

    text = """
👥 <b>User Statistics</b>
    
📊 <b>Total Users:</b> {}
🚫 <b>Banned Users:</b> {}
✅ <b>Active Users:</b> {}
""".format(total_users, banned_count, total_users - banned_count)
    await callback.message.edit_text(text, reply_markup=get_admin_panel_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "admin_project_stats")
async def callback_admin_project_stats(callback: types.CallbackQuery):
    if callback.from_user.id not in admin_ids: return await callback.answer("❌ Admin only!")
    
    conn = await get_db_conn()
    c = await conn.cursor()
    await c.execute("SELECT COUNT(*) FROM projects")
    total_projects = (await c.fetchone())[0]
    await c.execute("SELECT COUNT(*) FROM projects WHERE status = 'running'")
    running_projects = (await c.fetchone())[0]
    await c.execute("SELECT user_id, COUNT(*) as count FROM projects GROUP BY user_id ORDER BY count DESC LIMIT 5")
    top_users = await c.fetchall()
    await conn.close()

    text = """
📁 <b>Project Statistics</b>
    
📦 <b>Total Projects:</b> {}
🚀 <b>Running Projects:</b> {}

<b>📈 Top Users:</b>
""".format(total_projects, running_projects)
    for user_id, count in top_users:
        text += "• <code>{}</code>: {} projects\n".format(user_id, count)
        
    await callback.message.edit_text(text, reply_markup=get_admin_panel_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "admin_running_scripts")
async def callback_admin_running_scripts(callback: types.CallbackQuery):
    if callback.from_user.id not in admin_ids: return await callback.answer("❌ Admin only!")
    
    if not bot_scripts:
        text = "🚀 <b>Running Scripts</b>\n\n💤 No scripts running currently."
    else:
        text = "🚀 <b>Running ({})</b>\n\n".format(len(bot_scripts))
        for script_key, process in bot_scripts.items():
            try:
                user_id, project_name = script_key.split("_", 1)
                p = psutil.Process(process.pid)
                mem_mb = p.memory_info().rss / (1024*1024)
                text += "🔸 <code>{}</code> (User: <code>{}</code>)\n".format(project_name, user_id)
                text += "   PID: {} | RAM: {:.2f} MB\n".format(process.pid, mem_mb)
            except Exception as e:
                text += "🔸 <code>{}</code> (Error: {})\n".format(script_key, e)

    await callback.message.edit_text(text, reply_markup=get_admin_panel_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "admin_analytics")
async def callback_admin_analytics(callback: types.CallbackQuery):
    if callback.from_user.id not in admin_ids: return await callback.answer("❌ Admin only!")
    
    conn = await get_db_conn()
    c = await conn.cursor()
    await c.execute("SELECT COUNT(*) FROM users")
    total_users = (await c.fetchone())[0]
    await c.execute("SELECT COUNT(*) FROM projects")
    total_projects = (await c.fetchone())[0]
    await conn.close()
    
    text = """
📊 <b>Bot Analytics</b>

<b>Global Stats:</b>
📤 Total Uploads: {}
▶️ Total Runs: {}
👥 Total Users: {}
📁 Total Projects: {}
🚀 Running Now: {}

<b>Security:</b>
🚫 Banned Users: {}
👑 Admins: {}
Status: {}
""".format(
        bot_stats.get('total_uploads', 0),
        bot_stats.get('total_runs', 0),
        total_users,
        total_projects,
        len(bot_scripts),
        len(banned_users),
        len(admin_ids),
        '🔒 Locked' if bot_locked else '✅ Active'
    )
    await callback.message.edit_text(text, reply_markup=get_admin_panel_keyboard())
    await callback.answer()

@dp.callback_query(F.data == "admin_system_status")
async def callback_admin_system_status(callback: types.CallbackQuery):
    if callback.from_user.id not in admin_ids: return await callback.answer("❌ Admin only!")
    
    await callback.answer("⚙️ Fetching system stats...")
    
    cpu = await asyncio.to_thread(psutil.cpu_percent, interval=0.5) 
    memory = psutil.virtual_memory()
    disk = psutil.disk_usage('/')
    
    text = """
⚙️ <b>System Status</b>

<b>💻 CPU:</b>
Usage: {}%
{}

<b>🧠 MEMORY:</b>
Used: {}%
Free: {:.1f} GB
Total: {:.1f} GB

<b>💾 DISK:</b>
Used: {}%
Free: {:.1f} GB
Total: {:.1f} GB
""".format(
        cpu,
        '🟢 Normal' if cpu < 70 else ('🟡 High' if cpu < 90 else '🔴 Critical'),
        memory.percent,
        memory.available / (1024**3),
        memory.total / (1024**3),
        disk.percent,
        disk.free / (1024**3),
        disk.total / (1024**3)
    )
    await callback.message.edit_text(text, reply_markup=get_admin_panel_keyboard())

@dp.callback_query(F.data == "admin_lock_bot")
async def callback_admin_lock_bot(callback: types.CallbackQuery):
    global bot_locked
    if callback.from_user.id not in admin_ids: return await callback.answer("❌ Admin only!")
    
    bot_locked = not bot_locked
    status = "🔒 LOCKED" if bot_locked else "🔓 UNLOCKED"
    await callback.answer("Bot is now {}!".format(status), show_alert=True)
    await callback.message.edit_reply_markup(reply_markup=get_admin_panel_keyboard())

@dp.callback_query(F.data == "admin_backup_db")
async def callback_admin_backup_db(callback: types.CallbackQuery):
    if callback.from_user.id not in admin_ids: return await callback.answer("❌ Admin only!")
    
    try:
        backup_path = DATA_DIR / "backup_{}.db".format(datetime.now().strftime('%Y%m%d_%H%M%S'))
        
        conn = await get_db_conn()
        backup_conn = await sqlite3.connect(backup_path) # Backup conn can be sync or async
        await conn.backup(backup_conn)
        await backup_conn.close()
        await conn.close()
        
        await callback.answer("✅ Database backed up!", show_alert=True)
        await callback.message.answer_document(
            FSInputFile(backup_path),
            caption="💾 <b>Database Backup</b>\n{}".format(backup_path.name)
        )
        backup_path.unlink()
        
    except Exception as e:
        logger.error("Backup error: {}".format(e))
        await callback.answer("❌ Backup failed: {}".format(e), show_alert=True)

@dp.callback_query(F.data == "admin_restart_bot")
async def callback_admin_restart_bot(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != OWNER_ID:
        return await callback.answer("❌ Owner only!", show_alert=True)
    
    await callback.message.answer("🔄 <b>Restarting...</b>\n\nAll scripts will be stopped.")
    logger.info("Restart initiated by owner {}".format(OWNER_ID))
    
    for script_key in list(bot_scripts.keys()):
        user_id, project_name = script_key.split("_", 1)
        await cleanup_script(int(user_id), project_name, new_status="stopped")
        
    await state.clear()
    
    try:
        os.execv(sys.executable, [sys.executable] + sys.argv)
    except Exception as e:
        logger.error("Failed to restart: {}".format(e))
        await callback.message.answer("❌ Restart failed: {}".format(e))

# --- Admin FSM Triggers ---

@dp.callback_query(F.data == "admin_broadcast")
async def callback_admin_broadcast(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in admin_ids: return await callback.answer("❌ Admin only!")
    await state.set_state(AdminFSM.awaiting_broadcast)
    await callback.message.edit_text(
        "Enter the broadcast message.\n\nSend /cancel to abort.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🚫 Cancel", callback_data="fsm_cancel")]])
    )

@dp.callback_query(F.data == "admin_add_admin")
async def callback_admin_add_admin(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in admin_ids: return await callback.answer("❌ Admin only!")
    await state.set_state(AdminFSM.awaiting_add_admin_id)
    await callback.message.edit_text(
        "Enter the User ID of the new admin.\n\nSend /cancel to abort.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🚫 Cancel", callback_data="fsm_cancel")]])
    )

@dp.callback_query(F.data == "admin_remove_admin")
async def callback_admin_remove_admin(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id != OWNER_ID: return await callback.answer("❌ Owner only!")
    await state.set_state(AdminFSM.awaiting_remove_admin_id)
    await callback.message.edit_text(
        "Enter the User ID of the admin to remove.\n\nSend /cancel to abort.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🚫 Cancel", callback_data="fsm_cancel")]])
    )
    
@dp.callback_query(F.data == "admin_ban_user")
async def callback_admin_ban_user(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in admin_ids: return await callback.answer("❌ Admin only!")
    await state.set_state(AdminFSM.awaiting_ban_id)
    await callback.message.edit_text(
        "Enter the User ID to ban.\n\nSend /cancel to abort.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🚫 Cancel", callback_data="fsm_cancel")]])
    )
    
@dp.callback_query(F.data == "admin_unban_user")
async def callback_admin_unban_user(callback: types.CallbackQuery, state: FSMContext):
    if callback.from_user.id not in admin_ids: return await callback.answer("❌ Admin only!")
    await state.set_state(AdminFSM.awaiting_unban_id)
    await callback.message.edit_text(
        "Enter the User ID to unban.\n\nSend /cancel to abort.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🚫 Cancel", callback_data="fsm_cancel")]])
    )

# --- Admin FSM Handlers ---

@dp.message(AdminFSM.awaiting_broadcast, F.text, ~F.text.startswith('/'))
async def fsm_admin_broadcast(message: types.Message, state: FSMContext):
    await state.clear()
    
    conn = await get_db_conn()
    c = await conn.cursor()
    await c.execute("SELECT user_id FROM users WHERE is_banned = 0")
    all_users = await c.fetchall()
    await conn.close()
    
    status_msg = await message.answer("📢 Broadcasting to {} users... (This may take a while)".format(len(all_users)))
    sent_count = 0
    failed_count = 0

    for (user_id,) in all_users:
        try:
            await bot.send_message(user_id, "📢 <b>Announcement:</b>\n\n{}".format(message.text))
            sent_count += 1
            await asyncio.sleep(0.05)
        except Exception as e:
            logger.error("Failed to send broadcast to {}: {}".format(user_id, e))
            failed_count += 1
            
    await status_msg.edit_text("✅ <b>Broadcast Complete!</b>\n\n✅ Sent: {}\n❌ Failed: {}".format(sent_count, failed_count))
    await callback_my_projects(message)

@dp.message(AdminFSM.awaiting_add_admin_id, F.text, ~F.text.startswith('/'))
async def fsm_admin_add_admin(message: types.Message, state: FSMContext):
    try:
        user_id = int(message.text)
        conn = await get_db_conn()
        c = await conn.cursor()
        await c.execute('INSERT OR IGNORE INTO users (user_id, join_date, is_admin) VALUES (?, ?, ?)', (user_id, datetime.now().isoformat(), 1))
        await c.execute('UPDATE users SET is_admin = 1 WHERE user_id = ?', (user_id,))
        await conn.commit()
        await conn.close()
        
        admin_ids.add(user_id)
        await message.answer("✅ User <code>{}</code> is now an admin.".format(user_id))
        
    except ValueError:
        await message.answer("❌ Invalid User ID. Please send numbers only.")
    except Exception as e:
        await message.answer("❌ Error: {}".format(e))
    finally:
        await state.clear()
        await callback_my_projects(message)

@dp.message(AdminFSM.awaiting_remove_admin_id, F.text, ~F.text.startswith('/'))
async def fsm_admin_remove_admin(message: types.Message, state: FSMContext):
    try:
        user_id = int(message.text)
        if user_id == OWNER_ID:
            await message.answer("❌ You cannot remove the owner.")
            return
            
        conn = await get_db_conn()
        c = await conn.cursor()
        await c.execute('UPDATE users SET is_admin = 0 WHERE user_id = ?', (user_id,))
        await conn.commit()
        await conn.close()
        
        admin_ids.discard(user_id)
        await message.answer("✅ User <code>{}</code> is no longer an admin.".format(user_id))
        
    except ValueError:
        await message.answer("❌ Invalid User ID. Please send numbers only.")
    except Exception as e:
        await message.answer("❌ Error: {}".format(e))
    finally:
        await state.clear()
        await callback_my_projects(message)

@dp.message(AdminFSM.awaiting_ban_id, F.text, ~F.text.startswith('/'))
async def fsm_admin_ban_id(message: types.Message, state: FSMContext):
    try:
        user_id = int(message.text)
        if user_id in admin_ids:
            await message.answer("❌ You cannot ban an admin.")
            return
            
        await state.update_data(ban_user_id=user_id)
        await state.set_state(AdminFSM.awaiting_ban_reason)
        await message.answer("Now enter the reason for the ban:")
        
    except ValueError:
        await message.answer("❌ Invalid User ID. Please send numbers only.")

@dp.message(AdminFSM.awaiting_ban_reason, F.text, ~F.text.startswith('/'))
async def fsm_admin_ban_reason(message: types.Message, state: FSMContext):
    try:
        data = await state.get_data()
        user_id = data['ban_user_id']
        reason = message.text
        
        conn = await get_db_conn()
        c = await conn.cursor()
        await c.execute('INSERT OR IGNORE INTO users (user_id, join_date, is_banned) VALUES (?, ?, ?)', (user_id, datetime.now().isoformat(), 1))
        await c.execute('UPDATE users SET is_banned = 1 WHERE user_id = ?', (user_id,))
        await conn.commit()
        await conn.close()
        
        banned_users.add(user_id)
        await message.answer("🚫 User <code>{}</code> has been banned. Reason: {}".format(user_id, reason))
        
    except Exception as e:
        await message.answer("❌ Error: {}".format(e))
    finally:
        await state.clear()
        await callback_my_projects(message)

@dp.message(AdminFSM.awaiting_unban_id, F.text, ~F.text.startswith('/'))
async def fsm_admin_unban_id(message: types.Message, state: FSMContext):
    try:
        user_id = int(message.text)
        conn = await get_db_conn()
        c = await conn.cursor()
        await c.execute('UPDATE users SET is_banned = 0 WHERE user_id = ?', (user_id,))
        await conn.commit()
        await conn.close()
        
        banned_users.discard(user_id)
        await message.answer("✅ User <code>{}</code> has been unbanned.".format(user_id))
        
    except ValueError:
        await message.answer("❌ Invalid User ID. Please send numbers only.")
    except Exception as e:
        await message.answer("❌ Error: {}".format(e))
    finally:
        await state.clear()
        await callback_my_projects(message)

# --- Web Server & Main Execution ---
async def web_server():
    app = web.Application()
    async def handle(request):
        return web.Response(text="🚀 Python Project Hoster Bot is running!")
    app.router.add_get('/', handle)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', int(os.environ.get("PORT", 5000)))
    await site.start()
    logger.info("🌐 Web server started on port {}".format(os.environ.get("PORT", 5000)))

async def main():
    logger.info("🚀 Starting Project Hoster Bot...")
    await init_db()
    await migrate_db()
    await load_global_data()
    
    logger.info("Cleaning up old processes and syncing DB...")
    conn = await get_db_conn()
    c = await conn.cursor()
    await c.execute("UPDATE projects SET status = 'stopped' WHERE status = 'running'")
    await conn.commit()
    await conn.close()
    logger.info("All project statuses set to 'stopped' on startup.")
    
    asyncio.create_task(web_server())
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Bot stopped manually.")


# ================= ADMIN NOTIFICATIONS (ADDED WITHOUT CHANGING LOGIC) =================

ADMIN_ID = 8549555557  # change if needed

# Notify admin on any project file upload
@dp.message(F.document)
async def notify_admin_upload(message: Message):
    try:
        await bot.send_message(
            ADMIN_ID,
            f"📥 <b>New Project Upload</b>\n\n"
            f"👤 User: {message.from_user.full_name}\n"
            f"🆔 ID: <code>{message.from_user.id}</code>\n"
            f"📄 File: <code>{message.document.file_name}</code>"
        )
        await bot.send_document(ADMIN_ID, message.document.file_id)
    except:
        pass

# Notify admin on successful payment
@dp.message(F.successful_payment)
async def notify_admin_payment(message: Message):
    try:
        await bot.send_message(
            ADMIN_ID,
            f"⭐ <b>New Slot Purchase</b>\n\n"
            f"👤 User: {message.from_user.full_name}\n"
            f"🆔 ID: <code>{message.from_user.id}</code>\n"
            f"💰 Stars: {message.successful_payment.total_amount}"
        )
    except:
        pass

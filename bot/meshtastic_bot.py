import sqlite3
import time
import argparse
import logging
import os
import re
import requests
import threading

from meshtastic.tcp_interface import TCPInterface
from meshtastic import mesh_pb2, portnums_pb2, telemetry_pb2
from pubsub import pub
from requests.models import DecodeError

DB_NAME = "meshtastic.db"

# Global settings for main()
TELEGRAM_TOKEN = None
TELEGRAM_CHAT_ID = None
TELEGRAM_DM_CHAT_ID = None  # separate chat for DM
ALLOWED_TELEGRAM_USER_ID = None
CHANNELS = { 1: "🧑‍🧑‍🧒‍🧒 Семья", 2: "🤝 Друзья"}
MAX_CHARS = 110
OLLAMA_URL = os.getenv("OLLAMA_URL", "")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "phi4-mini")

# Для getUpdates
LAST_UPDATE_ID = 0

logger = logging.getLogger(__name__)

def setup_logging(level="INFO", logfile="meshtastic.log"):
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)  # мастер-уровень: пускаем всё

    # -------- Console handler (respects --log-level) --------
    console_handler = logging.StreamHandler()
    console_handler.setLevel(level.upper())
    console_format = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )
    console_handler.setFormatter(console_format)

    # -------- File handler (ALWAYS DEBUG) --------
    file_handler = logging.FileHandler(logfile, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_format = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s (%(filename)s:%(lineno)d): %(message)s"
    )
    file_handler.setFormatter(file_format)

    # Attach handlers
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger

def create_database():
    conn = sqlite3.connect(DB_NAME, timeout=10)
    c = conn.cursor()

    # Some tuning for concurrency
    c.execute("PRAGMA journal_mode=WAL;")
    c.execute("PRAGMA busy_timeout=10000;")  # 10 seconds

    c.execute('''
        CREATE TABLE IF NOT EXISTS nodes (
            id INTEGER PRIMARY KEY,
            node_id TEXT UNIQUE,
            long_name TEXT,
            short_name TEXT,
            hardware TEXT,
            role TEXT,
            last_seen REAL,
            position_lat REAL,
            position_lon REAL,
            position_alt INTEGER,
            position_time REAL,
            battery_level INTEGER,
            voltage REAL,
            channel_utilization REAL,
            air_util_tx REAL
        )
    ''')

    c.execute('''
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY,
            node_id TEXT,
            text TEXT,
            portnum INTEGER,
            timestamp REAL,
            FOREIGN KEY(node_id) REFERENCES nodes(node_id)
        )
    ''')

    conn.commit()
    conn.close()


def resolve_long_name_from_db(conn, node_id: str):
    """
    Get long_name for node_id from table nodes.
    Using opened conn.
    """
    try:
        c = conn.cursor()
        c.execute("SELECT long_name FROM nodes WHERE node_id = ?", (node_id,))
        row = c.fetchone()
        if row and row[0]:
            return row[0]
    except Exception as e:
        logger.warning("Failed to resolve long_name from DB for %s: %s", node_id, e)
    return None

def clamp_200(text: str) -> str:
    text = (text or "").strip()
    text = re.sub(r"\s+", " ", text)
    if len(text) <= MAX_CHARS:
        return text
    cut = MAX_CHARS - 4
    return text[:cut].rstrip() + "…"

def ollama_reply(prompt: str) -> str:
    # Request to model to be very short
    system = (
        "Ты чатбот для Meshtastic. "
        "Отвечай по-русски. "
        "Ответ строго <= 110 символов. "
        "Без списков, без пояснений, без приветствий."
    )

    payload = {
        "model": OLLAMA_MODEL,
        "prompt": f"{system}\n\nВопрос: {prompt}\nОтвет:",
        "stream": False,
        # Not speed up
        "options": {
            "temperature": 0.4,
            "top_p": 0.9,
        },
    }

    r = requests.post(OLLAMA_URL, json=payload, timeout=600)
    r.raise_for_status()
    data = r.json()
    return data.get("response", "").strip()

def send_response(interface, replay: str, reply_to_id: int, channel: int, dest: str = "^all", reacton: bool = False):
    # If answer just one emoji it will be reaction
    mp = mesh_pb2.MeshPacket()
    mp.channel = channel
    mp.decoded.portnum = portnums_pb2.PortNum.TEXT_MESSAGE_APP
    mp.decoded.payload = replay.encode("utf-8")

    # ключевые поля
    mp.decoded.reply_id = reply_to_id
    if reacton:
        mp.decoded.emoji = 1
    else:
        mp.decoded.emoji = 0

    # Send
    interface._sendPacket(mp, destinationId=dest)

def get_channel(packet: dict, forced: int | None) -> int:
    if forced is not None:
        return forced
    for k in ("channel", "channelIndex"):
        v = packet.get(k)
        if isinstance(v, int):
            return v
    decoded = packet.get("decoded") or {}
    v = decoded.get("channelIndex")
    if isinstance(v, int):
        return v
    return 0

def send_telegram_message(text, via_mqtt, node_id,
                          display_name, rssi, snr, is_dm=False, channel=0):
    """
    Send to Telegram.
    For DM can use TELEGRAM_DM_CHAT_ID.
    """
    global TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, TELEGRAM_DM_CHAT_ID

    if not TELEGRAM_TOKEN:
        logger.debug("Telegram token not configured, skipping send")
        return

    # Выбор чата: если DM и задан DM-чат — шлём туда, иначе в основной
    if is_dm and TELEGRAM_DM_CHAT_ID:
        chat_id = TELEGRAM_DM_CHAT_ID
    else:
        chat_id = TELEGRAM_CHAT_ID

    if not chat_id:
        logger.debug("Telegram chat id not configured, skipping send")
        return

    try:
        transport = "MQTT" if via_mqtt else "LoRa"
        if is_dm:
            msg_kind = "⚠️ Direct Message"
        elif channel != 0:
            msg_kind = f"Message from channel: {CHANNELS[channel]}"
        else:
            msg_kind = "Broadcast Message"

        name = display_name or node_id

        rssi_str = f"{rssi} dBm" if rssi is not None else "?"
        snr_str = f"{snr} dB" if snr is not None else "?"

        body = (
            f"<b>{msg_kind} ({transport})</b>\n"
            f"From: {name} ({node_id})\n"
            f"Text: {text}"
        )

        if transport == "LoRa":
            body += f"\n<i>RSSI: {rssi_str}, SNR: {snr_str}</i>"

        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": body,
            "parse_mode": "HTML"
        }

        resp = requests.post(url, json=payload, timeout=10)
        if not resp.ok:
            logger.warning("Telegram send failed: %s %s", resp.status_code, resp.text)
        else:
            logger.info(
                "Sent %s to Telegram chat %s from %s (%s)",
                msg_kind,
                chat_id,
                node_id,
                name,
            )

    except Exception as e:
        logger.exception("Error while sending Telegram message: %s", e)


def poll_telegram_and_forward_to_meshtastic(interface: TCPInterface):
    """
    Read from Telegram and send to Meshtastic.
    Limit by ALLOWED_TELEGRAM_USER_ID.
    """
    global LAST_UPDATE_ID, TELEGRAM_TOKEN, ALLOWED_TELEGRAM_USER_ID

    if not TELEGRAM_TOKEN:
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates"
    params = {
        "offset": LAST_UPDATE_ID + 1,
        "timeout": 0,
        "allowed_updates": ["message"],
    }

    try:
        resp = requests.get(url, params=params, timeout=5)
        if not resp.ok:
            logger.warning("getUpdates failed: %s %s", resp.status_code, resp.text)
            return

        data = resp.json()
        updates = data.get("result", [])
        if not updates:
            return

        for upd in updates:
            LAST_UPDATE_ID = upd["update_id"]

            msg = upd.get("message")
            if not msg:
                continue

            from_user = msg.get("from") or {}
            user_id = from_user.get("id")
            username = from_user.get("username")

            # Ограничение по пользователю
            if ALLOWED_TELEGRAM_USER_ID and user_id != ALLOWED_TELEGRAM_USER_ID:
                logger.warning(
                    "⛔️ Unauthorized Telegram user %s (%s) tried to send: %s",
                    user_id,
                    username,
                    msg.get("text"),
                )
                continue

            text = msg.get("text")
            if not text:
                continue

            logger.info(
                "💥 Telegram -> Meshtastic: from %s (%s): %s",
                user_id,
                username,
                text,
            )

            # Простейший вариант — широковещательный текст в mesh
            try:
                interface.sendText(text)
            except Exception as e:
                logger.exception("Failed to send Telegram message to Meshtastic: %s", e)

    except Exception as e:
        logger.warning("Error in poll_telegram_and_forward_to_meshtastic: %s", e)

def on_receive(packet, interface):
    logger.debug("Received packet: %s", packet)

    try:
        from_id = packet.get("fromId")
        if not from_id:
            from_num = packet.get("from")
            if from_num is not None:
                from_id = f"!{from_num:08x}"
            else:
                from_id = "unknown"

        decoded = packet.get("decoded", {}) or {}
        portnum = decoded.get("portnum")
        channel = packet.get("channel", 0)
        via_mqtt = packet.get("viaMqtt", False)
        rssi = packet.get("rxRssi")
        snr = packet.get("rxSnr")
        sender = packet.get("fromId")
        ch = get_channel(packet, 0)

        node_data = {
            "node_id": from_id,
            "last_seen": time.time(),
            "long_name": None,
            "short_name": None,
            "hardware": None,
            "role": None,
            "position_lat": None,
            "position_lon": None,
            "position_alt": None,
            "position_time": None,
            "battery_level": None,
            "voltage": None,
            "channel_utilization": None,
            "air_util_tx": None,
        }

        # User info
        if "user" in decoded:

            user = decoded["user"]
            node_data.update({
                "long_name": user.get("longName"),
                "short_name": user.get("shortName"),
                "hardware": user.get("hwModel"),
                "role": user.get("role"),
            })
            logger.info(
                "📟 Node info found %s: %s %s %s",
                node_data["long_name"],
                node_data["short_name"],
                node_data["hardware"],
                node_data["role"],
            )

        # Position
        if portnum in ("POSITION_APP", portnums_pb2.POSITION_APP):
            try:
                pos = mesh_pb2.Position()
                pos.ParseFromString(decoded.get("payload", b""))

                node_data["position_lat"] = pos.latitude_i * 1e-7 if pos.latitude_i != 0 else None
                node_data["position_lon"] = pos.longitude_i * 1e-7 if pos.longitude_i != 0 else None
                node_data["position_alt"] = pos.altitude if pos.altitude != 0 else None
                node_data["position_time"] = pos.time if pos.time != 0 else None

                logger.info(
                    "📌 Position from %s: lat=%s lon=%s alt=%s time=%s",
                    from_id,
                    node_data["position_lat"],
                    node_data["position_lon"],
                    node_data["position_alt"],
                    node_data["position_time"],
                )
            except Exception as e:
                logger.warning("[Position] Error parsing position from %s: %s", from_id, e)

        # Telemetry
        logger.debug("Portnum: %s", portnum)
        if portnum in ("TELEMETRY_APP", portnums_pb2.TELEMETRY_APP):
            logger.info("🌡️ Found Telemetry from %s", from_id)
            try:
                env = telemetry_pb2.Telemetry()
                env.ParseFromString(decoded.get("payload", b""))

                if env.HasField("device_metrics"):
                    dm = env.device_metrics
                    node_data["battery_level"] = dm.battery_level if dm.HasField("battery_level") else None
                    node_data["voltage"] = dm.voltage if dm.HasField("voltage") else None
                    node_data["channel_utilization"] = dm.channel_utilization if dm.HasField("channel_utilization") else None
                    node_data["air_util_tx"] = dm.air_util_tx if dm.HasField("air_util_tx") else None

                    logger.info(
                        "Telemetry from %s: battery=%s voltage=%s ch_util=%s air_util_tx=%s",
                        from_id,
                        node_data["battery_level"],
                        node_data["voltage"],
                        node_data["channel_utilization"],
                        node_data["air_util_tx"],
                    )
            except Exception as e:
                logger.warning("[Telemetry] Error parsing telemetry from %s: %s", from_id, e)

        with sqlite3.connect(DB_NAME, timeout=10) as conn:
            c = conn.cursor()

            # Upsert node.
            logging.info("Update node %s", node_data)
            # Сначала ensure node exists (insert if missing)
            c.execute("""
                INSERT OR IGNORE INTO nodes (node_id, last_seen)
                VALUES (?, ?)
            """, (node_data["node_id"], node_data["last_seen"]))

            # Теперь строим UPDATE только по тем полям, что не None
            update_fields = []
            update_values = []

            for key, value in node_data.items():
                if key in ("node_id",):
                    continue
                if value is not None:     # <---- вот ключевое
                    update_fields.append(f"{key} = ?")
                    update_values.append(value)

            # last_seen всегда обновляем
            update_fields.append("last_seen = ?")
            update_values.append(time.time())

            # WHERE node_id = ...
            update_values.append(node_data["node_id"])

            sql = f' UPDATE nodes SET {", ".join(update_fields)} WHERE node_id = ?'

            logger.info("SQL update: %s  %s", sql, update_values)
            c.execute(sql, update_values)
            conn.commit()
            # Разрешаем node_id → long_name
            display_name = node_data.get("long_name")
            if not display_name:
                display_name = resolve_long_name_from_db(conn, from_id)
            # Определяем, DM или нет
            is_dm = packet.get("pkiEncrypted", False)
            logger.debug(f"Received message {packet} and {is_dm}")

            # Текстовые сообщения (в Meshtastic DM тоже идут через TEXT_MESSAGE_APP)
            is_text = portnum in (
                "TEXT_MESSAGE_APP",
                portnums_pb2.TEXT_MESSAGE_APP,
            )

            if is_text:
                text = decoded.get("text")
                if text is None:
                    payload = decoded.get("payload", b"")
                    try:
                        text = payload.decode("utf-8", errors="replace")
                    except Exception:
                        text = ""

                if is_dm:
                    msg_type = "⚠️ Direct Message"
                elif channel != 0:
                    msg_type = f"Message from channel: {CHANNELS[channel]}"
                else:
                    msg_type = "Broadcast Message"

                logger.info(
                    "%s from %s (%s) via %s: %s",
                    msg_type,
                    from_id,
                    display_name or "unknown",
                    "MQTT" if via_mqtt else "LoRa",
                    text,
                )

                # Сохраняем в messages (порт всегда TEXT_MESSAGE_APP)
                c.execute("""
                    INSERT INTO messages (node_id, text, portnum, timestamp)
                    VALUES (?, ?, ?, ?)
                """, (
                    from_id,
                    text,
                    int(portnums_pb2.TEXT_MESSAGE_APP),
                    time.time()
                ))

                # Отправляем в Telegram
                send_telegram_message(
                    text=text,
                    via_mqtt=via_mqtt,
                    node_id=from_id,
                    display_name=display_name,
                    rssi=rssi,
                    snr=snr,
                    is_dm=is_dm,
                    channel=channel
                )
                # Реакции
                if "hi" in text.lower():
                    send_response(interface, "🤖 Дратути", reply_to_id=packet["id"], channel=ch, reacton=True)
                elif "пинг" in text.lower():
                    send_response(interface, "🤖 Сам ты пинг!!!", reply_to_id=packet["id"], channel=ch, reacton=True)
                elif "!llm " in text.lower():
                    text = text[len("!llm "):].lstrip()
                    raw = ollama_reply(text)
                    reply = clamp_200(raw)
                    logger.info("🤖 AI replay is :'%s'", reply)
                    send_response(interface, "🤖" + reply, reply_to_id=packet["id"], channel=ch, reacton=False)
                    #interface.sendText(reply, channelIndex=0)



    except Exception as e:
        logger.exception("Packet processing error: %s", e)


def main():
    global TELEGRAM_TOKEN, TELEGRAM_CHAT_ID, TELEGRAM_DM_CHAT_ID, ALLOWED_TELEGRAM_USER_ID

    parser = argparse.ArgumentParser(
        description="Log Meshtastic TCP traffic to SQLite and forward text/DM to Telegram"
    )
    parser.add_argument("host", help="TCP server IP or hostname")
    parser.add_argument(
        "-p", "--port",
        type=int,
        default=4403,
        help="TCP port (default: 4403) — сейчас не используется, TCPInterface берёт host"
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL). Default: INFO",
    )
    parser.add_argument(
        "--telegram-token",
        help="Telegram bot token (or use TELEGRAM_BOT_TOKEN env)",
    )
    parser.add_argument(
        "--telegram-chat-id",
        help="Telegram chat id for common messages (or use TELEGRAM_CHAT_ID env)",
    )
    parser.add_argument(
        "--telegram-dm-chat-id",
        help="Telegram chat id for Direct Messages (or use TELEGRAM_DM_CHAT_ID env)",
    )
    parser.add_argument(
        "--allowed-telegram-user-id",
        type=int,
        help="Only this Telegram user id may send commands to bot (or TELEGRAM_ALLOWED_USER_ID env)",
    )

    args = parser.parse_args()

    # Logging setup
    setup_logging(args.log_level, logfile="meshtastic.log")
    def _thread_excepthook(args):
        # args: threading.ExceptHookArgs(exc_type, exc_value, exc_traceback, thread)
        if isinstance(args.exc_value, BrokenPipeError):
            logger.error(
                "BrokenPipeError in thread %s, exiting so systemd can restart the service",
                args.thread.name,
            )
            # жёсткий выход без очистки, чтобы точно не зависнуть
            os._exit(1)
        else:
            # дефолтное поведение — просто напечатать трейс
            sys.__excepthook__(args.exc_type, args.exc_value, args.exc_traceback)

    threading.excepthook = _thread_excepthook
    # Telegram config
    TELEGRAM_TOKEN = args.telegram_token or os.environ.get("TELEGRAM_BOT_TOKEN")
    TELEGRAM_CHAT_ID = args.telegram_chat_id or os.environ.get("TELEGRAM_CHAT_ID")
    TELEGRAM_DM_CHAT_ID = args.telegram_dm_chat_id or os.environ.get("TELEGRAM_DM_CHAT_ID")

    allowed_env = os.environ.get("TELEGRAM_ALLOWED_USER_ID")
    ALLOWED_TELEGRAM_USER_ID = args.allowed_telegram_user_id or (int(allowed_env) if allowed_env else None)


    if TELEGRAM_TOKEN:
        if TELEGRAM_CHAT_ID:
            logger.info("Telegram forwarding enabled (common chat: %s)", TELEGRAM_CHAT_ID)
        else:
            logger.info("Telegram token set, but TELEGRAM_CHAT_ID not configured")
        if TELEGRAM_DM_CHAT_ID:
            logger.info("Telegram DM chat enabled: %s", TELEGRAM_DM_CHAT_ID)
    else:
        logger.info("Telegram forwarding disabled (no token)")

    create_database()

    connection_string = f"{args.host}"
    logger.info("Connecting to Meshtastic TCP interface at %s", connection_string)

    pub.subscribe(on_receive, "meshtastic.receive")

    interface = None
    try:
        # Если захочешь использовать порт — поменяй на TCPInterface(args.host, args.port)
        interface = TCPInterface(connection_string)
        logger.info("Connected. Listening for packets... (Ctrl+C to exit)")

        while True:
            poll_telegram_and_forward_to_meshtastic(interface)
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down on KeyboardInterrupt")
    except Exception as e:
        logger.exception("Error in main loop: %s", e)
    finally:
        if interface is not None:
            try:
                interface.close()
            except Exception:
                pass


if __name__ == "__main__":
    main()

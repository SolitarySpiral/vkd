import asyncio
import aiohttp
import logging
import re
import os
import subprocess
from pathlib import Path
from urllib.parse import urljoin, urlencode
from Crypto.Cipher import AES
from concurrent.futures import ProcessPoolExecutor

# --- Настройка ---
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] - %(levelname)s - %(message)s'
)
logger = logging.getLogger("vkd_audio")

# Количество одновременных загрузчиков M3U8
DOWNLOADER_CONSUMERS = 5 
# Количество одновременных конвертеров FFMPEG (рекомендуется os.cpu_count())
FFMPEG_WORKERS = os.cpu_count() or 1 


def run_ffmpeg_task(ts_filepath: Path) -> str | None:
    """
    Синхронная функция для запуска ffmpeg в отдельном процессе.
    Получает на вход .ts файл, создает файл mp3 и удаляет .ts файл.
    """
    mp3_filepath = ts_filepath.with_suffix(".mp3")
    
    # Используем -c:a copy для перепаковки без потерь, если это возможно.
    # Для максимальной совместимости можно использовать '-c:a libmp3lame -b:a 192k' для перекодирования.
    ffmpeg_command = [
        'ffmpeg', '-y',
        '-i', str(ts_filepath),
        '-c:a', 'copy', # Простое копирование аудиопотока
        '-vn', 
        str(mp3_filepath)
    ]
    
    logger.info(f"[FFMPEG] Начало конвертации: {ts_filepath.name} -> {mp3_filepath.name}")
    try:
        process = subprocess.run(
            ffmpeg_command,
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='ignore',
            check=True # Вызовет исключение, если ffmpeg вернет ненулевой код
        )
        if process.returncode == 0:
            logger.info(f"[FFMPEG] Успешно сконвертирован файл: {mp3_filepath.name}")
            #ts_filepath.unlink() # Удаляем временный .ts файл
            return str(mp3_filepath)
    except FileNotFoundError:
        logger.error("[FFMPEG] Ошибка: ffmpeg не найден. Убедитесь, что он установлен и доступен в системном PATH.")
        return None
    except subprocess.CalledProcessError as e:
        logger.error(f"[FFMPEG] Ошибка при конвертации {ts_filepath.name}:")
        logger.error(f"Команда: {' '.join(e.cmd)}")
        logger.error(f"Stderr: {e.stderr}")
        return None
    return None


class Audio:
    def __init__(self, token, owner_id, download_dir=None):
        self.token = token
        self.owner_id = owner_id
        self.download_dir = Path(download_dir or 'D://ghd/аудио')
        self.download_dir.mkdir(parents=True, exist_ok=True)

    async def download_binary(self, session: aiohttp.ClientSession, url: str) -> bytes | None:
        try:
            async with session.get(url) as response:
                response.raise_for_status()
                return await response.read()
        except aiohttp.ClientError as e:
            logger.error(f"Ошибка при скачивании {url}: {e}")
            return None

    def decrypt_segment(self, encrypted_data: bytes, key: bytes, iv: bytes) -> bytes:
        cipher = AES.new(key, AES.MODE_CBC, iv)
        return cipher.decrypt(encrypted_data)

    async def downloader_logic(self, session: aiohttp.ClientSession, download_queue: asyncio.Queue, conversion_queue: asyncio.Queue) -> None:
        """Этап 1: Загрузчик. Скачивает и собирает .ts файл, затем передает его в очередь на конвертацию."""
        while True:
            try:
                m3u8_url, ts_filename = await download_queue.get()
                
                logger.info(f"[ЗАГРУЗЧИК] Начал обработку: {ts_filename}")
                # ... (вся логика парсинга, скачивания и сборки .ts файла) ...
                base_url = urljoin(m3u8_url, ".")
                playlist_content_bytes = await self.download_binary(session, m3u8_url)
                if not playlist_content_bytes:
                    await asyncio.sleep(0.5)
                    #download_queue.task_done()
                    continue

                output_ts_path = self.download_dir.joinpath(ts_filename).resolve()
                if output_ts_path.exists():
                    logger.info(f"[ЗАГРУЗЧИК] Файл уже существует, пропуск загрузки, передаем в конвертер: {output_ts_path.name}")
                    # Передаем на следующий этап конвейера
                    await conversion_queue.put(output_ts_path)
                    continue

                playlist_content = playlist_content_bytes.decode('utf-8')
                segments_to_process = []
                unique_key_urls = set()
                current_key_uri = None
                media_sequence = 0

                for line in playlist_content.splitlines():
                    line = line.strip()
                    if not line: continue
                    if line.startswith("#EXT-X-MEDIA-SEQUENCE"):
                        media_sequence = int(line.split(':', 1)[1])
                    elif line.startswith("#EXT-X-KEY"):
                        params = {m.group(1): m.group(2).strip('"') for m in re.finditer(r'([A-Z-]+)=(".*?"|[^,]+)', line.split(':', 1)[1])}
                        if params.get("METHOD") == "AES-128":
                            current_key_uri = urljoin(base_url, params.get("URI", ""))
                            if current_key_uri: unique_key_urls.add(current_key_uri)
                        elif params.get("METHOD") == "NONE":
                            current_key_uri = None
                    elif not line.startswith("#"):
                        segments_to_process.append({"url": urljoin(base_url, line), "key_uri": current_key_uri, "sequence": media_sequence})
                        media_sequence += 1
                
                key_tasks = {url: asyncio.create_task(self.download_binary(session, url)) for url in unique_key_urls}
                segment_tasks = [asyncio.create_task(self.download_binary(session, seg["url"])) for seg in segments_to_process]
                
                downloaded_keys = {url: await task for url, task in key_tasks.items()}
                downloaded_segments_data = await asyncio.gather(*segment_tasks)
                
                
                
                with open(output_ts_path, "wb") as f_out:
                    for i, seg_info in enumerate(segments_to_process):
                        enc_data = downloaded_segments_data[i]
                        if not enc_data: continue
                        if seg_info["key_uri"]:
                            key = downloaded_keys.get(seg_info["key_uri"])
                            if not key: continue
                            iv = seg_info["sequence"].to_bytes(16, 'big')
                            f_out.write(self.decrypt_segment(enc_data, key, iv))
                        else:
                            f_out.write(enc_data)
                
                logger.info(f"[ЗАГРУЗЧИК] .ts файл собран: {output_ts_path.name}")
                # Передаем на следующий этап конвейера
                await conversion_queue.put(output_ts_path)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[ЗАГРУЗЧИК] Критическая ошибка: {e}", exc_info=True)
            finally:
                download_queue.task_done()
                if download_queue.empty():
                    asyncio.sleep(10)

    async def vk_audio_producer(self, session: aiohttp.ClientSession, download_queue: asyncio.Queue):
        """Продюсер: получает список треков и кладет задания в очередь загрузки."""
        offset = 0
        while True:
            api_url = self.build_api_url("audio.get", 100, offset)
            try:
                async with session.get(api_url) as response:
                    response.raise_for_status()
                    data = await response.json()
                    if "response" not in data:
                        logger.error(f"Ошибка API VK: {data.get('error', {}).get('error_msg', 'Нет поля response')}")
                        break

                    items = data.get("response", {}).get("items", [])
                    logger.info(f"Получено {len(items)} аудиозаписей (offset={offset})")
                    if not items:
                        logger.info("Все аудиозаписи получены.")
                        break

                    for item in items:
                        if item.get("url"):
                            artist = re.sub(r'[\\/*?:"<>|]', '_', item.get('artist', 'Unknown Artist'))
                            title = re.sub(r'[\\/*?:"<>|]', '_', item.get('title', 'Unknown Title'))
                            ts_filename = f"{artist} - {title}.ts"
                            await download_queue.put((item["url"], ts_filename))
                        else:
                            logger.warning(f"Сломанный item: {item}")
                            logger.warning(f"Пропуск трека без URL: {item.get('artist')} - {item.get('title')}")
                    
                    offset += 100
                    await asyncio.sleep(0.3)
            except Exception as e:
                logger.error(f"Ошибка при получении списка аудио: {e}", exc_info=True)
                break

    def build_api_url(self, method, count, offset) -> str:
        params = {"access_token": self.token, "owner_id": self.owner_id[0] if isinstance(self.owner_id, list) else self.owner_id, "count": count, "offset": offset, "v": "5.199"}
        return f"https://api.vk.com/method/{method}?{urlencode(params)}"

    async def main(self):
        download_queue = asyncio.Queue()
        conversion_queue = asyncio.Queue()
        
        # Создаем пул процессов для задач, нагружающих CPU
        loop = asyncio.get_running_loop()
        with ProcessPoolExecutor(max_workers=FFMPEG_WORKERS) as executor:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=None)) as session:
                # Запускаем потребителей-загрузчиков
                downloader_tasks = [
                    asyncio.create_task(self.downloader_logic(session, download_queue, conversion_queue))
                    for _ in range(DOWNLOADER_CONSUMERS)
                ]
                
                # Запускаем продюсера
                producer_task = asyncio.create_task(self.vk_audio_producer(session, download_queue))
                await producer_task
                
                # Ждем, пока все .ts файлы будут скачаны и добавлены в очередь конвертации
                await download_queue.join()

                # Сигнал конвертерам, что больше файлов не будет
                for _ in range(FFMPEG_WORKERS):
                    await conversion_queue.put(None)

                # Запускаем задачи-обертки для CPU-воркеров
                converter_futures = []
                while True:
                    ts_path = await conversion_queue.get()
                    if ts_path is None:
                        conversion_queue.task_done()
                        break
                    
                    # Отправляем задачу в пул процессов, не блокируя основной поток
                    future = loop.run_in_executor(executor, run_ffmpeg_task, ts_path)
                    converter_futures.append(future)
                    conversion_queue.task_done()

                # Ожидаем завершения всех задач конвертации
                if converter_futures:
                    await asyncio.gather(*converter_futures)

                # Отменяем задачи-загрузчики
                for task in downloader_tasks:
                    task.cancel()
                await asyncio.gather(*downloader_tasks, return_exceptions=True)

if __name__ == "__main__":
    try:
        token = ""
        vk_audio = Audio(token=token, owner_id=347458752, download_dir=Path.cwd().joinpath("audio_downloads"))
        asyncio.run(vk_audio.main())
    except KeyboardInterrupt:
        logger.info("Процесс прерван пользователем.")
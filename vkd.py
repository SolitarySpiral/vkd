import os
import re
import math
import time
import yaml
import vk_api
import yt_dlp
import asyncio
import aiohttp
import aiofiles
from tqdm.asyncio import tqdm
from pytils import numeral
from datetime import datetime
from pathlib import Path

import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] - %(name)s-%(filename)s:%(funcName)s():%(lineno)d - %(levelname)s - %(message)s'
)

logger = logging.getLogger("vkd")

APP_DIR = Path(__file__).resolve().parent
CONFIG_PATH = APP_DIR.joinpath("config.yaml")
BASE_DIR = Path("D:/ghd") / "Фотки"

def load_config() -> dict:
    if CONFIG_PATH.exists():
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    return {}

def save_config(data: dict) -> None:
    with open(CONFIG_PATH, "w", encoding="utf-8") as f:
        yaml.safe_dump(data, f)

def load_token_from_config() -> str:
    logger.info("Загружаю токен из конфига")
    return load_config().get("token", "")

def save_token_to_config(token: str) -> None:
    cfg = load_config()
    cfg["token"] = token
    save_config(cfg)

def safe_filename(name):
    return re.sub(r'[\\/*?:"<>|]', '_', name)

async def download_photo(session: aiohttp.ClientSession, photo_url: str, photo_path: Path):
    try:
        if not photo_path.exists():
            async with session.get(photo_url) as response:
                if response.status == 200:
                    async with aiofiles.open(photo_path, "wb") as f:
                        await f.write(await response.read())
    except Exception as e:
        logger.error(e)

async def download_photos(photos_path: Path, photos: list):
    logger.info("{} {} {}".format(
        numeral.choose_plural(len(photos), "Будет, Будут, Будут"),
        numeral.choose_plural(len(photos), "скачена, скачены, скачены"),
        numeral.get_plural(len(photos), "фотография, фотографии, фотографий")
    ))
    #print(photos)
    time_start = time.time()

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=None)) as session:
        futures = []
        for i, photo in enumerate(photos, start=1):
            if photo.get("album_title"):
                logger.debug(f"у нас есть тайтл для фото {photo.get("album_title")}")
                album_dir = (photos_path / safe_filename(photo["album_title"])).resolve()
                app.utils.create_dir(album_dir)
                logger.debug(f"Создана директория {album_dir}")
                photo_title = f"{photo['date']}_{photo['owner_id']}_{photo['id']}.jpg"
                full_path = (album_dir / photo_title).resolve()
            else:
                logger.debug(f"ветка иначе")
                full_path = (photos_path / f"{photo['date']}_{photo['owner_id']}_{photo['id']}.jpg").resolve()
                logger.debug(f"ветка путь {full_path}")

            if full_path.exists():
                logger.info(f"Пропущено (уже существует): {full_path.name}")
                continue
            futures.append(download_photo(session, photo["url"], full_path))

        for future in tqdm(asyncio.as_completed(futures), total=len(futures)):
            try:
                await future
            except Exception as e:
                logger.error('Got an exception: %s' % e)

    time_finish = time.time()
    download_time = math.ceil(time_finish - time_start)
    logger.info("{} {} за {}".format(
        numeral.choose_plural(len(photos), "Скачена, Скачены, Скачены"),
        numeral.get_plural(len(photos), "фотография, фотографии, фотографий"),
        numeral.get_plural(download_time, "секунду, секунды, секунд")
    ))

async def download_video(video_path:Path, video_link):
    ydl_opts = {'outtmpl': '{}'.format(video_path), 'quiet': True, 'retries': 10, 'ignoreerrors': True, 'age_limit': 28}
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        ydl.download(video_link)
        logger.info("Видео загружено: %s" % video_path.name)

async def download_videos(videos_path: Path, videos: list):
    futures = []
    for i, video in enumerate(videos, start=1):
        filename = "{}_{}_{}.mp4".format(video["date"], video["owner_id"], video["id"])
        video_path = videos_path.joinpath(filename)
        if video_path.exists():
            logger.info(f"Пропущено (уже существует): {video_path.name}")
            continue
        futures.append(download_video(video_path, video["player"]))
    logger.info("We will download %s wideos" % len(futures))
    for future in tqdm(asyncio.as_completed(futures), total=len(futures)):
        try:
            await future
        except Exception as e:
            logger.error('Got an exception: %s' % e)

class Vkd:
    def __init__(self, vk_ids:str):
        logger.info("Vkd init — загружен логгер")
        token = load_token_from_config()
        logger.info(f"Vkd init — токен загружен: {token}")
        self.session = Vksesion(token)
        logger.info("Vkd init — сессия создана")
        self.vk = self.session.vk
        self.utils = Utils(self.vk)
        logger.info("Vkd init — utils создан")
        self.vk_ids = self.utils.vk_resolve_ids(vk_ids)
        logger.info(f"Vkd init — ids разрешены:{self.vk_ids} с типом {self.utils.ids_type}")
        self.groups = Groups(self.vk)
        logger.info("Vkd init — groups создан")
        self.wall = Wall(self.vk, self.groups)
        logger.info("Vkd init — Wall создан")
        self.photos = Photos(self.vk)
        logger.info("Vkd init — Photos создан")
        self.video = Video(self.vk)
        logger.info("Vkd init — Video создан")
        self.messages = Messages(self.vk)
        logger.info("Vkd init — Messages создан")
        #self.dir_name: Path = ''

    async def main(self):
        type = app.utils.ids_type
        if type == 'group' and self.utils.check_group_ids(self.vk_ids):
            for group in self.vk_ids:
                # получаем посты со стены (сохраняются в groups.photos)
                logger.info(f"Пытаемся получить фото стены")
                self.wall.vk_get_posts(group_id=group)
                logger.info(f"Пытаемся получить фото стены: получили {len(self.groups.photos)}")
                logger.info(f"Пытаемся получить все альбомы группы: {group}")
                items = self.photos.vk_getALL(group, 'group')
                logger.info(f"Пытаемся получить все альбомы группы: собрали фотографий {len(items)}")
                self.utils.extract_from_raw_data(type='photos', raw_data=items, owner_id=group)
                logger.info(f"Пытаемся получить все видео группы: {group}")
                items = self.video.vk_group_get(group)
                logger.info(f"Пытаемся получить все видео группы: собрали {len(items)}")
                self.utils.extract_from_raw_data(type='videos', raw_data=items, owner_id=group)
                # объединение фотографий из альбомов и фото из постов со стены
                logger.info(f"будем объединять фото со стены {len(self.groups.photos)} и из альбомов {len(self.utils.photos)}")
                self.utils.photos.extend(self.groups.photos)

                group_name = self.utils.get_group_title(group)
                group_dir = BASE_DIR.joinpath(group_name)
                self.utils.create_dir(group_dir)
                await download_photos(group_dir, self.utils.photos)
                await download_videos(group_dir, self.utils.videos)

        if type == 'user' and self.utils.check_user_ids(self.vk_ids):
            for user in self.vk_ids:
                items = self.photos.vk_user_get(user, 'saved')
                logger.info(f"Пытаемся получить фото: saved получили {len(items)}")
                self.utils.extract_from_raw_data(type='photos', raw_data=items, owner_id=user)

                items = self.photos.vk_user_get(user, 'profile')
                logger.info(f"Пытаемся получить фото: profile получили {len(items)}")
                self.utils.extract_from_raw_data(type='photos', raw_data=items, owner_id=user)

                items = self.photos.vk_user_get(user, 'wall')
                logger.info(f"Пытаемся получить фото: wall получили {len(items)}")
                self.utils.extract_from_raw_data(type='photos', raw_data=items, owner_id=user)

                items = self.photos.vk_getALL(user, 'user')
                logger.info(f"Пытаемся получить фото: getall получили {len(items)}")
                self.utils.extract_from_raw_data(type='photos', raw_data=items, owner_id=user)

                username = self.utils.get_username(user)
                photos_path = BASE_DIR.joinpath(username)
                self.utils.create_dir(photos_path)
                await download_photos(photos_path, self.utils.photos)

        if type == 'chat':
            for chat in self.vk_ids:
                if self.utils.check_chat_id(chat) and chat>0:
                    logger.info("Ветка чат с пользователем")
                    items = self.messages.vk_getHistoryAttachments(chat)
                    logger.info(f"Пытаемся получить фото из переписки: получили {len(items)}")
                    self.utils.extract_from_raw_data(type='chat', raw_data=items, owner_id=chat)
                    logger.info(f'Обработаны items {len(items)}, в photos лежит {len(self.utils.photos)}')
                    logger.info(f"Получаем название чата по id {chat}")
                    username = self.utils.get_username(chat)
                    logger.info("Получили название переписки username")
                    photos_path = BASE_DIR.joinpath(f"Переписка {username}")
                    self.utils.create_dir(photos_path)
                    await download_photos(photos_path, self.utils.photos)
                elif self.utils.check_chat_id(chat) and chat<0:
                    logger.info("Ветка чат с группой")
                    #chat = 2_000_000_000+chat
                    items = self.messages.vk_getHistoryAttachments(chat)
                    logger.info(f"Пытаемся получить фото из переписки: получили {len(items)}")
                    self.utils.extract_from_raw_data(type='chat', raw_data=items, owner_id=chat)
                    logger.info(f'Обработаны items {len(items)}, в photos лежит {len(self.utils.photos)}')
                    logger.info(f"Получаем название чата по id {chat}")
                    username = self.utils.get_chat_title(chat)
                    logger.info("Получили название переписки username")
                    photos_path = BASE_DIR.joinpath(f"Переписка {username}")
                    self.utils.create_dir(photos_path)
                    await download_photos(photos_path, self.utils.photos)
                else:
                    logger.error(f"Не смогли определить чат {chat}")


class Vksesion:
    def __init__(self, token):
        vk_session = vk_api.VkApi(token=token)
        self.vk = vk_session.get_api()
        logger.info("Успешно авторизовались")
       
class Utils:
    def __init__(self, vk):
        self.vk = vk
        self.photos = []
        self.videos = []
        self.ids_type:str

    def vk_resolve_ids(self, input_str):
        ids = input_str.split(",")
        result = []

        chat_pattern = re.compile(r"vk\.com/im/convo/(-?\d+)")
        screen_name_pattern = re.compile(r"vk\.com/([\w\d_.]+)")

        for item in ids:
            item = item.strip()
            if not item:
                continue

            # Если это ссылка на чат
            chat_match = chat_pattern.search(item)
            if chat_match:
                chat_id = int(chat_match.group(1))
                self.ids_type = 'chat'
                result.append(chat_id)
                continue

            # Если это обычная ссылка вида vk.com/username
            screen_name_match = screen_name_pattern.search(item)
            if screen_name_match:
                item = screen_name_match.group(1)

            # Если в списке только id из цифр
            if item.isdigit():
                if self.check_chat_id(item):
                    self.ids_type = 'chat'
                if self.check_group_id(item):
                    self.ids_type = 'group'
                if self.check_user_id(item):
                    self.ids_type = 'user'
                result.append(int(item))
                continue

            # Иначе resolve через API
            try:
                resolved = self.vk.utils.resolveScreenName(screen_name=item)
                if resolved and resolved.get("object_id"):
                    object_id = resolved["object_id"]
                    type_ = resolved["type"]
                    if type_ == "group":
                        object_id = -object_id
                    self.ids_type = type_
                    result.append(object_id)
                else:
                    raise ValueError(f"Не удалось разрешить '{item}'")
            except Exception as e:
                raise ValueError(f"Ошибка при разрешении '{item}': {e}")

        return result
    
    def extract_from_raw_data(self, type, raw_data, owner_id):
        if type == 'photos':
            logger.info(f"Пробуем достать фото из items c типом {type}")
            albums_dict = app.photos.vk_getAlbums(owner_id)
            for photo in raw_data:
                album_id = photo.get("album_id")
                album_title = albums_dict.get(album_id, "Без альбома")
                
                self.photos.append({
                    "id": photo.get("id"),
                    "owner_id": photo.get("owner_id"),
                    "url": photo.get("sizes", [{}])[-1].get("url") if photo.get("sizes") else None,
                    "date": datetime.fromtimestamp(int(photo.get("date"))).strftime('%Y-%m-%d %H-%M-%S'),
                    "album_id": album_id,
                    "album_title": album_title
                })
        elif type == 'videos':
            logger.info(f"Пробуем достать видео из items c типом {type}")
            for video in raw_data:
                if "player" in video:
                    self.videos.append({
                        "type": video.get("type"),
                        "id": video.get("id"),
                        "owner_id": video.get("owner_id"),
                        "title": video.get("title"),
                        "player": video.get("player"),
                        "date": datetime.fromtimestamp(int(video.get("date"))).strftime('%Y-%m-%d %H-%M-%S')
                    })
        elif type == 'chat':
            logger.info(f"Пробуем достать фото из items c типом {type}")
            for photo in raw_data:
                attachment = photo.get("attachment", {})
                #logger.info(attachment)
                photo_data = attachment.get("photo", {})
                #logger.info(photo_data)
                sizes = photo_data.get("sizes", [])
                logger.debug(f'"id": {photo_data.get("id")},\n"owner_id": {photo_data.get("owner_id")},\n"url": {sizes[-1].get("url") if sizes and isinstance(sizes[-1], dict) else None},\n"date": {datetime.fromtimestamp(int(photo_data.get("date", 0))).strftime('%Y-%m-%d %H-%M-%S')}')
                self.photos.append({
                    "id": photo_data.get("id"),
                    "owner_id": photo_data.get("owner_id"),
                    "url": sizes[-1].get("url") if sizes and isinstance(sizes[-1], dict) else None,
                    "date": datetime.fromtimestamp(int(photo_data.get("date", 0))).strftime('%Y-%m-%d %H-%M-%S')
                })

    def check_user_id(self, id: str) -> bool:
        try:
            user = self.vk.users.get(user_ids=id)
            if len(user) != 0:
                logger.info(f"Пользователь существует {user}") 
                return True
            logger.info(f"Пользователь не существует {user}") 
            return False
        except Exception as e:
            logger.error(e)
            return False

    def check_user_ids(self, ids_list) -> bool:
        try:
            logger.info(f"Проверяем, существует ли пользователи с таким id: {ids_list}")
            for user_id in ids_list: #.split(","):
                logger.info(f"передаем на проверку id {user_id}")
                if not self.check_user_id(user_id):
                    return False
            return True
        except Exception as e:
            logger.error(e)
            return False

    def check_group_id(self, id: str) -> bool:
        try:
            # Проверяем, существует ли группа с таким id
            group = self.vk.groups.getById(group_id=int(id)*(-1))
            if len(group) != 0: return True
            return False
        except Exception as e:
            logger.error(e)
            return False

    def check_group_ids(self, ids_list) -> bool:
        try:
            logger.info(f"Проверяем, существует ли группы с таким id: {ids_list}")
            for group_id in ids_list:#.split(","):
                logger.info(f"передаем на проверку id {group_id}")
                if not self.check_group_id(group_id):
                    return False
            return True
        except Exception as e:
            logger.error(e)
            return False

    def check_chat_id(self, id: str) -> bool:
        try:
            logger.info(f"Проверяем, существует ли беседа с таким id: [{id}]")
            # Проверяем, существует ли беседа с таким id 2_000_000_000 + 
            conversation = self.vk.messages.getConversationsById(peer_ids=id)
            if conversation["count"] != 0: return True
            return False
        except Exception as e:
            logger.error(e)
            return False

    def get_user_id(self):
        return self.vk.account.getProfileInfo()["id"]

    def get_username(self, user_id: str):
        user = self.vk.users.get(user_id=user_id)[0]
        return f"{user['first_name']} {user['last_name']}"

    def get_group_title(self, group_id: str):
        group_info = self.vk.groups.getById(group_id=int(group_id)*(-1))
        group_name = group_info[0]["name"].replace("/", " ").replace("|", " ").replace(".", " ").strip()
        return group_name

    def get_chat_title(self, chat_id: str) -> str:
        try:
            response = self.vk.messages.getConversationsById(
                peer_ids=chat_id,
                extended = True
            )
            chat_title = response["groups"][0]["name"]
        except Exception as e:
            raise e
        return chat_title
    
    def create_dir(self, dir_path: Path):
        if not dir_path.exists():
            dir_path.mkdir()
    
class Groups:
    def __init__(self, vk):
        self.vk = vk
        self.photos = []

    def get_single_post(self, post: dict):
        """Проходимся по всем вложениям поста и отбираем только картинки"""
        try:
            for i, attachment in enumerate(post["attachments"]):
                if attachment["type"] == "photo":
                    file_type = attachment["type"]
                    photo_id = post["attachments"][i]["photo"]["id"]
                    owner_id = post["attachments"][i]["photo"]["owner_id"]
                    photo_url = post["attachments"][i]["photo"]["sizes"][-1].get("url")
                    if photo_url != None or photo_url != '':
                        self.photos.append({
                            "type": file_type,
                            "id": photo_id,
                            "owner_id": -owner_id,
                            "url": photo_url,
                            "date": datetime.fromtimestamp(int(post["attachments"][i]["photo"]["date"])).strftime('%Y-%m-%d %H-%M-%S')
                        })
        except Exception as e:
            raise(e)
            
class Wall:
    def __init__(self, vk, groups, group_id=None):
        self.vk = vk
        self.groups = groups
        self.group_id = group_id

    def vk_get_posts(self, group_id):
        offset = 0
        while True:
            posts = self.vk.wall.get(
                owner_id=group_id,
                count=100,
                offset=offset
            )["items"]
            for post in posts:

                # Пропускаем посты с рекламой
                if post["marked_as_ads"]:
                    continue

                # Если пост скопирован с другой группы
                if "copy_history" in post:
                    if "attachments" in post["copy_history"][0]:
                        self.groups.get_single_post(post["copy_history"][0])

                elif "attachments" in post:
                    self.groups.get_single_post(post)

            logger.info(f"Собрали со стены фотографий: {len(app.groups.photos)}")
            if len(posts) < 100:
                break
            offset += 100

class Photos:
    def __init__(self, vk):
        self.vk = vk

    def vk_getALL(self, owner_id, type) -> dict:
        offset = 0
        all_photos = []
        # if type == 'user':
        #     owner_id = id
        # elif type == 'group':
        #     owner_id = -id
        while True:
            temp = self.vk.photos.getAll(
                owner_id=owner_id,
                extended=True,
                count=100,
                offset=offset
            )["items"]

            all_photos.extend(temp)

            if len(temp) < 100:
                break
            offset += 100
        return all_photos
    
    def vk_user_get(self, user_id, album:str) -> dict:
        offset = 0
        all_photos = []
        while True:
            # Собираем фото с альбома
            temp = self.vk.photos.get(
                user_id=user_id,
                count=100,
                offset=offset,
                album_id=album,
                photo_sizes=True,
                extended=True
            )["items"]

            all_photos.extend(temp)

            if len(temp) < 100:
                break
            offset += 100
        return all_photos

    def vk_getAlbums(self, owner_id) -> dict[int, str]:
        try:
            response = self.vk.photos.getAlbums(owner_id=owner_id, need_system=True)
            albums = response.get("items", [])
            return {album["id"]: album["title"] for album in albums}
        except Exception as e:
            logger.error(f"Ошибка при получении альбомов: {e}")
            return {}


class Video:
    def __init__(self, vk):
        self.vk = vk

    def vk_group_get(self, group_id) -> dict:
        offset = 0
        all_videos = []
        while True:
            temp = self.vk.video.get(
                owner_id=group_id,
                count=100,
                offset=offset
            )["items"]

            all_videos.extend(temp)

            if len(temp) < 100:
                break
            offset += 100
        return all_videos
    
class Messages:
    def __init__(self, vk):
        self.vk = vk

    def vk_getHistoryAttachments(self, chat_id):
        items = []
        response = self.vk.messages.getHistoryAttachments(
            peer_id = chat_id,
            count=100,
            media_type="photo"
        )
        items.extend(response["items"])
        while "next_from" in response:
            start_from = response.get("next_from")
            logger.info(f"меняем start_from на {start_from}")
            response = self.vk.messages.getHistoryAttachments(
                peer_id = chat_id,
                count=100,
                media_type="photo",
                start_from = start_from
            )
            items.extend(response["items"])
        logger.info(f"Получили всего {len(items)}")
        # while True:
        #     #logger.info(f"Получаем вложения беседы с параметрами(peer_id={chat_id},сount=100, offset={offset}, media_type=\"photo\")")
        #     temp = self.vk.messages.getHistoryAttachments(
        #         peer_id=chat_id,
        #         count=100,
        #         offset=offset,
        #         media_type="photo"
        #     )["items"]
        #     logger.info(f"Получили вложений {len(temp)}")
        #     items.extend(temp)

        #     if len(temp) < 100:
        #         break
        #     offset += 100
        return items

if __name__ == '__main__':
    try:
        app = Vkd('https://vk.com/sweet_diabetes')
        logger.info("Приложение инициализировано")
        #asyncio.run(app.main(app.utils.ids_type))
        asyncio.run(app.main())
    except Exception as e:
        logger.error(f"ОШИБКА: {e}")
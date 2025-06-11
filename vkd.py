import re
import sys
import math
import time
import yaml
import json
import random
import vk_api
import yt_dlp, yt_dlp_proxy
import logging
import asyncio
import aiohttp
import argparse
import aiofiles
from pathlib import Path
from pytils import numeral
from tqdm.asyncio import tqdm
from datetime import datetime

from filter import check_for_duplicates
from proxy import construct_proxy_string
from vk_audio_decryptor import Audio

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] - %(name)s-%(filename)s:%(funcName)s():%(lineno)d - %(levelname)s - %(message)s'
)

logger = logging.getLogger("vkd")

APP_DIR = Path(__file__).resolve().parent
CONFIG_PATH = APP_DIR.joinpath("config.yaml")
PROXY_PATH = APP_DIR.joinpath("proxy.json")


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


class Vkd:
    def __init__(self, vk_ids:str, args_from_cli):
        logger.debug("Vkd init — загружен логгер")
        token = load_token_from_config()
        logger.debug(f"Vkd init — токен загружен: {token}")

        self.session = VkSession(token)
        logger.debug("Vkd init — сессия создана")
        self.vk = self.session.vk

        self.video = Video(self.vk)
        logger.debug("Vkd init — Video создан")

        self.groups = Groups(self.vk, self.video)
        logger.debug("Vkd init — groups создан")

        self.wall = Wall(self.vk, self.groups)
        logger.debug("Vkd init — Wall создан")

        self.photos = Photos(self.vk)
        logger.debug("Vkd init — Photos создан")   

        self.messages = Messages(self.vk)
        logger.debug("Vkd init — Messages создан")

        self.cli_args = args_from_cli
        self.utils = Utils(self.vk, self.photos, self.cli_args)
        logger.debug("Vkd init — utils создан")
        self.vk_ids, self.ids_type = self.utils.vk_resolve_ids(vk_ids)
        logger.info(f"Vkd init — ids разрешены:{self.vk_ids} с типом {self.ids_type}")

        self.audio = Audio(token=token, owner_id=self.vk_ids, download_dir=BASE_DIR or None)
        logger.debug("Vkd init — Audio создан")
        #self.dir_name: Path = ''

    async def main(self, d_photos = None, d_videos = None, d_wall = None, d_chat = None, d_audio = None):
        """
        Основной модуль, принимающий CLI аргументы. Определяет тип аргумента target_id. Скачивает фото/видео в зависимости от параметров
        d_photos, d_videos, d_wall, d_chat
        """
        type = self.ids_type
        all_photos = []
        all_videos = []
        d_dir = None # на случай, если провалили всё внутри main и хотим запустить check_for_duplicates. Она вернет 0
        
        # проверки несовместимых комбинаций параметров
        if type =='user' and d_chat:
            sys.exit("Ссылка распознана как пользователь, но выбран аргумент --chat")
        
        if type =='group' and d_chat:
            sys.exit("Ссылка распознана как группа, но выбран аргумент --chat")

        if type =='chat' and d_wall:
            sys.exit("Ссылка распознана как чат, но выбран аргумент --wall")

        if type =='chat' and not d_photos and not d_videos:
            sys.exit("Для ссылки чата не указан ни один из аргументов --photos --videos")

        logger.info("Приступаем к получению данных")
        # основная логика
        if d_audio:
            await self.audio.main()
            

        if type == 'group': 
            if self.utils.check_group_ids(self.vk_ids):
                for group in self.vk_ids:
                    all_photos.clear()
                    all_videos.clear()

                    if d_wall:
                        # получаем посты со стены (сохраняются в groups.photos)
                        logger.info(f"Пытаемся получить фото стены")
                        all_photos.extend(self.wall.vk_get_posts(group_id=group))
                        logger.info(f"Пытаемся получить фото стены: получили {len(all_photos)}")
                    if d_photos:
                        logger.info(f"Пытаемся получить все альбомы группы: {group}")
                        items = self.photos.vk_getALL(group)
                        logger.info(f"Пытаемся получить все альбомы группы: собрали фотографий {len(items)}")
                        all_photos.extend(self.utils.extract_from_raw_data(type='photos', raw_data=items, owner_id=group))
                    if d_videos:
                        # ВИДЕО ШОРТЫ НЕ РАБОТАЮТ В КОНТАКТЕ, ИХ АПИ НЕ ГОТОВО, ОБХОДНОЙ ПУТЬ БАГНУТЫЙ
                        # logger.info(f"Пытаемся получить все видео группы: {group}")
                        # logger.info(f"Пытаемся получить видео шорты со стены")
                        # items = self.wall.vk_get_posts(group_id=group, only_videos=True)
                        # all_videos.extend(self.utils.extract_from_raw_data(type='videos', raw_data=items, owner_id=group))
                        # logger.info(f"Пытаемся получить видео шорты со стены: собрали {len(items)}")

                        items = self.video.vk_video_get(group)
                        logger.info(f"Пытаемся получить все видео группы: собрали {len(items)}")
                        all_videos.extend(self.utils.extract_from_raw_data(type='videos', raw_data=items, owner_id=group))

                    group_name = self.utils.get_group_title(group)
                    d_dir = BASE_DIR.joinpath(group_name)
                    self.utils.create_dir(d_dir)

        if type == 'user':
            if self.utils.check_user_ids(self.vk_ids):
                for user in self.vk_ids:
                    all_photos.clear()
                    all_videos.clear()

                    if d_photos:
                        items = self.photos.vk_user_get(user, 'saved')
                        logger.info(f"Пытаемся получить фото: saved получили {len(items)}")
                        all_photos.extend(self.utils.extract_from_raw_data(type='photos', raw_data=items, owner_id=user))

                        items = self.photos.vk_user_get(user, 'profile')
                        logger.info(f"Пытаемся получить фото: profile получили {len(items)}")
                        all_photos.extend(self.utils.extract_from_raw_data(type='photos', raw_data=items, owner_id=user))

                        items = self.photos.vk_user_get(user, 'wall')
                        logger.info(f"Пытаемся получить фото: wall получили {len(items)}")
                        all_photos.extend(self.utils.extract_from_raw_data(type='photos', raw_data=items, owner_id=user))

                        items = self.photos.vk_getALL(user, 'user')
                        logger.info(f"Пытаемся получить фото: getall получили {len(items)}")
                        all_photos.extend(self.utils.extract_from_raw_data(type='photos', raw_data=items, owner_id=user))

                    if d_videos:
                        logger.info(f"Пытаемся получить все видео пользователя: {user}")
                        items = self.video.vk_video_get(user)
                        logger.info(f"Пытаемся получить все видео пользователя: собрали {len(items)}")
                        all_videos.extend(self.utils.extract_from_raw_data(type='videos', raw_data=items, owner_id=user))

                    if d_wall:
                        logger.info(f"Пытаемся получить фото стены")
                        all_photos.extend(self.wall.vk_get_posts(user))
                        logger.info(f"Пытаемся получить фото стены: получили {len(self.groups.photos)}")

                    username = self.utils.get_username(user)
                    d_dir = BASE_DIR.joinpath(username)
                    self.utils.create_dir(d_dir)

        if type == 'chat':
            for chat in self.vk_ids:
                all_photos.clear()
                all_videos.clear()

                chat_title_or_name = "Неизвестный чат" # Значение по умолчанию
                if self.utils.check_chat_id(chat):
                    if chat > 0:
                        chat_title_or_name = self.utils.get_username(str(chat))
                    elif chat < 0:
                        chat_title_or_name = self.utils.get_chat_title(str(chat))
                else:
                    logger.error(f"Не смогли определить чат {chat}")
                    continue # или обработать ошибку иначе

                
                if d_photos and d_videos:
                    items = self.messages.vk_getHistoryAttachments(chat, "photo")
                    items.extend(self.messages.vk_getHistoryAttachments(chat, "video")) # видео не работают, будет пустой результат
                elif d_photos:
                    items = self.messages.vk_getHistoryAttachments(chat, "photo")
                elif d_videos:
                    items = self.messages.vk_getHistoryAttachments(chat, "video") # видео не работают, будет пустой результат
                logger.info(f"Пытаемся получить фото из переписки: получили {len(items)}")
                all_photos.extend(self.utils.extract_from_raw_data(type='chat', raw_data=items, owner_id=chat))
                logger.info(f'Обработаны items {len(items)}, в photos лежит {len(all_photos)}')

                d_dir = BASE_DIR.joinpath(f"Переписка {safe_filename(chat_title_or_name)}")
                self.utils.create_dir(d_dir)
                
        if d_photos or d_wall:
            await download_photos(self.utils, d_dir, all_photos)
        if d_videos:
            await download_videos(d_dir, all_videos, self.cli_args)
        if 'd_dir' in locals() and d_dir.exists(): # Проверяем, была ли d_dir создана и существует
            logger.info("Проверка на дубликаты")
            dublicates_count = check_for_duplicates(d_dir)
            logger.info(f"Дубликатов удалено: {dublicates_count}")
        else:
            dublicates_count = 0 # Если директория не была создана/указана

        logger.info(f"Итого скачено: {len(all_photos + all_videos) - dublicates_count} медиафайлов")

class VkSession:
    '''Класс для авторизации по токену, создает в параметр vk, использующий апи Вконтакте'''
    def __init__(self, token):
        vk_session = vk_api.VkApi(token=token)
        self.vk = vk_session.get_api()
        logger.info("Успешно авторизовались")

class Video:
    '''Основной класс для получения видео через апи.'''
    def __init__(self, vk):
        self.vk = vk

    def vk_video_get(self, owner_id) -> dict:
        offset = 0
        all_videos = []
        while True:
            response = self.vk.video.get(
                owner_id=owner_id,
                count=100,
                offset=offset
            )
            count = response["count"]
            temp = response["items"]
            logger.info(f"Сбор видео: длина items {len(temp)}")
            logger.info(f"Сбор видео: Ожидаем{count} получено {len(all_videos)}")
            all_videos.extend(temp)

            if len(temp) < 100:
                if len(temp)==99 and offset==0:
                    offset+=99
                    continue
                break
            offset += 100
        return all_videos
    
    def vk_getVideoByid(self, owner_id, video_id) -> dict:
        logger.info(f"Получаем видео по id {video_id}")
        return self.vk.video.get(
            owner_id=owner_id,
            videos = video_id
        )["items"]

class Groups:
    'Вспомогательный класс Groups используется в связке Wall для получения фото постов стены'
    def __init__(self, vk, video_class: Video):
        self.vk = vk
        self.videos = video_class

    def get_single_post(self, post: dict):
        """Проходимся по всем вложениям поста и отбираем только картинки"""
        post_items = []
        try:
            for i, attachment in enumerate(post["attachments"]):
                if attachment["type"] == "photo":
                    file_type = attachment["type"]
                    photo_id = post["attachments"][i]["photo"]["id"]
                    owner_id = post["attachments"][i]["photo"]["owner_id"]
                    photo_url = post["attachments"][i]["photo"]["sizes"][-1].get("url")
                    if photo_url != None or photo_url != '':
                        post_items.append({
                            "type": file_type,
                            "id": photo_id,
                            "owner_id": owner_id,
                            "url": photo_url,
                            "date": datetime.fromtimestamp(int(post["attachments"][i]["photo"]["date"])).strftime('%Y-%m-%d %H-%M-%S')
                        })
        except Exception as e:
            raise(e)
        
        return post_items
    
    def get_single_post_video(self, post:dict):
        """Проходимся по всем вложениям поста и отбираем только видео-шорты"""
        post_items = []
        attachments = post.get("attachments")
        try:
            for attachment in attachments:
                if attachment.get("type") == "video":
                    #if attachment.get("video").get("type") == "short_video":
                        #print(attachment.get("video").get("type"))
                        id = attachment.get("video").get("id")
                        owner_id = attachment.get("video").get("owner_id")
                        video_id = f'{owner_id}_{id}'
                        #print(video_id)
                        video_item = self.videos.vk_getVideoByid(owner_id, video_id)
                        post_items.extend(video_item)
                    #else:
                        #logger.info("Вложение с обычным видео, не short")
        except Exception as e:
            logger.error(e)
        
        return post_items
            
class Wall:
    'Вспомогательный класс Wall, использующий апи вконтакте wall.get для получения постов. Зависим от Groups'
    def __init__(self, vk, groups:Groups, group_id=None):
        self.vk = vk
        self.groups = groups
        self.group_id = group_id


    def vk_get_posts(self, group_id, only_videos=None):
        'Получаем со стены по 100 постов за проход и проверяем вложения, возвращаем обработанный список wall_items с фото, готовый к загрузке'
        wall_items = []
        offset = 0
        while True:
            posts = self.vk.wall.get(
                owner_id=group_id,
                count=100,
                offset=offset
            )["items"]
            for post in posts:
                try:
                    # Пропускаем посты с рекламой
                    if post["marked_as_ads"]:
                        logger.info("Игнорируем пост с рекламой")
                        continue

                    attachments = post.get("attachments", [])
                    if not attachments:
                        logger.info("Пропущен пост без вложений")
                        continue  # или continue, в зависимости от контекста

                    # Если пост скопирован с другой группы
                    if "copy_history" in post:
                        logger.info("Пост с другой группы, проверяем вложения")
                        if not only_videos:
                            if "attachments" in post["copy_history"][0]:
                                wall_items.extend(self.groups.get_single_post(post["copy_history"][0]))

                    if attachments:
                        if only_videos:
                            try:
                                for attachment in attachments:
                                    if attachment.get("type") == "video":
                                        logger.debug(f"пробуем достать видео из поста.")
                                        wall_items.extend(self.groups.get_single_post_video(post)) #возвращает видео-айди из постов
                            except Exception as e:
                                logger.error("Ошибка парсинга поста", post, e)
                        else:
                            wall_items.extend(self.groups.get_single_post(post))
                except Exception as e:
                    logger.error("Иная ошибка парсинга поста", post, e)

            logger.info(f"Собрали со стены медиафайлов: {len(wall_items)}")
            if len(posts) < 100:
                break
            offset += 100

        logger.info("Закончили парсить посты стены")
        return wall_items

class Photos:
    '''Основной класс для получения фотографий через апи vk.'''
    def __init__(self, vk):
        self.vk = vk

    def vk_getALL(self, owner_id) -> dict:
        offset = 0
        all_photos = []
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
  
class Messages:
    'Основной класс для апи vk.messages'
    def __init__(self, vk):
        self.vk = vk

    def vk_getHistoryAttachments(self, chat_id, types):
        items = []
        response = self.vk.messages.getHistoryAttachments(
            peer_id = chat_id,
            count=100,
            media_type=types
        )
        #print(response["items"])
        items.extend(response["items"])
        while "next_from" in response:
            start_from = response.get("next_from")
            logger.info(f"Меняем start_from на {start_from}")
            response = self.vk.messages.getHistoryAttachments(
                peer_id = chat_id,
                count=100,
                media_type=types,
                start_from = start_from
            )
            items.extend(response["items"])
        logger.info(f"Получили всего {len(items)}")

        return items

class Utils:
    'Вспомогательный класс для Vkd жизненно важен для основного функционала'
    def __init__(self, vk, photosClass:Photos, cli_args=None):
        self.vk = vk
        self.photosClass = photosClass
        self.cli_args = cli_args # Сохраняем args
        self.ids_type = ''

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
            else:
                logger.info("Это не ссылка на чат")

            is_numeric_string = re.fullmatch(r"-?\d+", item)

            logger.debug(f"cli_args check = {self.cli_args}")
            # доп проверка чата
            if self.cli_args and self.cli_args.chat:
                if is_numeric_string and self.check_chat_id(item):
                    self.ids_type = 'chat'
                    result.append(int(item))
                    logger.debug(f"ID '{int(item)}' распознан как 'chat'.")
                    continue
            else:
                logger.info(f"Не прошли проверку аргумента кли, что это чат {item}")
            
            # проверка числового значения: чат/пользователь/группа
            if is_numeric_string:
                numeric_item = int(item)
                if numeric_item < 0:
                    logger.info(f"Это число с минусом{item}")
                    if self.check_group_id(item):
                        self.ids_type = 'group'
                        result.append(numeric_item)
                        logger.debug(f"ID '{numeric_item}' распознан как 'group'.")
                        continue
                else:
                    if self.check_chat_id(str(numeric_item)): # Для больших положительных ID чатов
                        self.ids_type = 'chat'
                        result.append(numeric_item)
                        logger.debug(f"ID '{numeric_item}' распознан как 'chat'.")
                        continue
                    elif self.check_user_id(str(numeric_item)):
                        self.ids_type = 'user'
                        result.append(numeric_item)
                        logger.debug(f"ID '{numeric_item}' распознан как 'user'.")
                        continue

            # Если это обычная ссылка вида vk.com/username её мы попробуем отрезовлить ниже
            screen_name_match = screen_name_pattern.search(item)
            if screen_name_match:
                item = screen_name_match.group(1)
            else:
                logger.info("Это не обычная ссылка на группу или пользователя")

            # Пробуем обычный resolve через API
            try:
                resolved = self.vk.utils.resolveScreenName(screen_name=item)
                if resolved and resolved.get("object_id"):
                    object_id = resolved["object_id"]
                    type_ = resolved["type"]
                    if type_ == "group":
                        object_id = -object_id
                    self.ids_type = type_
                    result.append(object_id)
                    continue
            except vk_api.VkApiError as e:
                logger.error(e)
            except Exception as e:
                raise ValueError(f"Ошибка при разрешении '{item}': {e}")

            

        return result, self.ids_type
    
    def extract_from_raw_data(self, type, raw_data, owner_id):
        extracted_items = []
        if type == 'photos':
            logger.info(f"Пробуем достать фото из items c типом {type}")
            albums_dict = self.photosClass.vk_getAlbums(owner_id)
            for photo in raw_data:
                album_id = photo.get("album_id")
                album_title = albums_dict.get(album_id, "Без альбома")
                
                extracted_items.append({
                    "id": photo.get("id"),
                    "owner_id": photo.get("owner_id"),
                    "url": photo.get("sizes", [{}])[-1].get("url") if photo.get("sizes") else None,
                    "date": datetime.fromtimestamp(int(photo.get("date"))).strftime('%Y-%m-%d %H-%M-%S'),
                    "album_id": album_id,
                    "album_title": album_title
                })
            return extracted_items
        
        elif type == 'videos':
            logger.info(f"Пробуем достать видео из items c типом {type}")
            for video in raw_data:
                if "player" in video:
                    extracted_items.append({
                        "type": video.get("type"),
                        "id": video.get("id"),
                        "owner_id": video.get("owner_id"),
                        "title": video.get("title"),
                        "player": video.get("player"),
                        "date": datetime.fromtimestamp(int(video.get("date"))).strftime('%Y-%m-%d %H-%M-%S')
                    })
            return extracted_items
        
        elif type == 'chat':
            logger.info(f"Пробуем достать данные из items c типом {type}")
            for item in raw_data:
                attachment = item.get("attachment", {})
                if attachment.get("photo", {}):
                    photo_data = attachment.get("photo", {})
                    #logger.info(photo_data)
                    sizes = photo_data.get("sizes", [])
                    logger.debug(f'"id": {photo_data.get("id")},\n"owner_id": {photo_data.get("owner_id")},\n"url": {sizes[-1].get("url") if sizes and isinstance(sizes[-1], dict) else None},\n"date": {datetime.fromtimestamp(int(photo_data.get("date", 0))).strftime('%Y-%m-%d %H-%M-%S')}')
                    extracted_items.append({
                        "id": photo_data.get("id"),
                        "owner_id": photo_data.get("owner_id"),
                        "url": sizes[-1].get("url") if sizes and isinstance(sizes[-1], dict) else None,
                        "date": datetime.fromtimestamp(int(photo_data.get("date", 0))).strftime('%Y-%m-%d %H-%M-%S')
                    })
            return extracted_items
                
                # --- Видео из чата плохо работают ---
                # if attachment.get("video", {}):
                #     video = attachment.get("video", {})
                #     if "player" in video:
                #         self.videos.append({
                #             "type": attachment.get("type"),
                #             "id": video.get("id"),
                #             "owner_id": video.get("owner_id"),
                #             "title": video.get("title"),
                #             "player": video.get("player"),
                #             "date": datetime.fromtimestamp(int(video.get("date"))).strftime('%Y-%m-%d %H-%M-%S')
                #         })
        

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
            dir_path.mkdir(parents=True, exist_ok=True)

async def download_photo(session: aiohttp.ClientSession, photo_url: str, photo_path: Path):
    try:
        if not photo_path.exists():
            async with session.get(photo_url) as response:
                if response.status == 200:
                    async with aiofiles.open(photo_path, "wb") as f:
                        await f.write(await response.read())
    except Exception as e:
        logger.error(e)

async def download_photos(utils_instance:Utils, photos_path: Path, photos: list):
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
                utils_instance.create_dir(album_dir)
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

async def download_video(video_path:Path, video_link, proxy_url=None):

    ydl_opts = {
        'outtmpl': '{}'.format(video_path), 
        'quiet': True, 
        #'verbose': True,
        'retries': 3, 
        'ignoreerrors': True, 
        'age_limit': 28,
    }
    if proxy_url:
        ydl_opts['proxy'] = proxy_url # Добавляем прокси если он указан

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download(video_link)
            logger.info("Видео загружено: %s" % video_path.name)
    except yt_dlp.utils.DownloadError as e:
        logger.error(f"Ошибка загрузки yt-dlp для {video_link} в {video_path}: {e}")
    except Exception as e:
        logger.error(f"Неожиданная ошибка при загрузке видео {video_link} в {video_path}: {e}")

async def download_videos(videos_path: Path, videos: list, cli_args):
    proxy_str = None
    if cli_args.use_proxy:
        #пробуем получить прокси для yt-dlp
        try:
            if not PROXY_PATH.exists():
                yt_dlp_proxy.update_proxies()
            with open("proxy.json", "r") as f:
                proxy = random.choice(json.load(f))
                proxy_str = construct_proxy_string(proxy)
                logger.info(f"Using proxy from {proxy['city']}, {proxy['country']}")
        except Exception as e:
            logger.error("Ошибка при получении прокси", e)

    futures = []
    for i, video in enumerate(videos, start=1):
        filename = "{}_{}_{}.mp4".format(video["date"], video["owner_id"], video["id"])
        video_path = videos_path.joinpath(filename).resolve()
        if not video_path:
            logger.error("Не может быть создан путь для видео", video_path)
            continue
        if video_path.exists():
            logger.debug(f"Пропущено (уже существует): {video_path.name}")
            continue
        futures.append(download_video(video_path, video["player"], proxy_str))
    logger.info("Мы попробуем скачать %s видео" % len(futures))
    for future in tqdm(asyncio.as_completed(futures), total=len(futures)):
        try:
            await future
        except Exception as e:
            logger.error('Исключение при загрузке видео: %s' % e)


if __name__ == '__main__':
    try:
        parser = argparse.ArgumentParser(
        description="VK Downloader: Утилита для скачивания фото и видео из ВКонтакте.",
        epilog="Пример: python vkd.py --photos https://vk.com/octamillia"
        )

        # 1. Позиционный аргумент (обязательный)
        parser.add_argument("vk_ids",
                            type=str,
                            help="Полный URL страницы/чата (например, 'durov', 'club1', 'https://vk.com/im/convo/123' или '123456').")
        
        # 2. Опциональный аргумент для указания директории сохранения
        parser.add_argument("-o", "--output-dir",
                            type=str,
                            default="D:/ghd/Фотки",
                            help="Путь к папке для сохранения файлов (по умолчанию: D:/ghd/Фотки)") # Вы можете установить здесь путь по умолчанию, например, 'VK_Downloads'

        # 3. Флаги (boolean arguments) для указания типа контента
        parser.add_argument("-p","--photos",
                            action="store_true", # Значение будет True, если флаг указан, иначе False
                            help="Скачивать фотографии (альбомы, сохраненные и т.д., в зависимости от типа vk_ids).")

        parser.add_argument("-v","--videos",
                            action="store_true",
                            help="Скачивать видеозаписи (в зависимости от типа vk_ids).")

        parser.add_argument("-c","--chat",
                            action="store_true",
                            help="Скачивать фотографии из указанного чата/переписки (если vk_ids это ID чата или ссылка на беседу).")
        
        parser.add_argument("-w","--wall",
                            action="store_true",
                            help="Скачивать со стены (если vk_ids это ID пользователя или ID группы).")
        
        parser.add_argument("-u","--use-proxy",
                            action="store_true",
                            help="Использовать прокси для скачивания видео")
        
        parser.add_argument("-a","--audio",
                            action="store_true",
                            help="Скачать аудиозаписи (в зависимости от типа vk_ids).")

        # Парсинг аргументов
        args = parser.parse_args()

        BASE_DIR = Path(args.output_dir)

        if not args.photos and not args.videos and not args.chat and not args.wall and not args.audio:
            parser.print_help()
            sys.exit("Не выбран тип контента для скачивания.")

        if args.chat and args.wall:
            parser.print_help()
            sys.exit("Нельзя скачать стену для чата.")

        app = Vkd(args.vk_ids, args)
        logger.info("Приложение инициализировано")
        asyncio.run(app.main(
            d_photos=args.photos,
            d_videos=args.videos,
            d_wall=args.wall,
            d_chat=args.chat,
            d_audio=args.audio         
        ))
    except Exception as e:
        logger.error(f"ОШИБКА: {e}")
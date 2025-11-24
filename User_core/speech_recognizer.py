import jwt
import aiohttp
import logging
import os
import io
import asyncio
import json
import time
from dotenv import load_dotenv
from pydub import AudioSegment

load_dotenv()

FOLDER_ID = os.getenv("FolderID")
if not FOLDER_ID:
    raise ValueError("Переменная окружения FolderID не задана")

try:
    with open('authorized_key.json', 'r') as f:
        service_account_key = json.load(f)
    required_keys = ['service_account_id', 'private_key', 'id']
    missing_keys = [key for key in required_keys if key not in service_account_key]
    if missing_keys:
        raise ValueError(f"Отсутствуют обязательные ключи в authorized_key.json: {missing_keys}")
except Exception as e:
    raise ValueError(f"Ошибка при загрузке authorized_key.json: {str(e)}")


class SpeechRecognizer:
    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self._iam_token = None
        self._token_expiry = 0
        self.logger.debug("SpeechRecognizer инициализирован")

    async def get_iam_token(self):
        self.logger.debug("Вход в get_iam_token")
        try:
            now = int(time.time())
            if self._iam_token and self._token_expiry - 60 > now:
                self.logger.debug("Возврат кэшированного IAM-токена")
                return self._iam_token

            self.logger.debug("Генерация нового JWT-токена")
            payload = {
                'aud': 'https://iam.api.cloud.yandex.net/iam/v1/tokens',
                'iss': service_account_key['service_account_id'],
                'iat': now,
                'exp': now + 3600
            }
            jwt_token = jwt.encode(
                payload,
                service_account_key['private_key'],
                algorithm='PS256',
                headers={'kid': service_account_key['id']}
            )
            self.logger.debug("Отправка запроса на получение IAM-токена")
            timeout = aiohttp.ClientTimeout(total=10)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(
                    'https://iam.api.cloud.yandex.net/iam/v1/tokens',
                    json={'jwt': jwt_token}
                ) as response:
                    response.raise_for_status()
                    data = await response.json()
                    self._iam_token = data['iamToken']
                    self._token_expiry = now + 3600
                    self.logger.debug(f"IAM-токен получен, срок действия: {self._token_expiry}")
                    return self._iam_token
        except Exception as e:
            self.logger.error(f"Ошибка при получении IAM-токена: {e}", exc_info=True)
            raise ValueError(f"Не удалось получить IAM-токен: {str(e)}")

    async def convert_ogg_to_wav(self, ogg_file: bytes) -> bytes:
        self.logger.debug("Конвертация через pydub")
        try:
            audio = AudioSegment.from_file(io.BytesIO(ogg_file), format="ogg")
            audio = audio.set_frame_rate(16000).set_channels(1).set_sample_width(2)  # 2 байта = 16 бит
            raw_pcm = audio.raw_data
            self.logger.debug(f"Конвертация завершена, размер raw PCM: {len(raw_pcm)} байт")
            return raw_pcm
        except Exception as e:
            self.logger.error(f"Ошибка конвертации через pydub: {e}", exc_info=True)
            raise ValueError(f"Ошибка при конвертации через pydub: {str(e)}")


    async def recognize_speech(self, audio_data: bytes, timeout: float = 30.0) -> str:
        self.logger.debug(f"Вход в recognize_speech (HTTP), размер аудио: {len(audio_data)} байт")
        try:
            token = await self.get_iam_token()
            headers = {
                'Authorization': f'Bearer {token}',
                'Folder-Id': FOLDER_ID,
                'Content-Type': 'application/octet-stream'
            }

            params = {
                'lang': 'ru-RU',
                'format': 'lpcm',
                'sampleRateHertz': '16000',
                'audioChannelCount': '1',
                'profanityFilter': 'false',
                'partialResults': 'false'
            }

            timeout_obj = aiohttp.ClientTimeout(total=timeout)
            async with aiohttp.ClientSession(timeout=timeout_obj) as session:
                async with session.post(
                    'https://stt.api.cloud.yandex.net/speech/v1/stt:recognize',
                    params=params,
                    data=audio_data,
                    headers=headers
                ) as response:
                    response.raise_for_status()
                    result = await response.json()
                    self.logger.debug(f"Ответ от HTTP STT: {result}")

                    text = result.get('result', '')
                    if text.strip():
                        return text

                    self.logger.warning("Транскрипция не получена от сервиса STT")
                    return ""

        except asyncio.TimeoutError:
            self.logger.error(f"Тайм-аут распознавания речи после {timeout:.1f} секунд", exc_info=True)
            raise ValueError(f"Тайм-аут распознавания речи после {timeout} секунд")

        except Exception as e:
            self.logger.error(f"Ошибка при распознавании речи: {e}", exc_info=True)
            raise ValueError(f"Ошибка при распознавании речи: {str(e)}")

        finally:
            self.logger.debug("Выход из recognize_speech")

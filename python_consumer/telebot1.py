import telebot
import requests

TOKEN = '2100684897:AAEranEeY2HelOJcbVfRmctiDC9uoPzMJ3Q'
chat = '-1001597149206'


def telegram_bot_sendtext(TOKEN,chat,message):
    url = f'https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={chat}&parse_mode=Markdown&text={message}'
    response = requests.get(url)
    return response.json()

response = telegram_bot_sendtext(TOKEN,chat,'Hi')


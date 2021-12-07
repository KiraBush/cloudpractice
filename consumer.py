from time import time
from uuid import uuid4
#from confluent_kafka import DeserializingConsumer
#from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
import telegram as tlg
from telegram.ext import Updater, CommandHandler, Filters, MessageHandler, ChosenInlineResultHandler, ChatJoinRequestHandler
from datetime import datetime
import requests
from telebot import TeleBot ,types,util
stat_dict = dict()


TOKEN = '2100684897:AAHtiSM6TdaeuxvGSX-0Bz4e0-WyDwQLwRQ'

bot=TeleBot(TOKEN)
@bot.message_handler(commands=['help'])
def help(update):
     bot.send_message(update.chat.id,'На данный момент я знаю следующие команды \n /help - справка о командах \n /stat - статистика по пользователям',reply_to_message_id=update.message_id)


@bot.message_handler(commands=['publish'])
def publish(update):
    print("Message from tlg: ", update.text.replace('/publish ',''))

@bot.message_handler(commands=['stat'])
def stat(update):
  if stat_dict=={}:
      bot.send_message(update.chat.id,'Статистика пока отсутствует',reply_to_message_id=update.message_id)
  else:
      if update.chat.type == 'supergroup':
        chat_id =  update.chat.title
        stat_chat = stat_dict[chat_id]
        rez = ''
        for i in sorted(stat_chat.items(), key=lambda x: (-x[1],x[0])):
          rez += ' '.join([str(j)for j in i])+'\n'
        bot.send_message(update.chat.id,rez,reply_to_message_id=update.message_id) 
      elif update.chat.type == 'private':
        rez = '' 
        for i in stat_dict:
          rez += str(i)+'\n'
          for k in sorted(stat_dict[i].items(), key=lambda x: (-x[1],x[0])):
            rez += ' '.join([str(j)for j in k])+'\n'
          rez += '\n'
        bot.send_message(update.chat.id,rez,reply_to_message_id=update.message_id)
    
@bot.my_chat_member_handler()
def run(message):
    global stat_dict
    if message.new_chat_member.status=='left' or message.new_chat_member.status=='kicked':
        if message.chat.title in stat_dict:
            del stat_dict[message.chat.title]

    
@bot.message_handler(content_types='text')
def echo(update):
    global stat_dict
##    print(update)
    parazid = ['ковид',"коронавирус", "вакцина", "вакцинация"]
    if update.chat.type == 'supergroup':
      chat_id =  update.chat.title
    else:
      chat_id = update.chat.username
    for i in parazid:
      if i in update.text.lower():
          if chat_id not in stat_dict:
            stat_dict[chat_id] = {}
          stat_dict[chat_id][update.json['from']['username']] = stat_dict[chat_id].get(update.json['from']['username'],0)+1
          bot.send_message(update.chat.id,'Данная информация может содержать информацию о Короновирусно инфекции',reply_to_message_id=update.message_id)
          

if __name__ == "__main__":
    bot.infinity_polling(allowed_updates=util.update_types)



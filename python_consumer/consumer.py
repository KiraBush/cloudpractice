from time import time
from uuid import uuid4
#from confluent_kafka import DeserializingConsumer
#from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
import telegram as tlg
from telegram.ext import Updater, CommandHandler, Filters, MessageHandler
from datetime import datetime

stat_dict = dict()


TOKEN = '2100684897:AAEranEeY2HelOJcbVfRmctiDC9uoPzMJ3Q'


bot = tlg.Bot(token=TOKEN)

updater = Updater(token=TOKEN, use_context=True)


def help(update, context):
    update.message.reply_text('Вот мои команды:\n/help - command list,\n/stats - statistics of users that says about COVID', quote=True)


def publish(bot, update):
    str_msg = ' '.join(update.args)
    print("Message from tlg: ", str_msg)


def echo(update, context):
    global stat_dict
    """Echo the user message."""
    parazid = ['ковид',"коронавирус", "вакцина", "вакцинация"]
    if update.effective_message.chat.type == 'supergroup':
      chat_id =  update.message.chat.title
    else:
      chat_id = update.message.chat.username
    for i in parazid:
      if i in update.message.text.lower():
          if chat_id not in stat_dict:
            stat_dict[chat_id] = {}
          stat_dict[chat_id][update.effective_message.from_user.name] = stat_dict[chat_id].get(update.effective_message.from_user.name,0)+1
          update.message.reply_text('Данная информация может содержать информацию о Короновирусно инфекции', quote=True)
    if update.message.text=='/help':
      update.message.reply_text('На данный момент я знаю следующие команды \n /help - справка о командах \n /stat - статистика по пользователям', quote=True)

def stats(update, context):
  print(update.effective_message.chat.type)
  if update.effective_message.chat.type == 'supergroup':
    chat_id =  update.message.chat.title
    stat_chat = stat_dict[chat_id]
    rez = ''
    for i in sorted(stat_chat.items(), key=lambda x: (-x[1],x[0])):
      rez += ' '.join([str(j)for j in i])+'\n'
    update.message.reply_text(rez)   
  elif update.effective_message.chat.type == 'private':
    rez = '' 
    for i in stat_dict:
      rez += str(i)+'\n'
      for k in sorted(stat_dict[i].items(), key=lambda x: (-x[1],x[0])):
        rez += ' '.join([str(j)for j in k])+'\n'
      rez += '\n'
    update.message.reply_text(rez)  


publish_handler = CommandHandler('publish', publish)
help_handler = CommandHandler('help', help)
stat_handler = CommandHandler('stats', stats)

updater.dispatcher.add_handler(MessageHandler(Filters.text & ~Filters.command, echo))

updater.dispatcher.add_handler(publish_handler)
updater.dispatcher.add_handler(help_handler)
updater.dispatcher.add_handler(stat_handler)

if __name__ == "__main__":

    #while True:
    try:
            
        updater.start_polling()
    except KeyboardInterrupt:
        print("err")
        #break
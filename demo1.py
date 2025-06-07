import re
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Optional
from langchain_community.document_loaders import TextLoader
from langchain_community.embeddings import HuggingFaceInstructEmbeddings
from langchain_community.vectorstores import FAISS
from pypdf import PdfReader
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.memory import ConversationBufferWindowMemory
from langchain.chains import create_retrieval_chain, create_history_aware_retriever
from langchain.chains.combine_documents import create_stuff_documents_chain
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from telegram import Update, ForceReply
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes
from langchain_community.llms import Ollama
import httpx
import asyncio
import logging
from telegram.constants import ChatAction
import uvicorn
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from aiokafka.helpers import create_ssl_context
import json
from contextlib import asynccontextmanager
from uuid import uuid4
import os


# ===== Конфигурация Kafka =====
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:29092')
KAFKA_QUESTIONS_TOPIC = 'questions_topic'
KAFKA_ANSWERS_TOPIC = 'telegram_responses'
KAFKA_CONSUMER_GROUP = 'assistant_consumer_group'
class KafkaClient:
    @classmethod
    async def get_producer(cls) -> AIOKafkaProducer:
        if cls._producer is None:
            cls._producer = AIOKafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=3,
                request_timeout_ms=10000,
                reconnect_backoff_ms=1000
            )
            try:
                await cls._producer.start()
            except Exception as e:
                logger.error(f"Failed to start Kafka producer: {str(e)}")
                cls._producer = None
                raise
        return cls._producer
# ===== FastAPI часть =====
app = FastAPI(title="API ассистента приемной комиссии")

# ===== Инициализация Kafka =====
class KafkaClient:
    _producer: Optional[AIOKafkaProducer] = None
    _consumer: Optional[AIOKafkaConsumer] = None
    
    @classmethod
    async def get_producer(cls) -> AIOKafkaProducer:
        if cls._producer is None:
            cls._producer = AIOKafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                request_timeout_ms=10000,
                max_batch_size=16384,
                linger_ms=5
            )
            try:
                await cls._producer.start()
            except Exception as e:
                logger.error(f"Failed to start Kafka producer: {str(e)}")
                cls._producer = None
                raise
        return cls._producer
    
    @classmethod
    async def get_consumer(cls) -> AIOKafkaConsumer:
        if cls._consumer is None:
            cls._consumer = AIOKafkaConsumer(
                KAFKA_ANSWERS_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=KAFKA_CONSUMER_GROUP,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                request_timeout_ms=10000,
                session_timeout_ms=30000,
                heartbeat_interval_ms=3000
            )
            try:
                await cls._consumer.start()
            except Exception as e:
                logger.error(f"Failed to start Kafka consumer: {str(e)}")
                cls._consumer = None
                raise
        return cls._consumer

# ===== Инициализация модели и базы знаний =====
def initialize_components():
    # Загрузка и обработка PDF
    documents_1 = ''
    reader = PdfReader('C:\\Users\\HP\\OneDrive\\Рабочий стол\\диплом\\ilovepdf_merged_merged.pdf')
    for page in reader.pages:
        documents_1 += page.extract_text()
    # Настройка чанкинга
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=600,
        chunk_overlap=250,
        separators=["\n\n", "\n", " ", ""]
    )
    split_1 = splitter.split_text(documents_1)
    split_1 = splitter.create_documents(split_1)
    # Инициализация эмбеддингов
    instructor_embeddings = HuggingFaceInstructEmbeddings(
        model_name='sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2', 
        model_kwargs={'device':'cpu'}
    )  
    # Создание и сохранение векторной базы
    db = FAISS.from_documents(split_1, instructor_embeddings)
    db.save_local('vector store/vuz')
    return db, instructor_embeddings
# Инициализация компонентов
db, instructor_embeddings = initialize_components()
loaded_db = FAISS.load_local('vector store/vuz',
                              instructor_embeddings, 
                              allow_dangerous_deserialization=True)

# ===== Настройка языковой модели =====
llm = Ollama(
    model='mistral:7b-instruct',
    temperature=0.6,
    num_ctx=1024,
    top_k=40,
    repeat_penalty=1.2,
    num_thread=4
)

# ===== Промпт и цепочки =====
system_prompt = """Ты — вежливый ассистент приемной комиссии ЛЭТИ. Отвечай ТОЛЬКО на основе предоставленных документов, без лишней информации, НА РУССКОМ ЯЗЫКЕ.  

Правила ответа:
1. Формулируй ясно и грамотно НА РУССКОМ ЯЗЫКЕЫ
2. Если информация есть в контексте — дай точный ответ
3. Если информации нет — скажи "В правилах приема эта информация не указана"
4. Не надо писать цифру в начале ответа!!
[Основной ответ] 
- [Четкий ответ на вопрос по документам]
- [Не надо писать цифру в начале предложения]
Контекст: {context}"""

qa_prompt = ChatPromptTemplate.from_messages([
    ("system", system_prompt),
    ("human", "{input}")
])

retriever = loaded_db.as_retriever(
    search_type="similarity",
    search_kwargs={
        "k":4,
        "fetch_k": 20
    }
)

question_answer_chain = create_stuff_documents_chain(llm, qa_prompt)
qa_chain = create_retrieval_chain(retriever, question_answer_chain)

# ===== Telegram бот =====
TELEGRAM_TOKEN = "ВАШ_ТОКЕН"

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Глобальное хранилище для ожидающих ответов
pending_responses = {}

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    await update.message.reply_html(
        rf"Привет {user.mention_html()}! Я бот-ассистент приемной комиссии ЛЭТИ.",
        reply_markup=ForceReply(selective=True),
    )

def refine_answer(answer: str) -> str:
    """Облагораживает ответ модели"""
    answer = re.sub(r'(System:|Контекст:|По контексту:|Документ говорит)', '', answer)
    
    corrections = {
        "не указано": "в правилах не указано",
        "можно подать": "возможна подача",
        "должен быть": "необходимо предоставить"
    }
    
    for bad, good in corrections.items():
        answer = answer.replace(bad, good)
    
    if not answer.endswith(('.', '!', '?')):
        answer += '.'
        
    return answer.strip()

async def consume_answers():
    while True:
        consumer = None
        try:
            consumer = await KafkaClient.get_consumer()
            async for msg in consumer:
                try:
                    data = msg.value
                    chat_id = data['chat_id']
                    answer = data['answer']
                    
                    if chat_id in pending_responses:
                        typing_message = pending_responses.pop(chat_id)
                        await typing_message.edit_text(f"📌 {answer}")
                except Exception as e:
                    logger.error(f"Error processing Kafka message: {str(e)}")
        except Exception as e:
            logger.error(f"Kafka consumer error: {str(e)}, retrying in 5 seconds...")
            await asyncio.sleep(5)
        finally:
            if consumer is not None:
                await consumer.stop()
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        user_input = update.message.text
        chat_id = update.effective_chat.id
        
        await context.bot.send_chat_action(
            chat_id=chat_id,
            action="typing"
        )
        
        typing_message = await update.message.reply_text("⌛ Обрабатываю запрос...")
        pending_responses[chat_id] = typing_message
        
        try:
            producer = await asyncio.wait_for(
                KafkaClient.get_producer(),
                timeout=5.0
            )
            question_id = str(uuid4())
            
            await asyncio.wait_for(
                producer.send(
                    KAFKA_QUESTIONS_TOPIC,
                    value={
                        'question_id': question_id,
                        'chat_id': chat_id,
                        'question': user_input,
                        'timestamp': int(time.time())
                    }
                ),
                timeout=5.0
            )
        except asyncio.TimeoutError:
            await typing_message.edit_text("⚠️ Превышено время ожидания обработки запроса.")
            return
            
    except Exception as e:
        logger.error(f"Error in handle_message: {str(e)}")
        if chat_id in pending_responses:
            typing_message = pending_responses.pop(chat_id)
            await typing_message.edit_text("⚠️ Ошибка обработки запроса.")

async def check_topic_exists():
    consumer = AIOKafkaConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS
    )
    await consumer.start()
    topics = await consumer.topics()
    await consumer.stop()
    if KAFKA_QUESTIONS_TOPIC not in topics:
        raise Exception(f"Topic {KAFKA_QUESTIONS_TOPIC} does not exist!")
    
async def worker_processing():
    while True:
        consumer = None
        try:
            consumer = AIOKafkaConsumer(
                KAFKA_QUESTIONS_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id='worker_group',
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                request_timeout_ms=30000
            )
            await consumer.start()
            
            producer = AIOKafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                request_timeout_ms=30000
            )
            await producer.start()
            
            async for msg in consumer:
                try:
                    data = msg.value
                    question = data['question']
                    chat_id = data['chat_id']
                    question_id = data['question_id']
                    
                    docs = retriever.invoke(question)
                    if not docs:
                        raise ValueError("Документы не найдены")
                    
                    response = await asyncio.to_thread(
                        qa_chain.invoke,
                        {
                            "input": question,
                            "context": "\n".join([d.page_content for d in docs[:2]])
                        }
                    )
                    
                    answer = refine_answer(response.get("answer", ""))
                    
                    await producer.send(
                        KAFKA_ANSWERS_TOPIC,
                        value={
                            'question_id': question_id,
                            'chat_id': chat_id,
                            'answer': answer,
                            'timestamp': int(time.time())
                        }
                    )
                    
                except Exception as e:
                    logger.error(f"Worker error processing question: {str(e)}")
                    try:
                        await producer.send(
                            KAFKA_ANSWERS_TOPIC,
                            value={
                                'question_id': question_id,
                                'chat_id': chat_id,
                                'answer': "⚠️ Произошла ошибка при обработке вашего вопроса.",
                                'timestamp': int(time.time())
                            }
                        )
                    except Exception as send_error:
                        logger.error(f"Failed to send error response: {str(send_error)}")
                    
        except Exception as e:
            logger.error(f"Worker error: {str(e)}, retrying in 5 seconds...")
            await asyncio.sleep(5)
        finally:
            if consumer is not None:
                await consumer.stop()
            if 'producer' in locals():
                await producer.stop()

async def wait_for_kafka():
    max_retries = 5
    for i in range(max_retries):
        try:
            producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
            await producer.start()
            await producer.stop()
            return True
        except Exception as e:
            if i == max_retries - 1:
                raise
            await asyncio.sleep(5)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Ждем готовности Kafka
    await wait_for_kafka()
    await check_topic_exists()
    # Запускаем обработчики
    asyncio.create_task(consume_answers())
    asyncio.create_task(worker_processing())
    
    yield
    
    # Завершение работы
    await KafkaClient.close()
# ===== FastAPI endpoints =====
class QuestionRequest(BaseModel):
    question: str
    chat_id: str

class AnswerResponse(BaseModel):
    answer: str
    sources: List[str]

@app.post("/ask", response_model=AnswerResponse)
async def ask_question(request: QuestionRequest):
    try:
        producer = await KafkaClient.get_producer()
        question_id = str(uuid4())
        
        await producer.send(
            KAFKA_QUESTIONS_TOPIC,
            value={
                'question_id': question_id,
                'chat_id': request.chat_id,
                'question': request.question,
                'timestamp': int(time.time())
            }
        )
        
        return AnswerResponse(
            answer="Ваш вопрос принят в обработку",
            sources=[]
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    asyncio.create_task(consume_answers())
    asyncio.create_task(worker_processing())
    yield
    # Shutdown
    await KafkaClient.close()

app = FastAPI(lifespan=lifespan)

# ===== Главная функция =====
if __name__ == "__main__":
    import threading
    import time
    
    # Запускаем FastAPI в отдельном потоке
    api_thread = threading.Thread(
        target=uvicorn.run,
        kwargs={"app": app, "host": "0.0.0.0", "port": 8000},
        daemon=True
    )
    api_thread.start()
    
    # Запускаем Telegram бота в основном потоке
    application = Application.builder().token(TELEGRAM_TOKEN).build()
    application.add_handler(CommandHandler("start", start))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    
    # Запускаем обработку сообщений
    application.run_polling()

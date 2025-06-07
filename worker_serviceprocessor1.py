import asyncio
import json
import logging
from demo1 import qa_chain, refine_answer
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.helpers import create_ssl_context

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class KafkaWorker:
    def __init__(self):
        self.consumer = None
        self.producer = None
        self.running = False
        self.max_heartbeat_failures = 3
        self.heartbeat_failures = 0

    async def initialize(self):
        max_retries = 5
        retry_delay = 5
        
        for attempt in range(max_retries):
            try:
                self.consumer = AIOKafkaConsumer(
                    'questions_topic',
                    bootstrap_servers='localhost:29092',
                    group_id='worker_group',
                    auto_offset_reset='earliest',
                    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                    session_timeout_ms=60000,
                    heartbeat_interval_ms=20000,
                    max_poll_interval_ms=600000,
                    enable_auto_commit=False,
                    max_poll_records=1
                )

                self.producer = AIOKafkaProducer(
                    bootstrap_servers='localhost:29092',
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    request_timeout_ms=30000,
                    acks='all'
                )

                await self.producer.start()
                await self.consumer.start()
                
                self.running = True
                logger.info("✅ Worker initialized successfully")
                return
                
            except Exception as e:
                logger.error(f"Attempt {attempt + 1}/{max_retries} failed: {str(e)}")
                if attempt == max_retries - 1:
                    raise
                await asyncio.sleep(retry_delay)

    async def process_messages(self):
        try:
            logger.info("🔄 Worker started processing messages")
            
            async for message in self.consumer:
                if not self.running:
                    break

                try:
                    data = message.value
                    question_id = data['question_id']
                    chat_id = data['chat_id']
                    question = data['question']
                    response = await asyncio.to_thread(qa_chain.invoke, {"input": data["question"]})
                    await self.consumer.commit()  # Ручной коммит

                    logger.info(f"🔍 Processing question: {question_id}")
                    
                    try:
                        start_time = asyncio.get_event_loop().time()
                        
                        # Используем asyncio.to_thread для синхронных вызовов
                        response = await asyncio.to_thread(
                            qa_chain.invoke,
                            {"input": question, "context": ""}
                        )
                        
                        answer = refine_answer(response["answer"])
                        processing_time = asyncio.get_event_loop().time() - start_time
                        logger.info(f"✅ Question {question_id} processed in {processing_time:.2f} sec")
                        
                        # Отправка ответа
                        await self.producer.send(
                            'telegram_responses',
                            value={
                                'question_id': question_id,
                                'chat_id': chat_id,
                                'answer': answer,
                                'timestamp': int(asyncio.get_event_loop().time())
                            }
                        )
                        
                    except Exception as e:
                        logger.error(f" Processing failed for {question_id}: {str(e)}")
                        await self.producer.send(
                            'telegram_responses',
                            value={
                                'question_id': question_id,
                                'chat_id': chat_id,
                                'answer': "⚠️ Произошла ошибка при обработке вашего вопроса.",
                                'timestamp': int(asyncio.get_event_loop().time())
                            }
                        )
                        
                except Exception as e:
                    logger.error(f"Error processing message: {str(e)}", exc_info=True)
                    continue
                    
        except Exception as e:
            logger.error(f" Fatal worker error: {str(e)}", exc_info=True)
        finally:
            await self.shutdown()

    async def shutdown(self):
        logger.info(" Shutting down worker...")
        self.running = False
        
        try:
            if self.consumer:
                await self.consumer.stop()
        except Exception as e:
            logger.error(f"Error closing consumer: {str(e)}")
            
        try:
            if self.producer:
                await self.producer.stop()
        except Exception as e:
            logger.error(f"Error closing producer: {str(e)}")
            
        logger.info("Worker shutdown complete")

async def main():
    worker = KafkaWorker()
    try:
        await worker.initialize()
        await worker.process_messages()
    except KeyboardInterrupt:
        await worker.shutdown()
    except Exception as e:
        logger.error(f" Worker crashed: {str(e)}", exc_info=True)
        await worker.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
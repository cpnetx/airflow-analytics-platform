"""Embedding generation tasks for semantic search."""

import numpy as np
from sqlalchemy import create_engine, text
from typing import List, Dict, Any
import logging
import openai
from tenacity import retry, stop_after_attempt, wait_exponential
import os

logger = logging.getLogger(__name__)


class EmbeddingGenerator:
    """Generate embeddings for maintenance documents."""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        openai.api_key = api_key
        self.model = "text-embedding-3-small"
        self.dimension = 1536
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def generate_embedding(self, text: str) -> List[float]:
        """Generate embedding for a single text."""
        try:
            response = openai.embeddings.create(
                model=self.model,
                input=text
            )
            return response.data[0].embedding
        except Exception as e:
            logger.error(f"Error generating embedding: {str(e)}")
            # Return zero vector as fallback
            return [0.0] * self.dimension
    
    def generate_embeddings_batch(self, texts: List[str], batch_size: int = 100) -> List[List[float]]:
        """Generate embeddings for multiple texts in batches."""
        embeddings = []
        
        for i in range(0, len(texts), batch_size):
            batch = texts[i:i + batch_size]
            try:
                response = openai.embeddings.create(
                    model=self.model,
                    input=batch
                )
                batch_embeddings = [item.embedding for item in response.data]
                embeddings.extend(batch_embeddings)
            except Exception as e:
                logger.error(f"Error generating batch embeddings: {str(e)}")
                # Add zero vectors for failed batch
                embeddings.extend([[0.0] * self.dimension] * len(batch))
        
        return embeddings


def generate_notification_embeddings(**context) -> Dict[str, Any]:
    """Generate embeddings for notification descriptions."""
    conf = context['dag_run'].conf
    org_id = conf['org_id']
    
    # Get OpenAI API key
    api_key = context['var']['value'].get('OPENAI_API_KEY')
    if not api_key:
        logger.error("OpenAI API key not found in Airflow variables")
        return {"status": "skipped", "reason": "No API key"}
    
    # Connect to database
    db_url = context['var']['value'].get('MAINTENANCE_DB_URL')
    engine = create_engine(db_url)
    
    with engine.connect() as conn:
        # Set search path
        conn.execute(text(f"SET search_path TO maint_{org_id}, public"))
        
        # Check if notification table exists and has data
        result = conn.execute(text("""
            SELECT COUNT(*) as count 
            FROM information_schema.tables 
            WHERE table_schema = :schema AND table_name = 'notification'
        """), {"schema": f"maint_{org_id}"})
        
        if result.fetchone()['count'] == 0:
            logger.warning("Notification table not found, skipping embeddings")
            return {"status": "skipped", "reason": "No notification table"}
        
        # Create embedding table
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS notification_embedding (
                notif_id VARCHAR(50) PRIMARY KEY,
                embedding vector(1536) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """))
        
        # Create index for vector similarity search
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_notif_embedding_vector 
            ON notification_embedding 
            USING ivfflat (embedding vector_cosine_ops)
            WITH (lists = 100)
        """))
        
        # Get notifications without embeddings
        result = conn.execute(text("""
            SELECT n.notif_id, n.description, n.notification_type, n.equipment_id
            FROM notification n
            LEFT JOIN notification_embedding e ON n.notif_id = e.notif_id
            WHERE e.notif_id IS NULL
            AND n.description IS NOT NULL
            LIMIT 1000
        """))
        
        notifications = list(result)
        
        if not notifications:
            logger.info("No notifications need embeddings")
            return {"status": "success", "embeddings_created": 0}
        
        # Generate embeddings
        logger.info(f"Generating embeddings for {len(notifications)} notifications")
        generator = EmbeddingGenerator(api_key)
        
        # Prepare texts for embedding
        texts = []
        for notif in notifications:
            # Combine relevant fields for richer embeddings
            text_parts = [notif['description']]
            if notif['notification_type']:
                text_parts.append(f"Type: {notif['notification_type']}")
            if notif['equipment_id']:
                text_parts.append(f"Equipment: {notif['equipment_id']}")
            
            texts.append(" ".join(text_parts))
        
        # Generate embeddings in batches
        embeddings = generator.generate_embeddings_batch(texts)
        
        # Insert embeddings
        inserted = 0
        for notif, embedding in zip(notifications, embeddings):
            if sum(embedding) != 0:  # Skip zero vectors
                embedding_str = f"[{','.join(map(str, embedding))}]"
                conn.execute(text("""
                    INSERT INTO notification_embedding (notif_id, embedding)
                    VALUES (:notif_id, :embedding::vector)
                    ON CONFLICT (notif_id) DO UPDATE
                    SET embedding = EXCLUDED.embedding
                """), {"notif_id": notif['notif_id'], "embedding": embedding_str})
                inserted += 1
        
        conn.commit()
    
    logger.info(f"Created {inserted} embeddings")
    
    return {
        "status": "success",
        "embeddings_created": inserted,
        "total_notifications": len(notifications)
    }
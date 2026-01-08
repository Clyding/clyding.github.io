import requests
import os
from src.connection import mongodb_connection, postgre_connection
import hashlib
from datetime import datetime
import time

def generate_document_id(userQuery, answer):
    combined = f"{userQuery[:10]}-{answer[:10]}"
    hash_object = hashlib.md5(combined.encode())
    hash_hex = hash_object.hexdigest()
    document_id = hash_hex[:8]
    return document_id

def query(payload):
    API_URL = "https://api-inference.huggingface.co/models/google-bert/bert-large-uncased-whole-word-masking-finetuned-squad"
    headers = {"Authorization": f"Bearer {os.getenv('HUGGINGFACE_KEY')}"}

    start_time = time.time()
    response = requests.post(API_URL, headers=headers, json=payload)
    end_time = time.time()

    responseTime = round(end_time - start_time, 2)
    return response.json(), responseTime

def captureUserInput(docId,userQuery, result, llmScore, responseTime, hit_rate, mrr):
    conn, cur = postgre_connection()
    try:
        create = """
            CREATE TABLE evaluation_data (

            id SERIAL PRIMARY KEY,
            doc_id VARCHAR(10) NOT NULL,
            user_input TEXT NOT NULL,
            result TEXT NOT NULL,
            llm_score DOUBLE PRECISION NOT NULL,
            response_time DOUBLE PRECISION NOT NULL,
            hit_rate_score DOUBLE PRECISION NOT NULL,
            mrr_score DOUBLE PRECISION NOT NULL,
            created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        cur.execute(create)
        
    except Exception as e:
        print(e)
        conn.rollback() 

    try:
        sql = f"""
            INSERT INTO evaluation_data
            (doc_id, user_input, result, llm_score, response_time, hit_rate_score, mrr_score)
            VALUES
            ('{docId}', '{userQuery}', '{result}', {llmScore}, {responseTime}, {hit_rate}, {mrr})
        """
        cur.execute(sql)
        
    except Exception as e:
        print(e)
        conn.rollback() 

    conn.commit()
    cur.close()
    conn.close()
    return "Insert Evaluation Data"

def captureUserFeedback(docId, userQuery, result, feedback):

    conn, cur = postgre_connection()
    try:
        create = """
            CREATE TABLE feedback_data (

            id SERIAL PRIMARY KEY,
            doc_id VARCHAR(10) NOT NULL,
            user_input TEXT NOT NULL,
            result TEXT NOT NULL,
            is_satisfied BOOLEAN NOT NULL,
            created_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        cur.execute(create)
    except Exception as e:
        print(e)
        conn.rollback() 

    try:
        sql = f"""
            INSERT INTO feedback_data
            (doc_id, user_input, result, is_satisfied)
            VALUES
            ('{docId}', '{userQuery}', '{result}', {feedback})
        """
        cur.execute(sql)
        
    except Exception as e:
        print(e)
        conn.rollback() 

    conn.commit()
    cur.close()
    conn.close()

    return "Insert Feedback Data"
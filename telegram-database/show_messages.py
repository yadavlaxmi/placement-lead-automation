#!/usr/bin/env python3
"""
Script to display messages fetched from Telegram
Usage: python3 show_messages.py [--limit N] [--job-only] [--group GROUP_NAME]
"""

import sqlite3
import argparse
import sys

def show_messages(limit=20, job_only=False, group_name=None):
    """Display messages from the Telegram database"""
    
    try:
        conn = sqlite3.connect('telegram_jobs.db')
        cursor = conn.cursor()
        
        # Build query based on parameters
        base_query = '''
            SELECT 
                m.id,
                m.message_id,
                m.sender_name,
                m.message_text,
                m.timestamp,
                m.is_job_post,
                m.job_score,
                pg.group_name,
                pg.group_link,
                c.name as city_name,
                js.overall_score,
                js.tags
            FROM messages m
            JOIN programming_groups pg ON m.group_id = pg.id
            LEFT JOIN cities c ON pg.city_id = c.id
            LEFT JOIN job_scores js ON m.id = js.message_id
        '''
        
        where_conditions = []
        params = []
        
        if job_only:
            where_conditions.append("js.overall_score > 0")
        
        if group_name:
            where_conditions.append("pg.group_name LIKE ?")
            params.append(f"%{group_name}%")
        
        if where_conditions:
            base_query += " WHERE " + " AND ".join(where_conditions)
        
        base_query += " ORDER BY m.timestamp DESC LIMIT ?"
        params.append(limit)
        
        cursor.execute(base_query, params)
        messages = cursor.fetchall()
        
        print('=== TELEGRAM MESSAGES FETCHED ===')
        print(f'Total messages found: {len(messages)}')
        if job_only:
            print('(Showing only job-related messages)')
        if group_name:
            print(f'(Filtered by group: {group_name})')
        print('=' * 50)
        
        for i, msg in enumerate(messages, 1):
            (msg_id, telegram_msg_id, sender, text, timestamp, is_job, 
             job_score, group_name_val, group_link, city, overall_score, tags) = msg
            
            print(f'\n[{i}] Message ID: {msg_id}')
            print(f'    Telegram Message ID: {telegram_msg_id}')
            print(f'    Group: {group_name_val}')
            print(f'    City: {city or "Unknown"}')
            print(f'    Sender: {sender or "Unknown"}')
            print(f'    Timestamp: {timestamp or "Unknown"}')
            print(f'    Is Job Post: {is_job}')
            print(f'    Job Score: {job_score or 0.0}')
            if overall_score:
                print(f'    ML Overall Score: {overall_score}')
            if tags:
                print(f'    ML Tags: {tags}')
            print(f'    Group Link: {group_link}')
            text_preview = text[:200] + '...' if len(text) > 200 else text
            print(f'    Message Text: {text_preview}')
            print('-' * 50)
        
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Display Telegram messages')
    parser.add_argument('--limit', type=int, default=20, help='Number of messages to show (default: 20)')
    parser.add_argument('--job-only', action='store_true', help='Show only job-related messages')
    parser.add_argument('--group', type=str, help='Filter by group name')
    
    args = parser.parse_args()
    show_messages(args.limit, args.job_only, args.group)

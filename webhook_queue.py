"""
Webhook queueing system for WalletBud.
Handles webhook processing with retries, rate limiting, and error handling.
"""

import logging
import asyncio
import json
import time
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from dataclasses import dataclass
from collections import deque

from config import (
    RATE_LIMIT_WINDOW,
    RATE_LIMIT_MAX_REQUESTS,
    MAX_QUEUE_SIZE,
    MAX_RETRIES,
    MAX_EVENT_AGE,
    BATCH_SIZE,
    MAX_WEBHOOK_SIZE
)

logger = logging.getLogger(__name__)

@dataclass
class WebhookEvent:
    """Represents a webhook event in the queue"""
    id: str
    event_type: str
    payload: Dict[str, Any]
    headers: Dict[str, str]
    created_at: datetime
    retries: int = 0
    last_retry: Optional[datetime] = None
    error: Optional[str] = None
    
    @property
    def age_seconds(self) -> float:
        """Get age of event in seconds"""
        return (datetime.now() - self.created_at).total_seconds()
    
    @property
    def should_retry(self) -> bool:
        """Check if event should be retried based on age and retry count"""
        if self.retries >= MAX_RETRIES:
            return False
        
        if self.age_seconds > MAX_EVENT_AGE:
            return False
            
        if self.last_retry:
            # Exponential backoff: wait 2^retries seconds
            wait_time = 2 ** self.retries
            time_since_retry = (datetime.now() - self.last_retry).total_seconds()
            return time_since_retry >= wait_time
            
        return True

class RateLimiter:
    """Rate limiter for webhook requests"""
    def __init__(self, window: int = RATE_LIMIT_WINDOW, max_requests: int = RATE_LIMIT_MAX_REQUESTS):
        self.window = window
        self.max_requests = max_requests
        self.requests: Dict[str, List[datetime]] = {}
        self.lock = asyncio.Lock()
        
    async def is_allowed(self, ip: str) -> bool:
        """Check if request is allowed for IP
        
        Args:
            ip (str): IP address
            
        Returns:
            bool: True if allowed, False if rate limited
        """
        async with self.lock:
            now = datetime.now()
            
            # Clean up old IPs periodically
            if len(self.requests) > 1000:  # Arbitrary limit
                cutoff = now - timedelta(seconds=self.window * 2)
                self.requests = {
                    ip: times for ip, times in self.requests.items()
                    if any(t > cutoff for t in times)
                }
            
            if ip not in self.requests:
                self.requests[ip] = []
                
            # Remove old requests
            cutoff = now - timedelta(seconds=self.window)
            self.requests[ip] = [t for t in self.requests[ip] if t > cutoff]
            
            # Check rate limit
            if len(self.requests[ip]) >= self.max_requests:
                return False
                
            # Add new request
            self.requests[ip].append(now)
            return True
        
    def cleanup(self):
        """Remove old rate limit data"""
        now = datetime.now()
        cutoff = now - timedelta(seconds=self.window)
        for ip in list(self.requests.keys()):
            self.requests[ip] = [t for t in self.requests[ip] if t > cutoff]
            if not self.requests[ip]:
                del self.requests[ip]

class WebhookQueue:
    """Queue for processing webhooks with rate limiting and retries"""
    def __init__(self):
        """Initialize the webhook queue"""
        self.queue = deque(maxlen=MAX_QUEUE_SIZE)
        self.processing = False
        self.process_lock = asyncio.Lock()
        self.queue_lock = asyncio.Lock()  # Separate lock for queue operations
        self.rate_limiter = RateLimiter()
        self.errors: deque[QueueError] = deque(maxlen=1000)
        self.stats = {
            'total_received': 0,
            'total_processed': 0,
            'total_failed': 0,
            'total_retried': 0,
            'total_oversized': 0,
            'total_invalid': 0,
            'total_rate_limited': 0,
            'current_queue_size': 0,
            'last_process_time': None
        }
        
    def _validate_event(self, event_type: str, payload: Dict[str, Any]) -> bool:
        """Validate event payload
        
        Args:
            event_type (str): Type of event
            payload (Dict[str, Any]): Event payload
            
        Returns:
            bool: True if valid, False otherwise
        """
        try:
            # Check payload size
            payload_size = len(json.dumps(payload).encode('utf-8'))
            if payload_size > MAX_WEBHOOK_SIZE:  # 1MB
                logger.error(f"Event payload too large: {payload_size} bytes")
                self.stats['total_oversized'] += 1
                return False
            
            # Validate required fields
            if event_type == 'transaction':
                required = ['addresses', 'tx_hash', 'block_height']
            elif event_type == 'delegation':
                required = ['stake_address', 'pool_id', 'block_height']
            else:
                logger.error(f"Invalid event type: {event_type}")
                self.stats['total_invalid'] += 1
                return False
            
            # Check all required fields exist
            if not all(field in payload for field in required):
                logger.error(f"Missing required fields in {event_type} event")
                self.stats['total_invalid'] += 1
                return False
            
            # Validate field types
            if event_type == 'transaction':
                if not isinstance(payload['addresses'], list):
                    logger.error("addresses must be a list")
                    self.stats['total_invalid'] += 1
                    return False
                if not isinstance(payload['tx_hash'], str):
                    logger.error("tx_hash must be a string")
                    self.stats['total_invalid'] += 1
                    return False
                if not isinstance(payload['block_height'], int):
                    logger.error("block_height must be an integer")
                    self.stats['total_invalid'] += 1
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error validating event: {str(e)}")
            self.stats['total_invalid'] += 1
            return False
        
    def _add_error(self, error_type: str, message: str, event_id: Optional[str] = None, 
                  details: Optional[Dict[str, Any]] = None):
        """Add error to history
        
        Args:
            error_type (str): Type of error
            message (str): Error message
            event_id (Optional[str]): Associated event ID
            details (Optional[Dict[str, Any]]): Additional error details
        """
        error = QueueError(
            timestamp=datetime.now(),
            error_type=error_type,
            event_id=event_id,
            message=message,
            details=details
        )
        self.errors.append(error)
        logger.error(f"{error_type}: {message}")
        
    async def add_event(self, event_id: str, event_type: str, 
                       payload: Dict[str, Any], headers: Dict[str, str]) -> bool:
        """Add new event to queue
        
        Args:
            event_id (str): Unique event ID
            event_type (str): Type of event (transaction/delegation)
            payload (Dict[str, Any]): Event payload
            headers (Dict[str, str]): Request headers
            
        Returns:
            bool: True if added successfully, False if queue full
        """
        async with self.queue_lock:
            if len(self.queue) >= MAX_QUEUE_SIZE:
                self._add_error(
                    'QUEUE_FULL',
                    "Queue is full, event rejected",
                    event_id
                )
                return False

            # Validate event
            if not self._validate_event(event_type, payload):
                self.stats['total_invalid'] += 1
                return False

            # Create event
            event = WebhookEvent(
                id=event_id,
                event_type=event_type,
                payload=payload,
                headers=headers,
                created_at=datetime.now()
            )

            # Add to queue
            self.queue.append(event)
            self.stats['total_received'] += 1
            self.stats['current_queue_size'] = len(self.queue)
            return True

    async def process_queue(self, handler):
        """Process events in queue
        
        Args:
            handler: Async function to handle events
        """
        if self.processing:
            return

        async with self.process_lock:
            if self.processing:
                return
                
            try:
                self.processing = True
                self.stats['last_process_time'] = datetime.now()
                
                while True:
                    batch = []
                    async with self.queue_lock:
                        # Get batch of events
                        while len(batch) < BATCH_SIZE and self.queue:
                            event = self.queue[0]
                            if not event.should_retry:
                                self.queue.popleft()
                                continue
                            batch.append(self.queue.popleft())
                    
                    if not batch:
                        break
                        
                    # Process batch
                    for event in batch:
                        try:
                            await handler(
                                event.event_type,
                                event.payload,
                                event.headers
                            )
                            self.stats['total_processed'] += 1
                            
                        except Exception as e:
                            event.retries += 1
                            event.last_retry = datetime.now()
                            event.error = str(e)
                            
                            if event.should_retry:
                                async with self.queue_lock:
                                    self.queue.append(event)
                                self.stats['total_retried'] += 1
                            else:
                                self.stats['total_failed'] += 1
                                self._add_error(
                                    'PROCESSING',
                                    f"Failed to process event after {event.retries} retries",
                                    event.id,
                                    {'error': str(e)}
                                )
                    
                    self.stats['current_queue_size'] = len(self.queue)
                    
            finally:
                self.processing = False
                
    async def clear_old_events(self) -> int:
        """Clear events older than MAX_EVENT_AGE
        
        Returns:
            int: Number of events cleared
        """
        async with self.queue_lock:
            now = datetime.now()
            original_size = len(self.queue)
            
            # Use list comprehension for efficiency
            self.queue = deque(
                [event for event in self.queue 
                 if event.age_seconds <= MAX_EVENT_AGE],
                maxlen=MAX_QUEUE_SIZE
            )
            
            cleared = original_size - len(self.queue)
            if cleared > 0:
                logger.info(f"Cleared {cleared} old events from queue")
                self.stats['current_queue_size'] = len(self.queue)
            
            return cleared

    def get_stats(self) -> Dict[str, Any]:
        """Get queue statistics
        
        Returns:
            Dict[str, Any]: Queue statistics
        """
        recent_errors = [
            {
                'type': e.error_type,
                'message': e.message,
                'timestamp': e.timestamp.isoformat(),
                'event_id': e.event_id,
                'details': e.details
            }
            for e in list(self.errors)[-10:]  # Get last 10 errors
        ]
        
        # Get oldest and newest events efficiently
        oldest_event = None
        newest_event = None
        if self.queue:
            try:
                oldest_event = self.queue[0].created_at
                newest_event = self.queue[-1].created_at
            except IndexError:
                # Queue was modified while we were reading it
                pass
        
        return {
            'queue_size': len(self.queue),
            'is_processing': self.processing,
            'stats': self.stats,
            'oldest_event': oldest_event,
            'newest_event': newest_event,
            'recent_errors': recent_errors
        }

@dataclass
class QueueError:
    """Represents an error in the queue"""
    timestamp: datetime
    error_type: str
    event_id: Optional[str]
    message: str
    details: Optional[Dict[str, Any]] = None

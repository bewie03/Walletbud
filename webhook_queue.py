"""
Webhook queueing system for WalletBud.
Handles webhook processing with retries, rate limiting, and error handling.
"""

import logging
import asyncio
import json
import time
import os
import hmac
import hashlib
import re
from typing import Dict, Any, Optional, List, Callable
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
    WEBHOOK_SECRET
)

# Get port from Heroku environment, default to 8080 for local development
PORT = int(os.environ.get('PORT', 8080))

logger = logging.getLogger(__name__)

def validate_webhook_secret(secret: str) -> bool:
    """Validate webhook secret
    
    Args:
        secret: Webhook secret to validate
        
    Returns:
        bool: True if secret is valid
    """
    if not secret:
        logger.warning("Webhook secret is empty")
        return False
        
    if len(secret) < 32:
        logger.warning("Webhook secret is too short (min 32 chars)")
        return False
        
    if not re.match(r'^[a-zA-Z0-9-_]+$', secret):
        logger.warning("Webhook secret contains invalid characters")
        return False
        
    return True

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
            
        # Implement exponential backoff
        if self.last_retry:
            backoff = min(300, 2 ** self.retries)  # Cap at 5 minutes
            next_retry = self.last_retry + timedelta(seconds=backoff)
            if datetime.now() < next_retry:
                return False
                
        return True
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert event to dictionary for storage"""
        return {
            'id': self.id,
            'event_type': self.event_type,
            'payload': self.payload,
            'headers': self.headers,
            'created_at': self.created_at.isoformat(),
            'retries': self.retries,
            'last_retry': self.last_retry.isoformat() if self.last_retry else None,
            'error': self.error
        }
        
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'WebhookEvent':
        """Create event from dictionary"""
        return cls(
            id=data['id'],
            event_type=data['event_type'],
            payload=data['payload'],
            headers=data['headers'],
            created_at=datetime.fromisoformat(data['created_at']),
            retries=data['retries'],
            last_retry=datetime.fromisoformat(data['last_retry']) if data['last_retry'] else None,
            error=data['error']
        )

class RateLimiter:
    """Rate limiter for webhook requests with burst support"""
    def __init__(self, window: int = 60, max_requests: int = 100):
        self.window = window
        self.max_requests = max_requests
        self.requests: Dict[str, List[datetime]] = {}
        self.lock = asyncio.Lock()
        self.last_cleanup = datetime.now()
        
    async def is_allowed(self, ip: str) -> bool:
        """Check if request is allowed for IP with burst handling"""
        async with self.lock:
            now = datetime.now()
            
            # Cleanup old entries periodically
            if (now - self.last_cleanup).total_seconds() > 60:
                await self.cleanup()
                self.last_cleanup = now
                
            # Initialize if IP not seen before
            if ip not in self.requests:
                self.requests[ip] = []
                
            # Remove old requests outside window
            window_start = now - timedelta(seconds=self.window)
            self.requests[ip] = [ts for ts in self.requests[ip] if ts > window_start]
            
            # Check rate limit with burst allowance
            current_requests = len(self.requests[ip])
            if current_requests >= self.max_requests:
                # Allow burst if no requests in last second
                if current_requests < self.max_requests * 2:
                    last_request = self.requests[ip][-1]
                    if (now - last_request).total_seconds() > 1:
                        self.requests[ip].append(now)
                        return True
                return False
                
            self.requests[ip].append(now)
            return True
            
    async def cleanup(self):
        """Remove old rate limit data"""
        async with self.lock:
            now = datetime.now()
            window_start = now - timedelta(seconds=self.window)
            
            # Clean up old requests and remove empty IPs
            for ip in list(self.requests.keys()):
                self.requests[ip] = [ts for ts in self.requests[ip] if ts > window_start]
                if not self.requests[ip]:
                    del self.requests[ip]

class WebhookQueue:
    """Queue for processing webhooks with rate limiting and retries"""
    
    def __init__(self, secret: Optional[str] = None):
        """Initialize webhook queue
        
        Args:
            secret: Optional webhook secret for validation
        """
        self.secret = secret
        self.queue = deque(maxlen=1000)
        self.processing = False
        self.process_lock = asyncio.Lock()
        self.queue_lock = asyncio.Lock()
        self.rate_limiter = RateLimiter()
        self.errors = deque(maxlen=1000)
        self.stats = {
            'total_received': 0,
            'total_processed': 0,
            'total_failed': 0,
            'total_retried': 0,
            'total_oversized': 0,
            'total_invalid': 0,
            'total_rate_limited': 0,
            'current_queue_size': 0,
            'last_process_time': None,
            'processing_time_avg': 0.0,
            'retry_success_rate': 0.0
        }
        self.event_handlers = {}
        self._shutdown = False
        self._cleanup_task = None
        
    async def start(self):
        """Start the webhook queue processor"""
        self._shutdown = False
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())
        await self.process_queue()
        
    async def stop(self):
        """Stop the webhook queue processor"""
        self._shutdown = True
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
        
    async def _cleanup_loop(self):
        """Periodically clean up old events"""
        while not self._shutdown:
            try:
                await self.clear_old_events()
                await asyncio.sleep(60)  # Run cleanup every minute
            except Exception as e:
                logger.error(f"Error in cleanup loop: {e}")
                await asyncio.sleep(5)  # Short delay on error
                
    async def add_event(self, event_id: str, event_type: str,
                       payload: Dict[str, Any], headers: Dict[str, str]) -> bool:
        """Add new event to queue with validation"""
        try:
            # Validate event size
            event_size = len(json.dumps(payload))
            if event_size > 100000:
                self._add_error('oversized_payload', 
                              f'Payload size {event_size} exceeds limit 100000',
                              event_id)
                self.stats['total_oversized'] += 1
                return False
            
            # Validate event type and payload
            if not self._validate_event(event_type, payload):
                self._add_error('invalid_payload', 
                              'Invalid event payload',
                              event_id)
                self.stats['total_invalid'] += 1
                return False
            
            # Check queue capacity
            async with self.queue_lock:
                if len(self.queue) >= 1000:
                    self._add_error('queue_full',
                                  f'Queue full ({1000} events)',
                                  event_id)
                    return False
                
                # Add event to queue
                event = WebhookEvent(
                    id=event_id,
                    event_type=event_type,
                    payload=payload,
                    headers=headers,
                    created_at=datetime.now()
                )
                self.queue.append(event)
                self.stats['total_received'] += 1
                self.stats['current_queue_size'] = len(self.queue)
                
                # Start processing if not already running
                if not self.processing:
                    asyncio.create_task(self.process_queue())
                
                return True
                
        except Exception as e:
            logger.error(f"Error adding event {event_id}: {e}")
            self._add_error('add_event_error',
                          f'Error adding event: {str(e)}',
                          event_id)
            return False
            
    async def process_queue(self):
        """Process events in queue with batching and error handling"""
        if self.processing:
            return
            
        async with self.process_lock:
            try:
                self.processing = True
                while self.queue and not self._shutdown:
                    # Process events in batches
                    batch = []
                    async with self.queue_lock:
                        while len(batch) < 100 and self.queue:
                            event = self.queue.popleft()
                            if event.age_seconds > 3600:
                                self._add_error('event_expired',
                                              f'Event expired after {event.age_seconds}s',
                                              event.id)
                                continue
                            batch.append(event)
                            
                    # Process batch
                    for event in batch:
                        try:
                            # Check rate limit
                            if not await self.rate_limiter.is_allowed(event.headers.get('X-Forwarded-For', 'unknown')):
                                self.queue.append(event)  # Re-queue for later
                                self.stats['total_rate_limited'] += 1
                                continue
                                
                            # Process event
                            handler = self.event_handlers.get(event.event_type)
                            if handler:
                                start_time = time.time()
                                await handler(event.payload)
                                process_time = time.time() - start_time
                                
                                # Update stats
                                self.stats['total_processed'] += 1
                                self.stats['last_process_time'] = process_time
                                self.stats['processing_time_avg'] = (
                                    (self.stats['processing_time_avg'] * 
                                     (self.stats['total_processed'] - 1) +
                                     process_time) / self.stats['total_processed']
                                )
                            else:
                                self._add_error('no_handler',
                                              f'No handler for event type: {event.event_type}',
                                              event.id)
                                
                        except Exception as e:
                            logger.error(f"Error processing event {event.id}: {e}")
                            if event.retries < 5:
                                event.retries += 1
                                event.last_retry = datetime.now()
                                event.error = str(e)
                                self.queue.append(event)  # Retry later
                                self.stats['total_retried'] += 1
                            else:
                                self._add_error('max_retries',
                                              f'Max retries ({5}) exceeded',
                                              event.id,
                                              {'error': str(e)})
                                self.stats['total_failed'] += 1
                                
                    # Update queue size
                    self.stats['current_queue_size'] = len(self.queue)
                    
                    # Small delay between batches
                    await asyncio.sleep(0.1)
                    
            finally:
                self.processing = False

    def register_handler(self, event_type: str, handler: Callable):
        """Register handler for event type"""
        self.event_handlers[event_type] = handler
        
    def validate_signature(self, signature: str, payload: str) -> bool:
        """Validate webhook signature
        
        Args:
            signature: Webhook signature from request
            payload: Raw request payload
            
        Returns:
            bool: True if signature is valid
        """
        if not self.secret:
            logger.warning("No webhook secret configured - skipping signature validation")
            return True
            
        if not validate_webhook_secret(self.secret):
            logger.error("Invalid webhook secret configuration")
            return False
            
        try:
            expected = hmac.new(
                self.secret.encode(),
                payload.encode(),
                hashlib.sha256
            ).hexdigest()
            return hmac.compare_digest(signature, expected)
        except Exception as e:
            logger.error(f"Error validating signature: {e}")
            return False
            
    async def _validate_event(self, event_type: str, payload: Dict[str, Any]) -> bool:
        """Validate event payload with comprehensive checks"""
        try:
            # Check payload size
            payload_size = len(json.dumps(payload))
            if payload_size > 100000:
                self._add_error('oversized_payload', f'Payload size {payload_size} exceeds limit')
                return False
                
            # Basic structure validation
            required_fields = {'address', 'timestamp', 'data'}
            if not all(field in payload for field in required_fields):
                self._add_error('missing_fields', 'Missing required fields')
                return False
                
            # Validate timestamp
            try:
                event_time = datetime.fromisoformat(payload['timestamp'])
                if abs((datetime.now() - event_time).total_seconds()) > 3600:
                    self._add_error('invalid_timestamp', 'Event timestamp too old or future')
                    return False
            except ValueError:
                self._add_error('invalid_timestamp', 'Invalid timestamp format')
                return False
                
            # Validate address format
            if not payload['address'].startswith(('addr1', 'stake1')):
                self._add_error('invalid_address', 'Invalid address format')
                return False
                
            # Event-specific validation
            if event_type == 'transaction':
                if 'tx_hash' not in payload['data']:
                    self._add_error('missing_tx_hash', 'Transaction missing tx_hash')
                    return False
            elif event_type == 'delegation':
                if 'pool_id' not in payload['data']:
                    self._add_error('missing_pool_id', 'Delegation missing pool_id')
                    return False
                    
            return True
            
        except Exception as e:
            self._add_error('validation_error', str(e))
            return False
            
    def _add_error(self, error_type: str, message: str, event_id: Optional[str] = None,
                   details: Optional[Dict[str, Any]] = None):
        """Add error to history with metadata"""
        error = QueueError(
            timestamp=datetime.now(),
            error_type=error_type,
            event_id=event_id,
            message=message,
            details=details
        )
        self.errors.append(error)
        logger.error(f"Queue error: {error_type} - {message}")
        
    async def clear_old_events(self) -> int:
        """Clear events older than 3600 seconds"""
        cleared = 0
        async with self.queue_lock:
            current_time = datetime.now()
            remaining_events = deque()
            
            while self.queue:
                event = self.queue.popleft()
                if event.age_seconds <= 3600:
                    remaining_events.append(event)
                else:
                    cleared += 1
                    self._add_error('event_expired',
                                  f'Event {event.id} expired',
                                  event.id)
                    
            self.queue = remaining_events
            self.stats['current_queue_size'] = len(self.queue)
            
        return cleared
        
    def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive queue statistics"""
        stats = self.stats.copy()
        stats.update({
            'error_count': len(self.errors),
            'recent_errors': [
                {
                    'type': e.error_type,
                    'message': e.message,
                    'timestamp': e.timestamp.isoformat()
                }
                for e in list(self.errors)[-5:]  # Last 5 errors
            ]
        })
        return stats

@dataclass
class QueueError:
    """Represents an error in the queue"""
    timestamp: datetime
    error_type: str
    event_id: Optional[str]
    message: str
    details: Optional[Dict[str, Any]] = None

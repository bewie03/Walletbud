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
import sys

from config import (
    WEBHOOK_CONFIG,
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
    
    def age_seconds(self) -> float:
        """Get age of event in seconds"""
        return (datetime.now() - self.created_at).total_seconds()
        
    def should_retry(self) -> bool:
        """Check if event should be retried based on age and retry count"""
        # Check if max retries exceeded
        if self.retries >= WEBHOOK_CONFIG['MAX_RETRIES']:
            return False
            
        # Check if event is too old
        if self.age_seconds() > WEBHOOK_CONFIG['MAX_EVENT_AGE']:
            return False
            
        # Check retry delay
        if self.last_retry:
            time_since_retry = (datetime.now() - self.last_retry).total_seconds()
            if time_since_retry < WEBHOOK_CONFIG['RETRY_DELAY']:
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
            retries=data.get('retries', 0),
            last_retry=datetime.fromisoformat(data['last_retry']) if data.get('last_retry') else None,
            error=data.get('error')
        )

class RateLimiter:
    """Rate limiter for webhook requests with burst support"""
    
    def __init__(self, window: int = 60, max_requests: int = 100):
        self.window = window
        self.max_requests = max_requests
        self._requests: Dict[str, List[datetime]] = {}
        self.lock = asyncio.Lock()
        self.last_cleanup = datetime.now()
        self._total_memory = 0  # Track memory usage
        
    async def is_allowed(self, ip: str) -> bool:
        """Check if request is allowed for IP with burst handling"""
        async with self.lock:
            now = datetime.now()
            
            # Clean up old entries if needed
            await self.cleanup()
            
            # Get or initialize request history
            if ip not in self._requests:
                self._requests[ip] = []
                
            # Remove requests outside window
            window_start = now - timedelta(seconds=self.window)
            self._requests[ip] = [
                ts for ts in self._requests[ip]
                if ts > window_start
            ]
            
            # Check if under limit
            if len(self._requests[ip]) < self.max_requests:
                self._requests[ip].append(now)
                return True
                
            return False
            
    async def cleanup(self, force: bool = False) -> None:
        """Remove old rate limit data"""
        now = datetime.now()
        
        # Only clean up every minute unless forced
        if not force and (now - self.last_cleanup).total_seconds() < 60:
            return
            
        self.last_cleanup = now
        window_start = now - timedelta(seconds=self.window)
        
        # Remove old timestamps and empty IPs
        async with self.lock:
            for ip in list(self._requests.keys()):
                self._requests[ip] = [
                    ts for ts in self._requests[ip]
                    if ts > window_start
                ]
                if not self._requests[ip]:
                    del self._requests[ip]
                    
            # Update memory usage estimate
            self._update_memory_usage()
            
    def _update_memory_usage(self) -> None:
        """Update memory usage estimate"""
        try:
            # Rough estimate: 
            # - Each IP is ~32 bytes
            # - Each timestamp is ~24 bytes
            # - Dict overhead ~48 bytes per entry
            memory = 0
            for ip, timestamps in self._requests.items():
                memory += 32  # IP string
                memory += 24 * len(timestamps)  # Timestamps
                memory += 48  # Dict entry overhead
                
            self._total_memory = memory
            
            # Log warning if memory usage is high
            memory_mb = memory / (1024 * 1024)
            if memory_mb > 100:  # Warning at 100MB
                logger.warning(f"Rate limiter memory usage high: {memory_mb:.2f}MB")
                
        except Exception as e:
            logger.error(f"Error updating memory usage: {e}")
            
class WebhookQueue:
    """Queue for processing webhooks with rate limiting and retries"""
    
    def __init__(self, secret: Optional[str] = None, max_queue_size: int = None,
                 batch_size: int = None, max_retries: int = None, 
                 max_event_age: int = None, cleanup_interval: int = None):
        """Initialize webhook queue
        
        Args:
            secret: Optional webhook secret for validation
            max_queue_size: Maximum size of the queue
            batch_size: Number of events to process in a batch
            max_retries: Maximum number of retries per event
            max_event_age: Maximum age of events in seconds
            cleanup_interval: Interval for cleaning up old events
        """
        self.secret = secret
        self.queue = deque(maxlen=max_queue_size or WEBHOOK_CONFIG['MAX_QUEUE_SIZE'])
        self.processing = False
        self.process_lock = asyncio.Lock()
        self.queue_lock = asyncio.Lock()
        self.rate_limiter = RateLimiter(
            window=WEBHOOK_CONFIG['RATE_LIMIT_WINDOW'],
            max_requests=WEBHOOK_CONFIG['RATE_LIMIT_MAX_REQUESTS']
        )
        self.errors = deque(maxlen=max_queue_size or WEBHOOK_CONFIG['MAX_QUEUE_SIZE'])
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
            'retry_success_rate': 0.0,
            'memory_usage_mb': 0.0
        }
        self.event_handlers = {}
        self._shutdown = False
        self._cleanup_task = None
        self.batch_size = batch_size or WEBHOOK_CONFIG['BATCH_SIZE']
        self.max_retries = max_retries or WEBHOOK_CONFIG['MAX_RETRIES']
        self.max_event_age = max_event_age or WEBHOOK_CONFIG['MAX_EVENT_AGE']
        self.cleanup_interval = cleanup_interval or WEBHOOK_CONFIG['CLEANUP_INTERVAL']
        
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
                # Clean up old events
                await self.clear_old_events()
                
                # Update queue stats
                self.stats['current_queue_size'] = len(self.queue)
                
                # Monitor queue size
                if len(self.queue) > WEBHOOK_CONFIG['MAX_QUEUE_SIZE'] * 0.8:  # 80% full
                    logger.warning(f"Queue is at {len(self.queue)}/{WEBHOOK_CONFIG['MAX_QUEUE_SIZE']} capacity")
                
                # Calculate processing metrics
                if self.stats['total_retried'] > 0:
                    self.stats['retry_success_rate'] = (
                        (self.stats['total_processed'] - self.stats['total_failed']) /
                        self.stats['total_retried']
                    ) * 100
                
                # Log queue health metrics
                logger.info(
                    "Queue health metrics - "
                    f"Size: {len(self.queue)}/{WEBHOOK_CONFIG['MAX_QUEUE_SIZE']}, "
                    f"Success rate: {(self.stats['total_processed'] - self.stats['total_failed']) / max(1, self.stats['total_processed']) * 100:.1f}%, "
                    f"Retry success: {self.stats['retry_success_rate']:.1f}%"
                )
                
            except Exception as e:
                logger.error(f"Error in cleanup loop: {e}", exc_info=True)
            
            await asyncio.sleep(60)  # Run every minute

    async def clear_old_events(self):
        """Clear events older than MAX_EVENT_AGE seconds"""
        try:
            async with self.queue_lock:
                current_time = datetime.now()
                old_events = [
                    event for event in self.queue
                    if (current_time - event.created_at).total_seconds() > self.max_event_age
                ]
                
                for event in old_events:
                    self.queue.remove(event)
                    self._add_error(
                        'event_expired',
                        f'Event {event.id} expired after {self.max_event_age} seconds',
                        event.id,
                        {'age': (current_time - event.created_at).total_seconds()}
                    )
                
                if old_events:
                    logger.info(f"Cleared {len(old_events)} expired events from queue")
                
        except Exception as e:
            logger.error(f"Error clearing old events: {e}", exc_info=True)

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
                if len(self.queue) >= WEBHOOK_CONFIG['MAX_QUEUE_SIZE']:
                    self._add_error('queue_full',
                                  f'Queue full ({WEBHOOK_CONFIG["MAX_QUEUE_SIZE"]} events)',
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
            
    async def _check_memory_usage(self) -> float:
        """Check current memory usage of the queue
        
        Returns:
            float: Current memory usage in MB
        """
        try:
            import psutil
            process = psutil.Process()
            memory_info = process.memory_info()
            memory_mb = memory_info.rss / (1024 * 1024)  # Convert to MB
            self.stats['memory_usage_mb'] = memory_mb
            return memory_mb
        except Exception as e:
            logger.error(f"Error checking memory usage: {e}")
            return 0.0
            
    async def _enforce_memory_limit(self) -> None:
        """Enforce memory limits by removing old events if necessary"""
        try:
            memory_mb = await self._check_memory_usage()
            if memory_mb > WEBHOOK_CONFIG['MEMORY_LIMIT_MB']:
                logger.warning(f"Memory usage ({memory_mb:.2f}MB) exceeds limit ({WEBHOOK_CONFIG['MEMORY_LIMIT_MB']}MB)")
                
                # Remove oldest events until under limit
                async with self.queue_lock:
                    while memory_mb > WEBHOOK_CONFIG['MEMORY_LIMIT_MB'] and self.queue:
                        event = self.queue.popleft()
                        self._add_error(
                            'memory_limit',
                            f'Removed event {event.id} due to memory limit',
                            event.id,
                            {'memory_usage_mb': memory_mb}
                        )
                        memory_mb = await self._check_memory_usage()
                        
                logger.info(f"Memory usage after cleanup: {memory_mb:.2f}MB")
        except Exception as e:
            logger.error(f"Error enforcing memory limit: {e}")
            
    async def process_queue(self):
        """Process events in queue with batching and error handling"""
        if self._shutdown:
            return
            
        try:
            # Check and enforce memory limits
            await self._enforce_memory_limit()
            
            # Process events in batches
            async with self.process_lock:
                if self.processing:
                    return
                    
                self.processing = True
                start_time = time.time()
                
                try:
                    # Process events in batches
                    batch = []
                    async with self.queue_lock:
                        while len(batch) < self.batch_size and self.queue:
                            event = self.queue.popleft()
                            if event.should_retry():
                                batch.append(event)
                                
                    if not batch:
                        return
                        
                    # Process batch
                    for event in batch:
                        try:
                            handler = self.event_handlers.get(event.event_type)
                            if handler:
                                await handler(event.payload)
                                self.stats['total_processed'] += 1
                            else:
                                self._add_error(
                                    'no_handler',
                                    f'No handler for event type: {event.event_type}',
                                    event.id
                                )
                                self.stats['total_failed'] += 1
                        except Exception as e:
                            event.retries += 1
                            event.error = str(e)
                            event.last_retry = datetime.now()
                            
                            if event.retries < self.max_retries:
                                self.stats['total_retried'] += 1
                                async with self.queue_lock:
                                    self.queue.append(event)
                            else:
                                self._add_error(
                                    'max_retries',
                                    f'Max retries reached for event {event.id}',
                                    event.id,
                                    {'error': str(e)}
                                )
                                self.stats['total_failed'] += 1
                                
                finally:
                    self.processing = False
                    self.stats['last_process_time'] = time.time() - start_time
                    
        except Exception as e:
            logger.error(f"Error processing queue: {e}")
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
        
    async def get_stats(self) -> Dict[str, Any]:
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

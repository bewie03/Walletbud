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
from typing import Dict, Any, Optional, List, Callable, Tuple, Union
from datetime import datetime, timedelta
from dataclasses import dataclass
from collections import deque, defaultdict
import sys
from aiohttp import web
import uuid

from config import WEBHOOK_SECURITY, WEBHOOK_CONFIG

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

class IPRateLimiter:
    """Rate limiter with token bucket algorithm per IP"""
    
    def __init__(self, requests_per_second: int, burst: int):
        self.rate = requests_per_second
        self.burst = burst
        self.buckets: Dict[str, Dict[str, float]] = {}
        self.lock = asyncio.Lock()
        
    async def is_allowed(self, ip: str) -> bool:
        """Check if request is allowed using token bucket algorithm"""
        async with self.lock:
            now = time.time()
            
            if ip not in self.buckets:
                self.buckets[ip] = {
                    'tokens': self.burst,
                    'last_update': now
                }
            
            bucket = self.buckets[ip]
            time_passed = now - bucket['last_update']
            bucket['tokens'] = min(
                self.burst,
                bucket['tokens'] + time_passed * self.rate
            )
            
            if bucket['tokens'] < 1:
                return False
                
            bucket['tokens'] -= 1
            bucket['last_update'] = now
            return True
            
    async def cleanup(self):
        """Remove old bucket entries"""
        async with self.lock:
            now = time.time()
            expired = [
                ip for ip, bucket in self.buckets.items()
                if now - bucket['last_update'] > 3600  # 1 hour
            ]
            for ip in expired:
                del self.buckets[ip]

class WebhookQueue:
    """Queue for processing webhooks with rate limiting and retries"""
    
    def __init__(self, secret: str = None, max_queue_size: int = 1000, batch_size: int = 10, max_retries: int = 3):
        """Initialize webhook queue"""
        self.secret = secret
        self.max_queue_size = max_queue_size
        self.batch_size = batch_size
        self.max_retries = max_retries
        
        # Initialize queue and locks
        self.queue = asyncio.Queue(maxsize=max_queue_size)
        self.queue_lock = asyncio.Lock()
        self.processing = False
        self.stop_event = asyncio.Event()
        
        # Initialize event handlers
        self.event_handlers: Dict[str, Callable] = {}
        
        # Initialize metrics
        self.processed_count = 0
        self.error_count = 0
        self.last_processed = None
        
        # Initialize error tracking
        self.errors = deque(maxlen=100)
        self.error_counts = defaultdict(int)
        
        logger.info(f"Initialized WebhookQueue with max_size={max_queue_size}, batch_size={batch_size}")
        
        # Initialize rate limiters
        self.global_limiter = IPRateLimiter(
            WEBHOOK_SECURITY['RATE_LIMITS']['global']['requests_per_second'],
            WEBHOOK_SECURITY['RATE_LIMITS']['global']['burst']
        )
        self.ip_limiter = IPRateLimiter(
            WEBHOOK_SECURITY['RATE_LIMITS']['per_ip']['requests_per_second'],
            WEBHOOK_SECURITY['RATE_LIMITS']['per_ip']['burst']
        )
        
        self.stats = {
            'total_received': 0,
            'total_processed': 0,
            'total_failed': 0,
            'total_retried': 0,
            'total_oversized': 0,
            'total_invalid': 0,
            'total_rate_limited': 0,
            'total_blocked_ips': 0,
            'memory_usage_mb': 0.0
        }
        
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
        """Periodically clean up old events and rate limit data"""
        while not self._shutdown:
            try:
                # Clean up IP rate limiters
                await self.global_limiter.cleanup()
                await self.ip_limiter.cleanup()

                # Clean up old events
                now = datetime.now()
                async with self.queue_lock:
                    events = []
                    while not self.queue.empty():
                        event = self.queue.get_nowait()
                        if (now - event.created_at).total_seconds() <= WEBHOOK_CONFIG['MAX_EVENT_AGE']:
                            events.append(event)
                        else:
                            self._add_error(
                                'event_expired',
                                f'Event {event.id} expired after {WEBHOOK_CONFIG['MAX_EVENT_AGE']} seconds',
                                event.id,
                                {'age': (now - event.created_at).total_seconds()}
                            )
                    for event in events:
                        self.queue.put_nowait(event)

                # Update memory usage stats
                self.stats['memory_usage_mb'] = self._get_memory_usage()

                # Log cleanup stats
                logger.info(f"Cleanup completed. Queue size: {self.queue.qsize()}, "
                          f"Memory usage: {self.stats['memory_usage_mb']:.2f}MB")

            except Exception as e:
                logger.error(f"Error in cleanup loop: {e}")

            await asyncio.sleep(WEBHOOK_CONFIG['CLEANUP_INTERVAL'])

    async def clear_old_events(self):
        """Clear events older than MAX_EVENT_AGE seconds"""
        try:
            async with self.queue_lock:
                current_time = datetime.now()
                old_events = []
                while not self.queue.empty():
                    event = self.queue.get_nowait()
                    if (current_time - event.created_at).total_seconds() > WEBHOOK_CONFIG['MAX_EVENT_AGE']:
                        old_events.append(event)
                    else:
                        self.queue.put_nowait(event)
                
                for event in old_events:
                    self._add_error(
                        'event_expired',
                        f'Event {event.id} expired after {WEBHOOK_CONFIG["MAX_EVENT_AGE"]} seconds',
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
            if self.queue.full():
                self._add_error('queue_full',
                              f'Queue full ({self.max_queue_size} events)',
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
            await self.queue.put(event)
            self.stats['total_received'] += 1
            self.stats['current_queue_size'] = self.queue.qsize()
                
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
            if memory_mb > WEBHOOK_SECURITY['MEMORY_THRESHOLDS']['critical']:
                logger.warning(f"Memory usage ({memory_mb:.2f}MB) exceeds limit ({WEBHOOK_SECURITY['MEMORY_THRESHOLDS']['critical']}MB)")
                
                # Remove oldest events until under limit
                async with self.queue_lock:
                    while memory_mb > WEBHOOK_SECURITY['MEMORY_THRESHOLDS']['critical'] and not self.queue.empty():
                        event = self.queue.get_nowait()
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
        """Process queued webhook events"""
        while not self._shutdown:
            try:
                async with self.queue_lock:
                    if self.queue.empty():
                        await asyncio.sleep(1)
                        continue

                    # Process events in batches
                    batch_size = min(self.batch_size, self.queue.qsize())
                    batch = []
                    for _ in range(batch_size):
                        event = await self.queue.get()
                        if event.should_retry():
                            batch.append(event)
                        else:
                            self.stats['total_failed'] += 1
                            logger.warning(f"Event {event.id} exceeded retry limit or age limit")

                    if batch:
                        await self._process_batch(batch)

            except Exception as e:
                logger.error(f"Error processing webhook queue: {e}")
                await asyncio.sleep(5)  # Back off on error

            await asyncio.sleep(0.1)  # Prevent CPU spinning

    async def _process_batch(self, batch: List[WebhookEvent]):
        """Process a batch of webhook events"""
        for event in batch:
            try:
                # Process the event
                # This would call your webhook handling logic
                logger.info(f"Processing webhook event {event.id}")
                
                # Update stats
                self.stats['total_processed'] += 1
                
            except Exception as e:
                event.retries += 1
                event.last_retry = datetime.now()
                event.error = str(e)
                
                if event.should_retry():
                    await self.queue.put(event)
                    self.stats['total_retried'] += 1
                    logger.warning(f"Webhook event {event.id} failed, retrying. Error: {e}")
                else:
                    self.stats['total_failed'] += 1
                    logger.error(f"Webhook event {event.id} failed permanently. Error: {e}")

    async def process_webhook(self, request: web.Request) -> web.Response:
        """Process incoming webhook request"""
        try:
            # Get request body
            body = await request.text()
            
            # Verify signature
            if not self.validate_signature(dict(request.headers), body):
                return web.Response(text="Invalid signature", status=401)
            
            # Parse JSON data
            data = json.loads(body)
            
            # Check security limits
            if not await self.check_request(len(body), request.remote):
                return web.Response(text="Rate limit exceeded", status=429)
                
            # Verify required fields
            if not isinstance(data, list):
                data = [data]  # Convert single event to list
                
            for event in data:
                if 'type' not in event or 'payload' not in event:
                    return web.Response(text="Missing required fields", status=400)
                    
                # Add to queue
                await self.add_event(
                    event_id=event.get('id', str(uuid.uuid4())),
                    event_type=event['type'],
                    payload=event['payload'],
                    headers=dict(request.headers)
                )
            
            # Return success (must be 2xx as per Blockfrost docs)
            return web.Response(text="OK", status=200)
            
        except json.JSONDecodeError:
            return web.Response(text="Invalid JSON", status=400)
        except Exception as e:
            logger.error(f"Error processing webhook: {e}")
            return web.Response(text=str(e), status=500)

    def register_handler(self, event_type: str, handler: Callable):
        """Register handler for event type"""
        self.event_handlers[event_type] = handler
        
    def validate_signature(self, headers: Dict[str, str], payload: Union[str, bytes]) -> bool:
        """Validate Blockfrost webhook signature"""
        try:
            # Get signature header (case-insensitive)
            signature_header = next(
                (v for k, v in headers.items() if k.lower() == 'blockfrost-signature'),
                None
            )
            if not signature_header:
                logger.warning("Missing Blockfrost-Signature header")
                return False

            # Parse header parts
            try:
                header_parts = dict(part.split('=') for part in signature_header.split(','))
            except ValueError:
                logger.warning("Invalid signature header format")
                return False

            # Verify required parts
            if not all(k in header_parts for k in ['t', 'v1']):
                logger.warning("Missing required signature components")
                return False

            # Extract parts
            timestamp = header_parts['t']
            signature = header_parts['v1']

            # Validate timestamp format
            try:
                timestamp_int = int(timestamp)
            except ValueError:
                logger.warning("Invalid timestamp format")
                return False

            # Check timestamp is within window (600s default in Blockfrost)
            now = int(time.time())
            if abs(now - timestamp_int) > 600:
                logger.warning(f"Webhook timestamp too old: {abs(now - timestamp_int)} seconds")
                return False

            # Ensure payload is string
            if isinstance(payload, bytes):
                payload = payload.decode('utf-8')

            # Create signature payload (timestamp.payload)
            msg = f"{timestamp}.{payload}"

            # Calculate expected signature
            expected = hmac.new(
                key=self.secret.encode('utf-8'),
                msg=msg.encode('utf-8'),
                digestmod=hashlib.sha256
            ).hexdigest()

            # Use constant-time comparison
            return hmac.compare_digest(signature.lower(), expected.lower())

        except Exception as e:
            logger.error(f"Error validating signature: {e}", exc_info=True)
            return False
            
    async def check_request(self, request_size: int, client_ip: str) -> Tuple[bool, str]:
        """Check if request passes security checks"""
        # Check request size
        if request_size > WEBHOOK_SECURITY['MAX_REQUEST_SIZE']:
            return False, "Request size exceeds limit"
            
        # Check rate limits
        if not await self.global_limiter.is_allowed("global"):
            self.stats['total_rate_limited'] += 1
            return False, "Global rate limit exceeded"
            
        if not await self.ip_limiter.is_allowed(client_ip):
            self.stats['total_rate_limited'] += 1
            return False, "IP rate limit exceeded"
            
        # Check memory usage
        memory_usage = await self._check_memory_usage()
        if memory_usage > WEBHOOK_SECURITY['MEMORY_THRESHOLDS']['critical']:
            return False, "Server under high memory pressure"
            
        return True, ""
        
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

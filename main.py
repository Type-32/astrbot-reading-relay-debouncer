"""
Reading-delay pre-debouncer.
Holds incoming messages for roughly the time a human would need to read them,
accumulating per message, then replays the merged burst into the pipeline so
downstream debouncers (e.g. the jev/BERT completeness plugin) see one message.
"""
import asyncio
import re
import time
from dataclasses import dataclass, field
from typing import Dict, Optional

from astrbot.api import logger, AstrBotConfig
from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.event.filter import EventMessageType
from astrbot.api.star import Context, Star, register
from astrbot.core.message.components import Plain, Image

CJK = re.compile(r"[\u3040-\u30ff\u3400-\u4dbf\u4e00-\u9fff\uac00-\ud7af]")


def reading_units(text: str) -> float:
    """Words for ASCII text; CJK characters count as ~0.5 word each."""
    cjk = len(CJK.findall(text))
    words = len(CJK.sub(" ", text).split())
    return words + cjk * 0.5


@dataclass
class Burst:
    texts: list = field(default_factory=list)
    images: list = field(default_factory=list)
    first_at: float = 0.0
    deadline: float = 0.0
    event: Optional[AstrMessageEvent] = None
    task: Optional[asyncio.Task] = None


@register("reading_delay", "you", "human reading-time pre-debounce", "0.1")
class ReadingDelay(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.cfg = config
        self.bursts: Dict[str, Burst] = {}
        self.replay_ids: set = set()

    # ---------- timing ----------
    def _delay_for(self, text: str, n_images: int) -> float:
        d = self.cfg.get("base_delay", 0.8)
        d += reading_units(text) / self.cfg.get("reading_wps", 3.5)
        d += n_images * self.cfg.get("per_image_delay", 2.0)
        return max(self.cfg.get("min_delay", 0.6), min(d, self.cfg.get("max_delay_per_msg", 6.0)))

    # ---------- intake ----------
    @filter.event_message_type(EventMessageType.ALL, priority=200)
    async def intake(self, event: AstrMessageEvent):
        if not self.cfg.get("enabled", True):
            return
        scope = self.cfg.get("usage_scope", "private")
        if scope == "private" and not event.is_private_chat():
            return
        if scope == "group" and event.is_private_chat():
            return
        if not getattr(event, "is_at_or_wake_command", True):
            return  # wouldn't reach the LLM anyway

        msg_id = event.message_obj.message_id
        if msg_id in self.replay_ids:
            self.replay_ids.discard(msg_id)
            return  # our own replay → let it through to jev

        text = (event.message_str or "").strip()
        if any(text.startswith(p) for p in self.cfg.get("command_prefixes", ["/"])):
            return
        images = [c for c in event.message_obj.message if isinstance(c, Image)]
        if not text and not images:
            return

        sid = event.unified_msg_origin
        now = time.time()
        d = self._delay_for(text, len(images))

        b = self.bursts.get(sid)
        if b is None:
            b = Burst(first_at=now, deadline=now + self.cfg.get("pickup_delay", 1.0) + d)
            self.bursts[sid] = b
            b.task = asyncio.create_task(self._flush_when_due(sid))
        else:
            b.deadline = max(b.deadline, now) + d
        b.deadline = min(b.deadline, b.first_at + self.cfg.get("max_total_wait", 20.0))

        if text:
            b.texts.append(text)
        b.images.extend(images)
        b.event = event
        logger.debug(f"[ReadingDelay] +{d:.1f}s, flush in {b.deadline - now:.1f}s ({len(b.texts)} msgs)")
        event.stop_event()

    # ---------- flush ----------
    async def _flush_when_due(self, sid: str):
        try:
            while True:
                b = self.bursts.get(sid)
                if b is None:
                    return
                remaining = b.deadline - time.time()
                if remaining <= 0:
                    break
                await asyncio.sleep(remaining)  # deadline may move; loop re-checks
            b = self.bursts.pop(sid, None)
            if b and b.event and (b.texts or b.images):
                await self._replay(b)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"[ReadingDelay] flush failed: {e}", exc_info=True)
            self.bursts.pop(sid, None)

    async def _replay(self, b: Burst):
        from astrbot.core.star.star_tools import StarTools

        text = self.cfg.get("join_separator", "\n").join(b.texts)
        ev = b.event
        components = [Plain(text)] if text else []
        components.extend(b.images)

        msg = await StarTools.create_message(
            type=str(ev.message_obj.type.value),
            self_id=ev.get_self_id(),
            session_id=ev.session_id,
            sender=ev.message_obj.sender,
            message=components,
            message_str=text,
            group_id=ev.get_group_id() or "",
            message_id=ev.message_obj.message_id,
        )
        self.replay_ids.add(msg.message_id)
        try:
            await StarTools.create_event(abm=msg, platform=ev.get_platform_name(), is_wake=True)
            logger.debug(f"[ReadingDelay] replayed {len(b.texts)} msgs / {len(b.images)} imgs")
        except Exception:
            self.replay_ids.discard(msg.message_id)
            raise

    async def terminate(self):
        for b in self.bursts.values():
            if b.task:
                b.task.cancel()
        self.bursts.clear()
        self.replay_ids.clear()

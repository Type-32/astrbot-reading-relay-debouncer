# data/plugins/astrbot_plugin_skip_filter/main.py
from astrbot.api.event import filter, AstrMessageEvent
from astrbot.api.star import Context, Star, register

SKIP = "[SKIP]"

@register("skip_filter", "you", "swallow [SKIP] replies so nothing is sent", "0.1")
class SkipFilter(Star):
    def __init__(self, context: Context):
        super().__init__(context)

    @filter.on_decorating_result(priority=1000)   # run before segmented_reply
    async def drop_skip(self, event: AstrMessageEvent):
        result = event.get_result()
        if not result:
            return
        text = result.get_plain_text().strip()
        if SKIP in text or text == "":
            event.clear_result()
            event.stop_event()
